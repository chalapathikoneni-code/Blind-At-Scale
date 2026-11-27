#!/usr/bin/env bash
# setup.sh -- Blind at Scale, Workstream 4: Spark cluster ground truth.
#
# Installs OpenJDK + Apache Spark 3.5.x (binary tarball) on a bare Ubuntu
# pod and starts a Spark STANDALONE cluster on this host:
#   1 master  (spark://<host>:7077, web UI :8080, REST /json/ enabled by UI)
#   2 workers (web UIs :8081/:8082)
# with Spark event logging ON (needed by collect_results.py).
#
# HONEST LABELLING: this is a multi-worker standalone cluster on a SINGLE
# host. All shuffle traffic is loopback. Results must be labelled exactly
# that way in the paper (see README_POD.md).
#
# Usage:  bash setup.sh
# Env overrides:
#   SPARK_VERSION   (default 3.5.3)
#   WORKERS         (default 2)
#   WORKER_CORES    (default: floor(nproc / WORKERS), min 1)
#   WORKER_MEMORY   (default: see below; e.g. 6g)
#   EVENTLOG_DIR    (default: $HOME/bas-spark/eventlog)
#   BAS_HOME        (default: $HOME/bas-spark)  -- logs, pids, event logs
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
TARBALL="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
URLS=(
  "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${TARBALL}"
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${TARBALL}"
)
WORKERS="${WORKERS:-2}"
BAS_HOME="${BAS_HOME:-$HOME/bas-spark}"
EVENTLOG_DIR="${EVENTLOG_DIR:-$BAS_HOME/eventlog}"
mkdir -p "$BAS_HOME" "$EVENTLOG_DIR" "$BAS_HOME/logs" "$BAS_HOME/pids" "$BAS_HOME/work"

if [ "$(id -u)" = "0" ]; then INSTALL_BASE=/opt; else INSTALL_BASE="$HOME/opt"; fi
mkdir -p "$INSTALL_BASE"
export SPARK_HOME="${SPARK_HOME:-$INSTALL_BASE/spark-${SPARK_VERSION}-bin-hadoop3}"

echo "=== [1/5] OS packages (OpenJDK 17, python3, pip) ==="
if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  # openjdk-17 is the LTS JDK Spark 3.5 is certified against; fall back to 21/11.
  apt-get install -y -qq openjdk-17-jdk-headless python3 python3-pip curl procps rsync \
    || apt-get install -y -qq openjdk-21-jdk-headless python3 python3-pip curl procps rsync \
    || apt-get install -y -qq default-jdk-headless python3 python3-pip curl procps rsync
else
  echo "WARNING: apt-get not found; assuming java/python3 already installed." >&2
fi
java -version 2>&1 | head -2
python3 --version

echo "=== [2/5] Python deps for gen_on_pod.py (numpy, pyarrow) ==="
# pyspark comes from the binary tarball ($SPARK_HOME/python); do NOT pip pyspark.
# numpy is PINNED to 2.5.4: Generator.multinomial changed between numpy
# 1.26 and 2.x, and only the 2.5.4 draws reproduce the local study's
# per-key counts bit-for-bit (verified 2026-11-06). gen_on_pod.py
# hard-fails its SHA-256 counts check if this is wrong -- do not relax it.
python3 -m pip install --quiet "numpy==2.5.4" pyarrow \
  || python3 -m pip install --quiet --break-system-packages "numpy==2.5.4" pyarrow \
  || echo "WARNING: pip install failed; gen_on_pod.py needs numpy+pyarrow." >&2
python3 -c "import numpy, pyarrow; print('numpy', numpy.__version__, '| pyarrow', pyarrow.__version__)"

echo "=== [3/5] Spark ${SPARK_VERSION} tarball -> $SPARK_HOME ==="
if [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  cd "$INSTALL_BASE"
  ok=0
  for u in "${URLS[@]}"; do
    echo "trying $u"
    if curl -fL --retry 2 -o "$TARBALL" "$u"; then ok=1; break; fi
  done
  [ "$ok" = "1" ] || { echo "ERROR: could not download Spark tarball from any mirror." >&2; exit 1; }
  tar --no-same-owner -xzf "$TARBALL"   # --no-same-owner: non-root extracts
                                        # otherwise fail on chown (seen locally)
  rm -f "$TARBALL"
fi
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | head -4 || true

echo "=== [4/5] spark-defaults (event logging ON) ==="
HOST_IP="$(hostname -i | awk '{print $1}')"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
cat > "$SPARK_HOME/conf/spark-defaults.conf" <<EOF
spark.eventLog.enabled           true
spark.eventLog.dir               file://${EVENTLOG_DIR}
spark.history.fs.logDirectory    file://${EVENTLOG_DIR}
spark.sql.shuffle.partitions     128
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.ui.showConsoleProgress     false
EOF
cat > "$SPARK_HOME/conf/spark-env.sh" <<EOF
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP}
export SPARK_LOG_DIR=${BAS_HOME}/logs
export SPARK_PID_DIR=${BAS_HOME}/pids
export SPARK_WORKER_DIR=${BAS_HOME}/work
EOF
echo "host ip: $HOST_IP (workers register on 127.0.0.1 by design: single host)"

echo "=== [5/5] Start standalone master + ${WORKERS} workers ==="
# Stop anything from a previous run of THIS script (ident-string scoped).
"$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
for i in $(seq 1 "$WORKERS"); do
  SPARK_IDENT_STRING="basworker$i" "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
done
sleep 2

"$SPARK_HOME/sbin/start-master.sh" -h 127.0.0.1 -p 7077 --webui-port 8080

NCORES="$(nproc)"
if [ -z "${WORKER_CORES:-}" ]; then WORKER_CORES=$(( NCORES / WORKERS )); [ "$WORKER_CORES" -lt 1 ] && WORKER_CORES=1; fi
if [ -z "${WORKER_MEMORY:-}" ]; then
  TOTAL_MB="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)"
  # Reserve ~35% for driver/OS/gen; split the rest across workers. Floor 2g.
  PER=$(( (TOTAL_MB * 65 / 100) / WORKERS / 1024 ))
  [ "$PER" -lt 2 ] && PER=2
  WORKER_MEMORY="${PER}g"
fi
echo "workers: cores=${WORKER_CORES} each, memory=${WORKER_MEMORY} each"
for i in $(seq 1 "$WORKERS"); do
  WPORT=$(( 7176 + i ))            # worker daemon port 7177, 7178, ...
  WUI=$(( 8079 + i ))              # worker web UI 8081, 8082, ...
  SPARK_IDENT_STRING="basworker$i" \
  SPARK_WORKER_CORES="$WORKER_CORES" \
  SPARK_WORKER_MEMORY="$WORKER_MEMORY" \
  SPARK_WORKER_PORT="$WPORT" \
  SPARK_WORKER_WEBUI_PORT="$WUI" \
  SPARK_WORKER_DIR="$BAS_HOME/work/worker$i" \
    "$SPARK_HOME/sbin/start-worker.sh" "spark://127.0.0.1:7077" \
    || SPARK_IDENT_STRING="basworker$i" \
       SPARK_WORKER_CORES="$WORKER_CORES" SPARK_WORKER_MEMORY="$WORKER_MEMORY" \
       SPARK_WORKER_PORT="$WPORT" SPARK_WORKER_WEBUI_PORT="$WUI" \
       SPARK_WORKER_DIR="$BAS_HOME/work/worker$i" \
       "$SPARK_HOME/sbin/start-slave.sh" "spark://127.0.0.1:7077"
done

echo "--- waiting for cluster to report ${WORKERS} alive workers (timeout 120s) ---"
for s in $(seq 1 60); do
  ALIVE="$(curl -fs http://127.0.0.1:8080/json/ 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("aliveworkers","?"))' 2>/dev/null || echo '?')"
  echo "  t+$((s*2))s aliveworkers=$ALIVE"
  [ "$ALIVE" = "$WORKERS" ] && break
  sleep 2
done
curl -fs http://127.0.0.1:8080/json/ | python3 -m json.tool | head -30 || true
echo
echo "SETUP DONE. SPARK_HOME=$SPARK_HOME  EVENTLOG_DIR=$EVENTLOG_DIR"
echo "Master: spark://127.0.0.1:7077   UI: http://127.0.0.1:8080"
echo "Stop later with: $SPARK_HOME/sbin/stop-all.sh  (or stop-master.sh + stop-worker.sh per ident)"
