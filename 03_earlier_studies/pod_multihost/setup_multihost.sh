#!/usr/bin/env bash
# setup_multihost.sh -- Blind at Scale multi-host Spark ground truth.
#
# Run on EACH of the 3 hosts, with that host's cluster.env in place
# (ROLE=master on host A, worker-b on B, worker-c on C):
#
#   bash setup_multihost.sh
#
# What it does, in order (every step is a gate -- failure stops the run):
#   0. pod-ops preflight, if present on the volume (RunPod path).
#   1. Port 1:1 hard guard (driver / block-manager / worker ports).
#   2. Resolve the ONE interpreter ($PY; never the system python3).
#   3. Install OpenJDK 17 + Spark 3.5.3 (idempotent; tarball mirrors as
#      in the single-host setup.sh).
#   4. Render spark-defaults.conf / spark-env.sh from cluster.env.
#   5a. ROLE=master: start the standalone master (bind local interface,
#       port MASTER_PORT).
#   5b. ROLE=worker-*: probe the master endpoint, then start ONE worker
#       (cores WORKER_CORES, memory WORKER_MEMORY) registered against
#       spark://MASTER_PUBLIC_HOST:EXT_MASTER_PORT.
#
# Daemons are (re)started with this package's ident strings only; other
# Spark processes on the host are not touched.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
. ./lib_multihost.sh
mh_load_env

echo "=== [0] preflight (if available) ==="
if [ -f /workspace/pod-ops/preflight.sh ]; then
  ( cd /workspace && bash pod-ops/preflight.sh "${PY:-.venv-pod/bin/python}" numpy ) || {
    echo "PREFLIGHT FAILED -- stopping (pod-ops standard)." >&2; exit 1; }
else
  echo "WARNING: /workspace/pod-ops/preflight.sh not found; continuing without it."
  echo "Generic hosts: confirm 'java -version' is 17 and >=8 vCPU/16 GB manually."
fi

echo "=== [1] port guard ==="
mh_port_guard

echo "=== [2] interpreter ==="
mh_resolve_py

echo "=== [3] OpenJDK 17 + Spark ${SPARK_VERSION} ==="
if command -v apt-get >/dev/null 2>&1 && ! java -version 2>&1 | grep -q '17\.'; then
  if [ "$(id -u)" = "0" ]; then SUDO=""; elif command -v sudo >/dev/null 2>&1; then SUDO="sudo"; else
    echo "ERROR: need root or sudo to install OpenJDK 17." >&2; exit 1; fi
  export DEBIAN_FRONTEND=noninteractive
  $SUDO apt-get update -qq
  $SUDO apt-get install -y -qq openjdk-17-jdk-headless curl procps rsync \
    || $SUDO apt-get install -y -qq default-jdk-headless curl procps rsync
fi
java -version 2>&1 | head -2
TARBALL="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
mkdir -p "$(dirname "$SPARK_HOME")"
if [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  ok=0
  for u in "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${TARBALL}" \
           "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${TARBALL}"; do
    echo "trying $u"
    if curl -fL --retry 2 -o "/tmp/$TARBALL" "$u"; then ok=1; break; fi
  done
  [ "$ok" = "1" ] || { echo "ERROR: Spark tarball download failed on all mirrors." >&2; exit 1; }
  tar --no-same-owner -xzf "/tmp/$TARBALL" -C "$(dirname "$SPARK_HOME")"
  rm -f "/tmp/$TARBALL"
fi
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | head -4 || true

echo "=== [4] render conf ==="
mh_render_conf

echo "=== [5] start daemon for ROLE=$ROLE ==="
LOCAL_IP="$(hostname -i | awk '{print $1}')"
if [ "$ROLE" = "master" ]; then
  SPARK_IDENT_STRING=basmh-master "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
  sleep 1
  "$SPARK_HOME/sbin/start-master.sh" -h "$LOCAL_IP" -p "$MASTER_PORT" \
    --webui-port "$MASTER_WEBUI_PORT"
  mh_status "master daemon started on $LOCAL_IP:$MASTER_PORT (advertised $MASTER_PUBLIC_HOST:$EXT_MASTER_PORT)"
  echo "Master up. Workers register at spark://$MASTER_PUBLIC_HOST:$EXT_MASTER_PORT"
  echo "Master JSON (local): http://127.0.0.1:$MASTER_WEBUI_PORT/json/"
else
  echo "--- probing master endpoint before worker start ---"
  mh_probe "$MASTER_PUBLIC_HOST" "$EXT_MASTER_PORT" "master RPC"
  SPARK_IDENT_STRING=basmh-worker "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
  sleep 1
  SPARK_IDENT_STRING=basmh-worker \
  SPARK_WORKER_CORES="${WORKER_CORES:-4}" \
  SPARK_WORKER_MEMORY="${WORKER_MEMORY:-6g}" \
  SPARK_WORKER_PORT="$WORKER_PORT" \
  SPARK_WORKER_WEBUI_PORT="$WORKER_WEBUI_PORT" \
  SPARK_WORKER_DIR="$BAS_HOME_MH/work/worker" \
    "$SPARK_HOME/sbin/start-worker.sh" \
      "spark://$MASTER_PUBLIC_HOST:$EXT_MASTER_PORT"
  mh_status "worker daemon started (advertised $(mh_my_public_host), daemon port $WORKER_PORT, block-manager $BLOCKMANAGER_PORT)"
  echo "Worker up. Verify on the master host: bash run_worker.sh (or check the master /json/)."
fi
echo "SETUP_MULTIHOST DONE for ROLE=$ROLE. SPARK_HOME=$SPARK_HOME"
