# lib_multihost.sh -- shared logic for the Blind at Scale multi-host
# package. Sourced by setup_multihost.sh, run_master.sh, run_worker.sh.
# Not executable on its own.

# ---------------------------------------------------------------- env --
mh_load_env() {
  local env_file="${MH_ENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cluster.env}"
  [ -f "$env_file" ] || { echo "ERROR: cluster.env not found at $env_file" >&2; \
    echo "Copy cluster.env.template to cluster.env and fill it (see runbook)." >&2; return 1; }
  # shellcheck disable=SC1090
  set -a; . "$env_file"; set +a
  : "${ROLE:?cluster.env: ROLE is empty}"
  : "${MASTER_PUBLIC_HOST:?cluster.env: MASTER_PUBLIC_HOST is empty}"
  : "${WORKER_B_PUBLIC_HOST:?cluster.env: WORKER_B_PUBLIC_HOST is empty}"
  : "${WORKER_C_PUBLIC_HOST:?cluster.env: WORKER_C_PUBLIC_HOST is empty}"
  : "${BAS_LABEL:?cluster.env: BAS_LABEL is empty}"
  case "$ROLE" in master|worker-b|worker-c) ;; *)
    echo "ERROR: ROLE must be master|worker-b|worker-c, got '$ROLE'" >&2; return 1;; esac
  export MH_ROOT="${MH_ROOT:-/workspace/blind-multihost}"
  export MH_PKG="${MH_PKG:-$MH_ROOT/pkg}"
  export SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
  if [ "$(id -u)" = "0" ]; then INSTALL_BASE=/opt; else INSTALL_BASE="$HOME/opt"; fi
  # SPARK_HOME is PRESET here (never glob-detected -- single-host lesson 2:
  # the ls-based auto-detection in run_pod.sh aborted under set -e/pipefail).
  export SPARK_HOME="${SPARK_HOME:-$INSTALL_BASE/spark-${SPARK_VERSION}-bin-hadoop3}"
  export BAS_HOME_MH="${BAS_HOME_MH:-$HOME/bas-spark-mh}"
  export EVENTLOG_DIR="${EVENTLOG_DIR:-$MH_ROOT/eventlog}"
}

mh_my_public_host() {
  case "$ROLE" in
    master)   echo "$MASTER_PUBLIC_HOST" ;;
    worker-b) echo "$WORKER_B_PUBLIC_HOST" ;;
    worker-c) echo "$WORKER_C_PUBLIC_HOST" ;;
  esac
}
mh_my_ext_blockmanager() {
  case "$ROLE" in
    worker-b) echo "$EXT_BLOCKMANAGER_PORT_B" ;;
    worker-c) echo "$EXT_BLOCKMANAGER_PORT_C" ;;
    *) echo "$BLOCKMANAGER_PORT" ;;
  esac
}
mh_my_ext_worker_port() {
  case "$ROLE" in
    worker-b) echo "$EXT_WORKER_PORT_B" ;;
    worker-c) echo "$EXT_WORKER_PORT_C" ;;
    *) echo "$WORKER_PORT" ;;
  esac
}

# ------------------------------------------------- 1:1 port hard guard --
# Spark binds and advertises ONE port number for the driver RPC, the
# block-manager service and the worker daemon. A NAT-shifted external
# mapping for any of these cannot be bridged by configuration. Refuse
# to start rather than silently run a cluster that cannot shuffle.
mh_port_guard() {
  local fail=0
  if [ "$ROLE" = "master" ] && [ "$EXT_DRIVER_PORT" != "$DRIVER_PORT" ]; then
    echo "PORT GUARD: EXT_DRIVER_PORT=$EXT_DRIVER_PORT != DRIVER_PORT=$DRIVER_PORT" >&2; fail=1
  fi
  if [ "$ROLE" != "master" ]; then
    if [ "$(mh_my_ext_blockmanager)" != "$BLOCKMANAGER_PORT" ]; then
      echo "PORT GUARD: this worker's EXT_BLOCKMANAGER=$(mh_my_ext_blockmanager) != BLOCKMANAGER_PORT=$BLOCKMANAGER_PORT" >&2; fail=1
    fi
    if [ "$(mh_my_ext_worker_port)" != "$WORKER_PORT" ]; then
      echo "PORT GUARD: this worker's EXT_WORKER_PORT=$(mh_my_ext_worker_port) != WORKER_PORT=$WORKER_PORT" >&2; fail=1
    fi
  fi
  if [ "$fail" = "1" ]; then
    echo "Refusing to start: single-knob Spark services need 1:1 port mappings." >&2
    echo "Redeploy the affected pod or STOP and report the platform block" >&2
    echo "(OPERATOR_RUNBOOK.md, failure diagnosis F1)." >&2
    return 1
  fi
}

# ------------------------------------------------------- interpreter --
# One-interpreter rule. The RunPod base image's system python3 is 3.10:
# its numpy (even if 2.5.4 could be installed, which it cannot -- 2.5.4
# needs >=3.12) must never run gen_on_pod.py. We require the volume
# venv (Python 3.13, numpy==2.5.4) or an explicitly configured $PY that
# passes the same checks.
mh_resolve_py() {
  local cand
  for cand in "${PY:-}" /workspace/.venv-pod/bin/python "$MH_ROOT/.venv/bin/python"; do
    [ -n "$cand" ] && [ -x "$cand" ] || continue
    if "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3,12) else 1)' 2>/dev/null; then
      export PY="$cand"; break
    fi
  done
  [ -n "${PY:-}" ] && [ -x "${PY:-/nonexistent}" ] || {
    echo "ERROR: no suitable interpreter found (need Python >= 3.12)." >&2
    echo "On RunPod pods use the volume venv /workspace/.venv-pod/bin/python." >&2
    echo "NEVER fall back to the system python3 (3.10 on the base image)." >&2; return 1; }
  "$PY" -m pip install --quiet "numpy==2.5.4" pyarrow 2>/dev/null || true
  "$PY" - <<'PYEOF'
import sys
import numpy
assert numpy.__version__ == "2.5.4", f"numpy must be 2.5.4, got {numpy.__version__}"
import pyarrow  # noqa: F401
print(f"interpreter OK: {sys.executable} python {sys.version.split()[0]} "
      f"numpy {numpy.__version__} pyarrow {pyarrow.__version__}")
PYEOF
}

# ------------------------------------------------------------- probes --
# TCP reachability probe using bash /dev/tcp. $1 host, $2 port, $3 label.
mh_probe() {
  local host="$1" port="$2" label="$3"
  if timeout 5 bash -c "exec 3<>/dev/tcp/$host/$port" 2>/dev/null; then
    echo "probe OK: $label $host:$port reachable"
    return 0
  fi
  echo "probe FAIL: $label $host:$port NOT reachable" >&2
  return 1
}

# ------------------------------------------------------------ status --
mh_status() {
  # STATUS.txt checkpointing lives on the master/driver host, under
  # $MH_ROOT so it survives on the shared volume.
  local line="$(date -u +%Y-%m-%dT%H:%M:%SZ) [$ROLE] $*"
  echo "$line"
  mkdir -p "$MH_ROOT"
  echo "$line" >> "$MH_ROOT/STATUS.txt"
}

# ----------------------------------------------------- conf rendering --
# Renders $SPARK_HOME/conf/spark-defaults.conf and spark-env.sh for this
# host's role into $1 (default $SPARK_HOME/conf). Used by
# setup_multihost.sh AND by selftest.sh (into a temp dir).
mh_render_conf() {
  local confdir="${1:-$SPARK_HOME/conf}"
  mkdir -p "$confdir" "$EVENTLOG_DIR" "$BAS_HOME_MH/logs" "$BAS_HOME_MH/pids" "$BAS_HOME_MH/work"
  local local_ip
  local_ip="$(hostname -i 2>/dev/null | awk '{print $1}')"; [ -n "$local_ip" ] || local_ip=127.0.0.1
  cat > "$confdir/spark-defaults.conf" <<EOF
spark.eventLog.enabled           true
spark.eventLog.dir               file://${EVENTLOG_DIR}
spark.history.fs.logDirectory    file://${EVENTLOG_DIR}
spark.sql.shuffle.partitions     128
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.ui.showConsoleProgress     false
spark.blockManager.port          ${BLOCKMANAGER_PORT}
spark.driver.host                ${MASTER_PUBLIC_HOST}
spark.driver.bindAddress         0.0.0.0
spark.driver.port                ${DRIVER_PORT}
EOF
  # spark-env.sh: SPARK_LOCAL_IP is the BIND address (this pod's real
  # interface). SPARK_LOCAL_HOSTNAME / SPARK_PUBLIC_DNS are the
  # ADVERTISED identity peers use -- the public endpoint. Confusing the
  # two is failure F2 in the runbook.
  cat > "$confdir/spark-env.sh" <<EOF
export SPARK_LOCAL_IP=${local_ip}
export SPARK_LOCAL_HOSTNAME=$(mh_my_public_host)
export SPARK_PUBLIC_DNS=$(mh_my_public_host)
export SPARK_LOG_DIR=${BAS_HOME_MH}/logs
export SPARK_PID_DIR=${BAS_HOME_MH}/pids
export SPARK_WORKER_DIR=${BAS_HOME_MH}/work
export PYSPARK_PYTHON=${PY:-python3}
export PYSPARK_DRIVER_PYTHON=${PY:-python3}
EOF
  echo "rendered Spark conf for ROLE=$ROLE into $confdir"
}
