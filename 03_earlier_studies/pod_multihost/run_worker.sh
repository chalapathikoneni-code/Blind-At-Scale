#!/usr/bin/env bash
# run_worker.sh -- Blind at Scale multi-host ground truth, WORKER side.
# Run on host B or C (ROLE=worker-b / worker-c) after setup_multihost.sh.
#
#   bash run_worker.sh            verify this worker is registered ALIVE
#                                 in the master's /json/ (retries 60 s)
#   bash run_worker.sh --archive  additionally copy this worker's Spark
#                                 logs to $MH_ROOT/logs_<role>/ (shared
#                                 volume on RunPod; on generic hosts the
#                                 files land in the local $MH_ROOT and
#                                 are returned with the run archive).
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
. ./lib_multihost.sh
mh_load_env
case "$ROLE" in worker-b|worker-c) ;; *)
  echo "ERROR: run_worker.sh must run on a worker host (ROLE=$ROLE)." >&2; exit 1;; esac
mh_resolve_py >/dev/null

MY_HOST="$(mh_my_public_host)"
echo "verifying registration of worker $MY_HOST with master $MASTER_PUBLIC_HOST:$EXT_MASTER_WEBUI_PORT ..."
ok=0
for i in $(seq 1 30); do
  if "$PY" - "$MASTER_PUBLIC_HOST" "$EXT_MASTER_WEBUI_PORT" "$MY_HOST" <<'PYEOF'
import json, sys, urllib.request
host, port, mine = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with urllib.request.urlopen(f"http://{host}:{port}/json/", timeout=5) as r:
        j = json.load(r)
except Exception:
    sys.exit(1)
for w in j.get("workers", []):
    if w.get("host") == mine or mine in str(w.get("host", "")):
        print(f"registered: id={w.get('id')} host={w.get('host')} "
              f"cores={w.get('cores')} state={w.get('state')}")
        sys.exit(0)
sys.exit(1)
PYEOF
  then ok=1; break; fi
  sleep 2
done
[ "$ok" = "1" ] || {
  echo "ERROR: worker $MY_HOST not visible in master /json/ after 60 s." >&2
  echo "Check: worker daemon log in $BAS_HOME_MH/logs/, master reachability" >&2
  echo "(probe: bash -c 'exec 3<>/dev/tcp/$MASTER_PUBLIC_HOST/$EXT_MASTER_PORT')," >&2
  echo "and OPERATOR_RUNBOOK.md failure diagnosis F2." >&2
  exit 1
}
mh_status "worker $MY_HOST verified registered with master"

if [ "${1:-}" = "--archive" ]; then
  dest="$MH_ROOT/logs_$ROLE"
  mkdir -p "$dest"
  cp -f "$BAS_HOME_MH"/logs/* "$dest/" 2>/dev/null || true
  mh_status "worker $ROLE logs archived to $dest"
  echo "logs archived to $dest"
fi
echo "run_worker OK for ROLE=$ROLE"
