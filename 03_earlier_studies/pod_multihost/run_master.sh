#!/usr/bin/env bash
# run_master.sh -- Blind at Scale multi-host ground truth, DRIVER side.
# Run on host A (ROLE=master) AFTER setup_multihost.sh has completed on
# all 3 hosts and both workers show ALIVE in the master /json/.
#
# Phases (each is a STATUS.txt checkpoint; every failure stops):
#   1. Cluster gate: aliveworkers == 2 with 2 DISTINCT worker hosts.
#   2. Dataset generation onto $MH_ROOT/data/zipf_s101 (shared volume on
#      RunPod; shared mount or pre-copied path on generic hosts). The
#      generator's SHA-256 counts gate must print PASS -- anything else
#      aborts the run. (If the dataset already exists with a PASS
#      summary it is reused, not regenerated.)
#   3. spark-submit of the 9 headline pipelines against the external
#      master URL.
#   4. Cross-host sanity gate: executor hosts in the event log must be
#      exactly the two worker hosts. If not, outputs are preserved and
#      the script exits nonzero -- the run must NOT be cited as
#      multi-host (see OPERATOR_RUNBOOK.md failure diagnosis).
#   5. collect_results.py with --predictions AND --t-load 200000
#      (explicit; never rely on the default).
#   6. Archive: master/driver logs copied under $MH_ROOT; final listing.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
. ./lib_multihost.sh
mh_load_env
[ "$ROLE" = "master" ] || { echo "ERROR: run_master.sh must run on ROLE=master." >&2; exit 1; }
mh_port_guard
mh_resolve_py

DATA_DIR="$MH_ROOT/data/zipf_s101"
RESULTS_OUT="$MH_ROOT/results_out"
MANIFEST="$MH_ROOT/run_manifest_zipf_s101.json"
mkdir -p "$DATA_DIR" "$RESULTS_OUT"
MASTER_URL="spark://$MASTER_PUBLIC_HOST:$EXT_MASTER_PORT"

mh_status "run_master start (master URL $MASTER_URL)"

echo "=== [1] cluster gate: 2 alive workers on distinct hosts ==="
"$PY" - "$MASTER_PUBLIC_HOST" "$EXT_MASTER_WEBUI_PORT" <<'PYEOF'
import json, sys, urllib.request
host, port = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(f"http://{host}:{port}/json/", timeout=10) as r:
    j = json.load(r)
alive = j.get("aliveworkers", 0)
hosts = sorted({w.get("host", "?") for w in j.get("workers", [])})
print(f"aliveworkers={alive} worker hosts={hosts}")
assert alive == 2, f"expected 2 alive workers, got {alive}"
assert len(hosts) == 2, f"workers are not on distinct hosts: {hosts}"
PYEOF
mh_status "cluster gate PASS (2 alive workers, distinct hosts)"

echo "=== [2] dataset generation (SHA-256 counts gate) ==="
if [ -f "$DATA_DIR/counts_summary.json" ] && grep -q '"headline_expected_check": "PASS"' "$DATA_DIR/counts_summary.json"; then
  echo "dataset already present with PASS counts summary -- reusing $DATA_DIR"
else
  "$PY" gen_on_pod.py --family zipf --seed 101 --n-fact 10000000 \
    --n-fact2 6000000 --outdir "$DATA_DIR" 2>&1 | tee "$RESULTS_OUT/gen_zipf_s101.log"
  grep -q '"headline_expected_check": "PASS"' "$DATA_DIR/counts_summary.json" || {
    echo "ERROR: SHA-256 counts gate did not PASS -- ABORTING (do not relax the check)." >&2
    mh_status "ABORT: counts SHA gate failed"
    exit 1
  }
fi
mh_status "dataset ready at $DATA_DIR (counts SHA gate PASS)"

echo "=== [3] spark-submit: 9 pipelines ==="
"$SPARK_HOME/bin/spark-submit" \
  --master "$MASTER_URL" \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --total-executor-cores "$(( 2 * ${WORKER_CORES:-4} ))" \
  spark_pipelines.py --data "$DATA_DIR" \
  --manifest "$MANIFEST" \
  --labelling "$BAS_LABEL" \
  2>&1 | tee "$RESULTS_OUT/spark_zipf_s101.log"
mh_status "pipelines done"

echo "=== [4] cross-host sanity gate (executor hosts in event log) ==="
"$PY" - "$EVENTLOG_DIR" "$WORKER_B_PUBLIC_HOST" "$WORKER_C_PUBLIC_HOST" "$RESULTS_OUT/executor_hosts.json" <<'PYEOF'
import glob, json, os, sys
evdir, wb, wc, out = sys.argv[1:5]
hosts = {}
for path in glob.glob(os.path.join(evdir, "*")):
    if not os.path.isfile(path):
        continue
    with open(path, "rb") as fh:
        for raw in fh:
            try:
                ev = json.loads(raw)
            except Exception:
                continue
            if ev.get("Event") == "SparkListenerExecutorAdded":
                info = ev.get("Executor Info", {})
                hosts[info.get("Executor Host", "?")] = info.get("Executor ID")
with open(out, "w") as f:
    json.dump(hosts, f, indent=1)
found = set(hosts)
print(f"executor hosts seen in event log: {sorted(found)}")
missing = {wb, wc} - found
if missing:
    print(f"CROSS-HOST GATE FAILED: no executors registered from {sorted(missing)}. "
          "Outputs preserved; do NOT cite this run as multi-host. "
          "See OPERATOR_RUNBOOK.md failure diagnosis F2/F3.")
    sys.exit(1)
print("cross-host gate PASS: executors ran on both worker hosts")
PYEOF
mh_status "cross-host gate PASS (executors on both worker hosts)"

echo "=== [5] collect (explicit T=200000, predictions included) ==="
"$PY" collect_results.py \
  --manifest "$MANIFEST" \
  --predictions staging/predictions_main.csv \
  --summary staging/summary.json \
  --t-load 200000 \
  --out "$RESULTS_OUT/spark_ground_truth_zipf_s101.json" \
  2>&1 | tee "$RESULTS_OUT/collect_zipf_s101.log"
mh_status "collect done"

echo "=== [6] archive master/driver logs + listing ==="
mkdir -p "$MH_ROOT/logs_master"
cp -f "$BAS_HOME_MH"/logs/* "$MH_ROOT/logs_master/" 2>/dev/null || true
cp -f "$MANIFEST" "$RESULTS_OUT/" || true
mh_status "run_master complete"
echo "DONE. Ground truth: $RESULTS_OUT/spark_ground_truth_zipf_s101.json"
echo "Ask each worker host to run: bash run_worker.sh --archive"
echo "Final run directory listing:"
find "$MH_ROOT" -type f -printf '%s %p\n' | sort -k2
