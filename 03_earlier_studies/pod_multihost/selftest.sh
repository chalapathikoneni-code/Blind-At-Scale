#!/usr/bin/env bash
# selftest.sh -- LOCAL dry validation of the multi-host package.
# No pods, no spend, no Spark cluster. Run from the package directory:
#   bash selftest.sh
# Checks: shell syntax, python syntax, staging inputs, collect threshold
# handling (source + --help), Spark conf rendering from
# cluster.env.example (master and worker roles), and -- the heavyweight
# check -- a full local run of gen_on_pod.py at headline scale whose
# SHA-256 counts gate must PASS (uses a throwaway venv with pinned
# numpy==2.5.4 + pyarrow so the study venv is never modified).
set -euo pipefail
cd "$(dirname "$0")"
STUDY_PY="${STUDY_PY:-$HOME/workspace/ieee-access-stress-test/venv/bin/python}"
[ -x "$STUDY_PY" ] || { echo "ERROR: study venv python not found at $STUDY_PY" >&2; exit 1; }
TMP="$(pwd)/.selftest_tmp"
rm -rf "$TMP"; mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT
pass() { echo "SELFTEST PASS: $*"; }

echo "=== [1] bash -n on all shell scripts ==="
for f in lib_multihost.sh setup_multihost.sh run_master.sh run_worker.sh selftest.sh; do
  bash -n "$f" && echo "  syntax OK: $f"
done
pass "shell syntax"

echo "=== [2] python syntax (py_compile, study venv) ==="
"$STUDY_PY" -m py_compile gen_on_pod.py spark_pipelines.py collect_results.py
pass "python syntax"

echo "=== [3] staging inputs ==="
[ -s staging/predictions_main.csv ] || { echo "staging/predictions_main.csv missing/empty" >&2; exit 1; }
[ -s staging/summary.json ] || { echo "staging/summary.json missing/empty" >&2; exit 1; }
"$STUDY_PY" - <<'PYEOF'
import csv, json
rows = list(csv.DictReader(open("staging/predictions_main.csv")))
pipes = {"G_A","G_B","G_N","J_A","J_B","J_N","F_A","F_B","W_A"}
arms = {"A1","A5","A8"}
have = {(r["pipeline"], r["arm"]) for r in rows}
missing = {(p, a) for p in pipes for a in arms} - have
assert not missing, f"predictions missing rows: {sorted(missing)}"
assert {"arm","pipeline","gt_load","gt_verdict","pred_load","pred_verdict"} <= set(rows[0])
s = json.load(open("staging/summary.json"))
for a in arms:
    assert {"recall","precision","spearman_rho","median_q_error"} <= set(s["arms"][a]), a
print(f"  predictions: {len(rows)} rows, all 9 pipelines x A1/A5/A8 present; summary arms OK")
PYEOF
pass "staging inputs"

echo "=== [4] collect_results.py threshold handling ==="
grep -q '"--t-load"' collect_results.py && echo "  --t-load flag present in source"
grep -q 't_load = args.t_load if args.t_load is not None else 0.02 \* n_fact' collect_results.py \
  && echo "  default is 0.02*n_fact (=200,000 at headline); run scripts pass 200000 explicitly"
"$STUDY_PY" collect_results.py --help 2>&1 | grep -q -- '--t-load' && echo "  --help lists --t-load"
# Recompute the single-host recorded verdicts under T=200000 as a
# regression anchor for the explicit threshold (values: POD_RUN_RECORD).
"$STUDY_PY" - <<'PYEOF'
loads = {"G_A":1086736,"G_B":156387,"G_N":1043904,"J_A":1086890,"J_B":156541,
         "J_N":1044058,"F_A":546740246171,"F_B":3719508913,"W_A":1086736}
recorded = {"G_A":"FAIL","G_B":"PASS","G_N":"FAIL","J_A":"FAIL","J_B":"PASS",
            "J_N":"FAIL","F_A":"FAIL","F_B":"FAIL","W_A":"FAIL"}
for T in (200000, 20000):
    got = {p: ("FAIL" if l > T else "PASS") for p, l in loads.items()}
    print(f"  T={T}: {sum(got[p]==recorded[p] for p in loads)}/9 verdicts match the single-host record")
assert all(("FAIL" if l > 200000 else "PASS") == recorded[p] for p, l in loads.items())
PYEOF
pass "collect threshold"

echo "=== [5] conf rendering from cluster.env.example ==="
export MH_ENV="$(pwd)/cluster.env.example"
export MH_ROOT="$TMP/mh" BAS_HOME_MH="$TMP/bas" EVENTLOG_DIR="$TMP/mh/eventlog"
# shellcheck disable=SC1091
. ./lib_multihost.sh
mh_load_env
PY="$STUDY_PY" mh_render_conf "$TMP/conf_master"
for key in spark.eventLog.enabled spark.sql.shuffle.partitions spark.blockManager.port \
           spark.driver.host spark.driver.bindAddress spark.driver.port; do
  grep -q "^$key" "$TMP/conf_master/spark-defaults.conf" || { echo "missing key $key" >&2; exit 1; }
done
grep -q '^spark.driver.host *= *203.0.113.10' "$TMP/conf_master/spark-defaults.conf" || \
  grep -q 'spark.driver.host                203.0.113.10' "$TMP/conf_master/spark-defaults.conf"
grep -q 'spark.driver.port                41000' "$TMP/conf_master/spark-defaults.conf"
grep -q 'spark.blockManager.port          42000' "$TMP/conf_master/spark-defaults.conf"
echo "  master conf OK (driver host/port, block-manager port, event log, 128 partitions)"
ROLE=worker-b
mh_render_conf "$TMP/conf_workerb"
grep -q 'export SPARK_LOCAL_HOSTNAME=203.0.113.11' "$TMP/conf_workerb/spark-env.sh"
echo "  worker-b conf OK (advertised hostname = worker B public host)"
mh_port_guard && echo "  port guard accepts the example (all 1:1)"
pass "conf rendering"

echo "=== [6] gen SHA-256 validation at headline scale (scratch venv) ==="
"$STUDY_PY" -m venv "$TMP/venv"
"$TMP/venv/bin/python" -m pip install --quiet "numpy==2.5.4" pyarrow
"$TMP/venv/bin/python" -c 'import numpy, pyarrow; assert numpy.__version__=="2.5.4"; print(f"  scratch venv: numpy {numpy.__version__}, pyarrow {pyarrow.__version__}")'
"$TMP/venv/bin/python" gen_on_pod.py --family zipf --seed 101 \
  --n-fact 10000000 --n-fact2 6000000 --outdir "$TMP/gen" > "$TMP/gen_stdout.json"
"$TMP/venv/bin/python" - <<'PYEOF'
import json
s = json.load(open(".selftest_tmp/gen/counts_summary.json"))
assert s["headline_expected_check"] == "PASS", s
assert s["counts_sha256"] == "f9f3fa8b29356e870df37e292f755a03d20fdcd016484d79dbcced148dcb528f", s["counts_sha256"]
print(f"  counts_sha256={s['counts_sha256'][:16]}... headline_expected_check=PASS")
PYEOF
pass "gen SHA-256 validation"

echo
echo "ALL SELFTESTS PASSED"
