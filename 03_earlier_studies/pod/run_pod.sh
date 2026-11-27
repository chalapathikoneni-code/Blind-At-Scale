#!/usr/bin/env bash
# run_pod.sh -- steps 2-5 of README_POD.md, in order, with logs.
# Assumes setup.sh has completed and the cluster is up.
# Usage:  bash run_pod.sh [smoke]
#   (no arg)  headline run: zipf seed 101, fact 10M / fact2 6M
#   smoke     zipf seed 999, fact 500k / fact2 300k (pipeline check only;
#             verdicts use T = 0.02 * n_fact = 10,000 -- NOT headline truth)
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "smoke" ]; then
  SEED=999; NF=500000; NF2=300000; TAG=smoke_zipf
else
  SEED=101; NF=10000000; NF2=6000000; TAG=zipf_s101
fi
DATA="$HOME/bas-data/$TAG"
export EVENTLOG_DIR="${EVENTLOG_DIR:-$HOME/bas-spark/eventlog}"
mkdir -p "$DATA" results_out

if [ -z "${SPARK_HOME:-}" ]; then
  SPARK_HOME="$(ls -d /opt/spark-*-bin-hadoop3 "$HOME"/opt/spark-*-bin-hadoop3 2>/dev/null | head -1)"
fi
[ -n "$SPARK_HOME" ] && [ -x "$SPARK_HOME/bin/spark-submit" ] || {
  echo "SPARK_HOME not found -- run setup.sh first." >&2; exit 1; }
echo "SPARK_HOME=$SPARK_HOME DATA=$DATA EVENTLOG_DIR=$EVENTLOG_DIR"

echo "=== gen_on_pod (seed $SEED, fact $NF) ==="
python3 gen_on_pod.py --family zipf --seed "$SEED" --n-fact "$NF" \
  --n-fact2 "$NF2" --outdir "$DATA" 2>&1 | tee "results_out/gen_${TAG}.log"

echo "=== spark_pipelines (standalone cluster) ==="
CORES="$(curl -fs http://127.0.0.1:8080/json/ 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("cores",""))' 2>/dev/null || echo "")"
[ -n "$CORES" ] || CORES="$(nproc)"
"$SPARK_HOME/bin/spark-submit" \
  --master spark://127.0.0.1:7077 \
  --driver-memory 2g \
  --total-executor-cores "$CORES" \
  spark_pipelines.py --data "$DATA" \
  --manifest "$HOME/bas-spark/run_manifest_${TAG}.json" \
  2>&1 | tee "results_out/spark_${TAG}.log"

echo "=== collect_results ==="
PREDS=""
[ -f ../data/zipf_s101/predictions_main.csv ] && PREDS="--predictions ../data/zipf_s101/predictions_main.csv"
SUMM=""
[ -f ../results/summary.json ] && SUMM="--summary ../results/summary.json"
python3 collect_results.py \
  --manifest "$HOME/bas-spark/run_manifest_${TAG}.json" \
  $PREDS $SUMM \
  --out "results_out/spark_ground_truth_${TAG}.json" \
  2>&1 | tee "results_out/collect_${TAG}.log"

echo "DONE. Results: $(pwd)/results_out/spark_ground_truth_${TAG}.json"
echo "REMEMBER: download results_out/ off the pod, then TERMINATE the pod."
