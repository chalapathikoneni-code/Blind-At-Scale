#!/usr/bin/env bash
# Add ALL steps IN ORDER: 0 smoke, 1 prep, 2 E1, 3 E6-off, 4 E6-on,
# 5 A9-at-100M, 6 E1-scales-and-sources, 7a E6-synth-off, 7b E6-synth-on.
# Every step is ActionOnFailure=CONTINUE: a failed step never blocks
# the next one. Run each step's predecessor to COMPLETED before
# adding the next if you want to watch them one at a time - or add
# them all at once; EMR runs steps sequentially in submission order.
#
# Steps 5 and 7 are the SYNTHETIC extras (they reuse the Nov 23
# synthetic parquet in s3://YOUR_BUCKET/Access/synthetic/data). They can
# be dropped from the sequence without affecting anything else.
#
# Usage: CLUSTER_ID=j-XXXX scripts/add_steps.sh [STEP]
#   STEP = smoke | prep | e1 | e6 | a9 | e1more | e6synth | all
#   (default: all)
set -euo pipefail
BUCKET="YOUR_BUCKET"
PREFIX="Access/e1e6"
REGION="us-east-1"
CLUSTER_ID="${CLUSTER_ID:-${2:-}}"
WHICH="${1:-all}"
if [ -z "$CLUSTER_ID" ]; then
  echo "FATAL: CLUSTER_ID=j-XXXX scripts/add_steps.sh [STEP]" >&2; exit 1
fi

COMMON=(--master yarn --deploy-mode cluster
  --driver-memory 8g --executor-memory 8g --executor-cores 4
  --num-executors 18
  --conf spark.dynamicAllocation.enabled=false
  --conf spark.yarn.maxAppAttempts=1
  --conf spark.sql.shuffle.partitions=128
  --conf spark.eventLog.enabled=true
  --conf "spark.eventLog.dir=s3a://$BUCKET/$PREFIX/eventlog/")

add_step() {
  local name="$1"; shift
  local f; f="$(mktemp /tmp/bas-step.XXXXXX.json)"
  python3 - "$name" "$f" "$@" <<'PYEOF'
import json, sys
name, path, args = sys.argv[1], sys.argv[2], sys.argv[3:]
step = [{"Name": name, "ActionOnFailure": "CONTINUE",
         "HadoopJarStep": {"Jar": "command-runner.jar",
                           "Args": ["spark-submit"] + args}}]
open(path, "w").write(json.dumps(step))
PYEOF
  local id
  id="$(aws emr add-steps --cluster-id "$CLUSTER_ID" --region "$REGION" \
        --steps "file://$f" --query StepIds --output text)"
  rm -f "$f"
  echo "STEP ADDED [$name]: $id"
}

if [ "$WHICH" = "smoke" ] || [ "$WHICH" = "all" ]; then
  add_step "0-smoke" "${COMMON[@]}" \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/smoke.py" \
    --out "s3://$BUCKET/$PREFIX/output"
fi
if [ "$WHICH" = "prep" ] || [ "$WHICH" = "all" ]; then
  add_step "1-prep-openalex" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/prep_openalex.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX"
fi
if [ "$WHICH" = "e1" ] || [ "$WHICH" = "all" ]; then
  add_step "2-e1-join-validation" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/e1_join.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX"
fi
if [ "$WHICH" = "e6" ] || [ "$WHICH" = "all" ]; then
  add_step "3-e6-aqe-off" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/e6_aqe.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX" --aqe off
  add_step "4-e6-aqe-on" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/e6_aqe.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX" --aqe on
fi
if [ "$WHICH" = "a9" ] || [ "$WHICH" = "all" ]; then
  add_step "5-a9-at-100m" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py" \
    "s3://$BUCKET/$PREFIX/code/a9_100m.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX"
fi
if [ "$WHICH" = "e1more" ] || [ "$WHICH" = "all" ]; then
  add_step "6-e1-scales-and-sources" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py,s3://$BUCKET/$PREFIX/code/e1_join.py,s3://$BUCKET/$PREFIX/code/prep_openalex.py" \
    "s3://$BUCKET/$PREFIX/code/e1_more.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX"
fi
if [ "$WHICH" = "e6synth" ] || [ "$WHICH" = "all" ]; then
  add_step "7a-e6-synth-aqe-off" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=false \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py,s3://$BUCKET/$PREFIX/code/e6_aqe.py" \
    "s3://$BUCKET/$PREFIX/code/e6_synth.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX" --aqe off
  add_step "7b-e6-synth-aqe-on" "${COMMON[@]}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --py-files "s3://$BUCKET/$PREFIX/code/common.py,s3://$BUCKET/$PREFIX/code/e6_aqe.py" \
    "s3://$BUCKET/$PREFIX/code/e6_synth.py" \
    --s3-prefix "s3://$BUCKET/$PREFIX" --aqe on
fi
echo "Monitor: aws emr list-steps --cluster-id $CLUSTER_ID --region $REGION --query 'Steps[*].{Name:Name,State:Status.State}' --output table"
