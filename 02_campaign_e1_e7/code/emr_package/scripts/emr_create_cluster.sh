#!/usr/bin/env bash
# Create the EMR cluster for E1/E6. NO bootstrap action - nothing is
# installed at cluster start (that is what killed earlier attempts).
# The jobs use only pyspark + the Python standard library.
# Long-lived: NO auto-terminate. Terminate it when done (runbook).
set -euo pipefail
BUCKET="YOUR_BUCKET"
PREFIX="Access/e1e6"
REGION="us-east-1"
RELEASE="emr-7.14.0"   # Spark 3.5.8-amzn-0, same as the successful runs
CLUSTER_NAME="blind-at-scale-e1e6"
PKG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CLUSTER_ID="$(aws emr create-cluster \
  --name "$CLUSTER_NAME" \
  --release-label "$RELEASE" \
  --applications Name=Spark \
  --use-default-roles \
  --region "$REGION" \
  --log-uri "s3://$BUCKET/$PREFIX/logs/" \
  --enable-debugging \
  --instance-groups "file://$PKG_ROOT/config/instance_groups.json" \
  --tags Name=blind-at-scale-e1e6 Project=blind-at-scale \
  --query ClusterId --output text)"

echo "CLUSTER CREATED: $CLUSTER_ID"
echo "Wait for WAITING, then: CLUSTER_ID=$CLUSTER_ID scripts/add_steps.sh"
