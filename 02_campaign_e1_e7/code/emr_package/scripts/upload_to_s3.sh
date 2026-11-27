#!/usr/bin/env bash
# Upload the E1/E6 package code+config+scripts to S3.
set -euo pipefail
BUCKET="YOUR_BUCKET"
PREFIX="Access/e1e6"
REGION="us-east-1"
PKG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

aws s3 sync "$PKG_ROOT/code"    "s3://$BUCKET/$PREFIX/code"    --region "$REGION"
aws s3 sync "$PKG_ROOT/config"  "s3://$BUCKET/$PREFIX/config"  --region "$REGION"
aws s3 sync "$PKG_ROOT/scripts" "s3://$BUCKET/$PREFIX/scripts" --region "$REGION"
aws s3 cp   "$PKG_ROOT/docs/RUNBOOK.md" "s3://$BUCKET/$PREFIX/RUNBOOK.md" --region "$REGION"
echo "--- uploaded keys ---"
aws s3 ls --recursive "s3://$BUCKET/$PREFIX/code" --region "$REGION"
echo "UPLOAD OK"
