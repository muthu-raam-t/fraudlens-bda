#!/usr/bin/env bash
# PHASE 7a -- Export stratified sample for CatBoost
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
source scripts/_spark_submit.sh

echo "=============================================================="
echo " PHASE 7a: Export stratified sample for CatBoost"
echo "=============================================================="

START=$(date +%s)
fraudlens_spark_submit "/spark-apps/export_sample.py" "FraudLens-Phase7a-ExportSample" "$@"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "FAILED (exit $STATUS) after $((ELAPSED/60))m $((ELAPSED%60))s" >&2
  echo "Inspect the failed stage at http://localhost:8088" >&2
  exit $STATUS
fi

printf 'WALL CLOCK: %dm %ds\n' $((ELAPSED/60)) $((ELAPSED%60))
echo "Next: bash scripts/run_catboost.sh"
