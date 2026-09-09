#!/usr/bin/env bash
# PHASE 5 -- Spark MLlib training (LR / RF / GBT)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
source scripts/_spark_submit.sh

echo "=============================================================="
echo " PHASE 5: Spark MLlib training (LR / RF / GBT)"
echo "=============================================================="

START=$(date +%s)
fraudlens_spark_submit "/spark-apps/train.py" "FraudLens-Phase5-MLlibTraining" "$@"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "FAILED (exit $STATUS) after $((ELAPSED/60))m $((ELAPSED%60))s" >&2
  echo "Inspect the failed stage at http://localhost:8088" >&2
  exit $STATUS
fi

printf 'WALL CLOCK: %dm %ds\n' $((ELAPSED/60)) $((ELAPSED%60))
echo "Next: bash scripts/run_tuning.sh"
