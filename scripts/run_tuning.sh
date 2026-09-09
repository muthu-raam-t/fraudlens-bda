#!/usr/bin/env bash
# PHASE 6 -- Performance tuning benchmarks
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
source scripts/_spark_submit.sh

echo "=============================================================="
echo " PHASE 6: Performance tuning benchmarks"
echo "=============================================================="

START=$(date +%s)
fraudlens_spark_submit "/spark-apps/tuning_benchmark.py" "FraudLens-Phase6-TuningBenchmark" "$@"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "FAILED (exit $STATUS) after $((ELAPSED/60))m $((ELAPSED%60))s" >&2
  echo "Inspect the failed stage at http://localhost:8088" >&2
  exit $STATUS
fi

printf 'WALL CLOCK: %dm %ds\n' $((ELAPSED/60)) $((ELAPSED%60))
echo "Next: bash scripts/run_export_sample.sh"
