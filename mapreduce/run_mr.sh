#!/usr/bin/env bash
# =============================================================================
# PHASE 3 -- MapReduce: fraud rate by merchant state
#
# Runs a genuine MAPREDUCE application on YARN using Hadoop Streaming, over the
# full 28 GB of RAW (unprocessed) CSV in HDFS.
#
# While it runs, open http://localhost:8088 -- the job appears with
# Application Type = MAPREDUCE. Screenshot that: it is the evidence that the
# MapReduce requirement is satisfied, not just Spark-on-YARN.
#
# The wall-clock time printed at the end is the BASELINE that
# spark-apps/spark_same_agg.py is compared against.
# =============================================================================
set -uo pipefail

NM="${NM_SERVICE:-nodemanager}"
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
INPUT="$HDFS_BASE/dataset/unprocessed"
OUTPUT="$HDFS_BASE/mapreduce_output/fraud_by_state"
REDUCERS="${REDUCERS:-4}"

echo "=============================================================="
echo " PHASE 3: MapReduce -- fraud rate by merchant state"
echo "=============================================================="

# --- 1. python3 must exist in the NodeManager (Hadoop Streaming runs the
#        mapper/reducer there as subprocesses) --------------------------------
echo ">>> Checking for python3 in the $NM container"
if docker compose exec -T "$NM" bash -c "command -v python3 >/dev/null 2>&1"; then
  echo "    $(docker compose exec -T "$NM" python3 --version 2>&1 | tr -d '\r')"
else
  echo "    not present -- installing (archive.debian.org: stretch is EOL)"
  docker compose exec -T -u root "$NM" bash -c '
    set -e
    printf "%s\n" \
      "deb http://archive.debian.org/debian stretch main" \
      "deb http://archive.debian.org/debian-security stretch/updates main" \
      > /etc/apt/sources.list
    apt-get -o Acquire::Check-Valid-Until=false update -qq
    apt-get install -y --no-install-recommends python3
  ' >/dev/null 2>&1 || {
      echo "ERROR: could not install python3 in $NM." >&2
      echo "Run it verbosely to see why:" >&2
      echo "  bash scripts/setup_nodemanager_python.sh" >&2
      exit 1
    }
  echo "    installed: $(docker compose exec -T "$NM" python3 --version 2>&1 | tr -d '\r')"
  echo "    NOTE: lost if the container is recreated -- re-run"
  echo "          scripts/setup_nodemanager_python.sh if so."
fi

# --- 2. Locate the streaming jar (path varies between Hadoop builds) --------
echo ">>> Locating hadoop-streaming.jar"
STREAMING_JAR=$(docker compose exec -T "$NN" bash -c \
  "ls /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1" | tr -d '\r')
if [ -z "$STREAMING_JAR" ]; then
  echo "ERROR: hadoop-streaming.jar not found in the namenode container." >&2
  exit 1
fi
echo "    $STREAMING_JAR"

# --- 3. MapReduce refuses to start if the output path already exists --------
echo ">>> Clearing any previous output"
docker compose exec -T "$NN" hdfs dfs -rm -r -skipTrash "$OUTPUT" 2>/dev/null \
  && echo "    removed previous $OUTPUT" \
  || echo "    (nothing to remove)"

echo ">>> Input"
docker compose exec -T "$NN" hdfs dfs -du -s -h "$INPUT"

# --- 4. Submit -------------------------------------------------------------
echo
echo ">>> Submitting to YARN  (watch http://localhost:8088)"
echo "    started at $(date '+%H:%M:%S')"
echo "--------------------------------------------------------------"
START=$(date +%s)

docker compose exec -T "$NN" bash -c "
  hadoop jar '$STREAMING_JAR' \
    -D mapreduce.job.name='FraudLens-FraudRateByState' \
    -D mapreduce.job.reduces=$REDUCERS \
    -files /mapreduce/mapper.py,/mapreduce/reducer.py \
    -input '$INPUT' \
    -output '$OUTPUT' \
    -mapper 'python3 mapper.py' \
    -reducer 'python3 reducer.py'
"
STATUS=$?
END=$(date +%s)
ELAPSED=$((END - START))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "JOB FAILED (exit $STATUS) after ${ELAPSED}s" >&2
  echo "Check the failed task logs at http://localhost:8088" >&2
  exit $STATUS
fi

# --- 5. Results ------------------------------------------------------------
printf '\n=== MAPREDUCE ELAPSED: %dm %ds (%d seconds) ===\n' \
  $((ELAPSED / 60)) $((ELAPSED % 60)) "$ELAPSED"
echo "Record this number -- it is the baseline for the Spark comparison."

echo
echo ">>> Output files in HDFS"
docker compose exec -T "$NN" hdfs dfs -ls "$OUTPUT"

echo
echo "=============================================================="
echo " TOP 15 STATES BY TRANSACTION VOLUME"
echo "=============================================================="
printf '%-10s %15s %12s %10s\n' "STATE" "TRANSACTIONS" "FRAUD" "RATE %"
printf '%-10s %15s %12s %10s\n' "-----" "------------" "-----" "------"
docker compose exec -T "$NN" bash -c "hdfs dfs -cat '$OUTPUT/part-*'" 2>/dev/null \
  | sort -t$'\t' -k2 -nr \
  | head -15 \
  | awk -F'\t' '{printf "%-10s %15s %12s %10s\n", $1, $2, $3, $4}'

echo
echo "=============================================================="
echo " HIGHEST FRAUD RATES (states with >100k transactions)"
echo "=============================================================="
printf '%-10s %15s %12s %10s\n' "STATE" "TRANSACTIONS" "FRAUD" "RATE %"
printf '%-10s %15s %12s %10s\n' "-----" "------------" "-----" "------"
docker compose exec -T "$NN" bash -c "hdfs dfs -cat '$OUTPUT/part-*'" 2>/dev/null \
  | awk -F'\t' '$2 > 100000' \
  | sort -t$'\t' -k4 -nr \
  | head -10 \
  | awk -F'\t' '{printf "%-10s %15s %12s %10s\n", $1, $2, $3, $4}'

echo
echo ">>> Row-count reconciliation (must match the raw dataset)"
TOTAL=$(docker compose exec -T "$NN" bash -c "hdfs dfs -cat '$OUTPUT/part-*'" 2>/dev/null \
  | awk -F'\t' '{s+=$2} END {print s}')
FRAUDS=$(docker compose exec -T "$NN" bash -c "hdfs dfs -cat '$OUTPUT/part-*'" 2>/dev/null \
  | awk -F'\t' '{s+=$3} END {print s}')
echo "    transactions counted : $TOTAL"
echo "    fraud transactions   : $FRAUDS"
if [ -n "$TOTAL" ] && [ "$TOTAL" -gt 0 ]; then
  awk -v f="$FRAUDS" -v t="$TOTAL" \
    'BEGIN {printf "    overall fraud rate   : %.4f%%\n", 100*f/t}'
fi

echo
echo "Next: bash scripts/run_spark_agg.sh  (same aggregation, in Spark)"
