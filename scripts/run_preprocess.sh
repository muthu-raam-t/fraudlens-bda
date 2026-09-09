#!/usr/bin/env bash
# =============================================================================
# PHASE 4 -- run the Scala preprocessing job on Spark, managed by YARN.
#
# All preprocessing logic is in Scala (spark-apps/preprocess.scala). This
# script only submits it.
#
# spark-shell -i runs a Scala script against a live SparkSession, which needs
# no sbt/Maven build inside the container. The job is still genuine Scala and
# still runs distributed on YARN -- check Application Type = SPARK at :8088.
# =============================================================================
set -uo pipefail

SM="${SM_SERVICE:-spark-master}"
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
NUM_EXEC="${SPARK_NUM_EXEC:-2}"
EXEC_MEM="${SPARK_EXEC_MEM:-2g}"
EXEC_CORES="${SPARK_EXEC_CORES:-2}"
DRIVER_MEM="${SPARK_DRIVER_MEM:-2g}"

echo "=============================================================="
echo " PHASE 4: Scala preprocessing on Spark / YARN"
echo "=============================================================="
echo " executors : $NUM_EXEC x $EXEC_MEM / $EXEC_CORES cores"
echo " driver    : $DRIVER_MEM"
echo " watch     : http://localhost:8088  (Application Type = SPARK)"
echo "             http://localhost:4040  (stages, while running)"
echo "--------------------------------------------------------------"

echo ">>> Checking the ZIP lookup table is in HDFS (needed for the broadcast join)"
if ! docker compose exec -T "$NN" hdfs dfs -test -e "$HDFS_BASE/lookup/zip_coords.csv"; then
  echo "    missing -- uploading from data/source/"
  docker compose exec -T "$NN" hdfs dfs -put -f /source/zip_coords.csv "$HDFS_BASE/lookup/" \
    || { echo "ERROR: could not upload zip_coords.csv" >&2; exit 1; }
fi
docker compose exec -T "$NN" hdfs dfs -du -h "$HDFS_BASE/lookup/zip_coords.csv"

echo ">>> Input"
docker compose exec -T "$NN" hdfs dfs -du -s -h "$HDFS_BASE/dataset/unprocessed"

echo
echo ">>> Submitting  (started $(date '+%H:%M:%S'))"
echo "    NOTE: the job makes several passes over 28 GB (count in, write,"
echo "          count out for verification). Expect 25-45 minutes."
echo "--------------------------------------------------------------"
START=$(date +%s)

docker compose exec -T "$SM" bash -c "
  export SPARK_HOME=/spark
  export HADOOP_CONF_DIR=/etc/hadoop/conf
  export YARN_CONF_DIR=/etc/hadoop/conf
  export PATH=\$PATH:\$SPARK_HOME/bin
  spark-shell \
    --master yarn \
    --deploy-mode client \
    --name FraudLens-Phase4-ScalaPreprocessing \
    --num-executors $NUM_EXEC \
    --executor-memory $EXEC_MEM \
    --executor-cores $EXEC_CORES \
    --driver-memory $DRIVER_MEM \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.driver.maxResultSize=512m \
    -i /spark-apps/preprocess.scala
"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "PREPROCESSING FAILED (exit $STATUS) after $((ELAPSED/60))m $((ELAPSED%60))s" >&2
  echo "Check the stage that failed at http://localhost:8088" >&2
  exit $STATUS
fi

printf '\n=== PHASE 4 WALL CLOCK: %dm %ds ===\n' $((ELAPSED/60)) $((ELAPSED%60))

echo
echo ">>> Output in HDFS"
docker compose exec -T "$NN" hdfs dfs -du -s -h "$HDFS_BASE/dataset/preprocessed"
echo
echo ">>> Partition directories (proof of partitionBy year/month)"
docker compose exec -T "$NN" hdfs dfs -ls "$HDFS_BASE/dataset/preprocessed" | head -12
echo
echo ">>> Raw vs preprocessed size"
docker compose exec -T "$NN" hdfs dfs -du -s -h \
  "$HDFS_BASE/dataset/unprocessed" "$HDFS_BASE/dataset/preprocessed"
echo "    (the drop is Parquet + Snappy columnar compression, NOT lost rows --"
echo "     the row-count reconciliation printed above is the number to quote)"
echo
echo ">>> Readable CSV sample"
docker compose exec -T "$NN" hdfs dfs -ls "$HDFS_BASE/dataset/preprocessed_sample_csv"
echo
echo "Next: bash scripts/run_spark_agg.sh   (MapReduce vs Spark benchmark)"
