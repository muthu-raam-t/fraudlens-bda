#!/usr/bin/env bash
# PHASE 11 -- Structured Streaming scorer (Velocity requirement).
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"

echo "=============================================================="
echo " PHASE 11: Spark Structured Streaming fraud scorer"
echo "=============================================================="

if ! docker compose exec -T "$NN" hdfs dfs -test -d "$HDFS_BASE/models/gbt"; then
  echo "ERROR: no saved GBT model at $HDFS_BASE/models/gbt" >&2
  echo "Run scripts/run_train.sh first." >&2
  exit 1
fi

echo ">>> Resetting the stream input and checkpoint"
docker compose exec -T "$NN" hdfs dfs -rm -r -skipTrash \
  "$HDFS_BASE/streaming/input" "$HDFS_BASE/streaming/checkpoint" \
  "$HDFS_BASE/streaming/verdicts" 2>/dev/null || true
docker compose exec -T "$NN" hdfs dfs -mkdir -p \
  "$HDFS_BASE/streaming/input" "$HDFS_BASE/streaming/checkpoint"
docker compose exec -T "$NN" hdfs dfs -chmod -R 777 "$HDFS_BASE/streaming"
rm -rf artifacts/stream_verdicts && mkdir -p artifacts/stream_verdicts

echo
echo ">>> Starting. In a SECOND terminal run:  bash scripts/feed_stream.sh"
echo ">>> Watch the Structured Streaming tab at http://localhost:4040"
echo "--------------------------------------------------------------"
# Scala, not PySpark: spark-master ships Python 3.7 (Alpine) while the
# nodemanager ships Python 3.5 (Debian 9), and PySpark refuses to run across
# minor versions. Scala starts no Python workers, so the mismatch is moot.
SM="${SM_SERVICE:-spark-master}"
NUM_EXEC="${SPARK_NUM_EXEC:-2}"
EXEC_MEM="${SPARK_EXEC_MEM:-2g}"
EXEC_CORES="${SPARK_EXEC_CORES:-2}"
DRIVER_MEM="${SPARK_DRIVER_MEM:-2g}"

echo " executors : $NUM_EXEC x $EXEC_MEM / $EXEC_CORES cores"
echo " engine    : Scala (spark-shell -i)"
echo "--------------------------------------------------------------"

docker compose exec -T "$SM" bash -c "
  export SPARK_HOME=/spark
  export HADOOP_CONF_DIR=/etc/hadoop/conf
  export YARN_CONF_DIR=/etc/hadoop/conf
  export PATH=\$PATH:\$SPARK_HOME/bin
  spark-shell \
    --master yarn \
    --deploy-mode client \
    --name FraudLens-Phase11-StreamingScorer \
    --num-executors $NUM_EXEC \
    --executor-memory $EXEC_MEM \
    --executor-cores $EXEC_CORES \
    --driver-memory $DRIVER_MEM \
    --conf spark.sql.streaming.schemaInference=false \
    -i /spark-apps/stream_score.scala
"
