#!/usr/bin/env bash
# PHASE 12b -- Spark Structured Streaming consuming from Kafka.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SM="${SM_SERVICE:-spark-master}"
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
NUM_EXEC="${SPARK_NUM_EXEC:-2}"
EXEC_MEM="${SPARK_EXEC_MEM:-2g}"
EXEC_CORES="${SPARK_EXEC_CORES:-2}"
DRIVER_MEM="${SPARK_DRIVER_MEM:-2g}"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"

echo "=============================================================="
echo " PHASE 12b: Kafka -> Spark Structured Streaming"
echo "=============================================================="

echo ">>> Waiting for HDFS to leave safe mode"
until docker compose exec -T "$NN" hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  echo "    still in safe mode..."
  sleep 5
done
echo "    safe mode OFF"

if ! docker compose exec -T "$NN" hdfs dfs -test -d "$HDFS_BASE/models/gbt"; then
  echo "ERROR: no saved GBT model at $HDFS_BASE/models/gbt" >&2
  echo "Run scripts/run_train.sh first." >&2
  exit 1
fi

if ! docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  echo "ERROR: the Kafka broker is not running." >&2
  echo "Run: bash scripts/setup_kafka.sh" >&2
  exit 1
fi

echo ">>> Clearing the previous checkpoint and verdicts"
docker compose exec -T "$NN" hdfs dfs -rm -r -skipTrash \
  "$HDFS_BASE/streaming/kafka_checkpoint" \
  "$HDFS_BASE/streaming/kafka_verdicts" 2>/dev/null || true
rm -rf artifacts/stream_verdicts && mkdir -p artifacts/stream_verdicts

echo
echo " executors : $NUM_EXEC x $EXEC_MEM / $EXEC_CORES cores"
echo " connector : $KAFKA_PKG"
echo "             (downloaded from Maven Central on first run, then cached)"
echo " In other terminals:"
echo "   bash scripts/run_producer.sh"
echo "   bash scripts/run_streamlit.sh"
echo "--------------------------------------------------------------"

docker compose exec -T "$SM" bash -c "
  export SPARK_HOME=/spark
  export HADOOP_CONF_DIR=/etc/hadoop/conf
  export YARN_CONF_DIR=/etc/hadoop/conf
  export PATH=\$PATH:\$SPARK_HOME/bin
  spark-shell \
    --master yarn \
    --deploy-mode client \
    --name FraudLens-Phase12-KafkaStreaming \
    --packages $KAFKA_PKG \
    --num-executors $NUM_EXEC \
    --executor-memory $EXEC_MEM \
    --executor-cores $EXEC_CORES \
    --driver-memory $DRIVER_MEM \
    --conf spark.sql.streaming.schemaInference=false \
    --conf spark.ui.filters= \
    --conf spark.ui.port=4040 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://namenode:9000/spark-logs \
    -i /spark-apps/stream_kafka.scala
"
