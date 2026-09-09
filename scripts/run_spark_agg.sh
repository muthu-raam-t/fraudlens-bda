#!/usr/bin/env bash
# Submits spark_same_agg.py to YARN -- the Spark half of the Phase 3 benchmark.
set -uo pipefail

SM="${SM_SERVICE:-spark-master}"
EXEC_MEM="${SPARK_EXEC_MEM:-2g}"
EXEC_CORES="${SPARK_EXEC_CORES:-2}"
NUM_EXEC="${SPARK_NUM_EXEC:-2}"

echo "=============================================================="
echo " PHASE 3 (part 2): Spark aggregation on YARN"
echo "=============================================================="
echo " executors: $NUM_EXEC x ${EXEC_MEM}/${EXEC_CORES} cores"
echo " watch: http://localhost:8088   (Application Type = SPARK)"
echo "--------------------------------------------------------------"

START=$(date +%s)

docker compose exec -T "$SM" bash -c "
  export SPARK_HOME=/spark
  export HADOOP_CONF_DIR=/etc/hadoop/conf
  export YARN_CONF_DIR=/etc/hadoop/conf
  export PATH=\$PATH:\$SPARK_HOME/bin
  export PYSPARK_PYTHON=python3
  export PYSPARK_DRIVER_PYTHON=python3
  spark-submit \
    --master yarn \
    --deploy-mode client \
    --name FraudLens-SparkAgg-FraudRateByState \
    --num-executors $NUM_EXEC \
    --executor-memory $EXEC_MEM \
    --executor-cores $EXEC_CORES \
    --driver-memory 1g \
    --conf spark.yarn.submit.waitAppCompletion=true \
    /spark-apps/spark_same_agg.py
"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))

echo "--------------------------------------------------------------"
if [ $STATUS -ne 0 ]; then
  echo "SPARK JOB FAILED (exit $STATUS) after ${ELAPSED}s" >&2
  exit $STATUS
fi

printf 'Total including JVM startup: %dm %ds\n' $((ELAPSED/60)) $((ELAPSED%60))
echo "(the in-job timing printed above excludes submission overhead --"
echo " use that one for the comparison)"
