#!/usr/bin/env bash
# Shared spark-submit helper. Sourced by the phase runner scripts.
#
# PYSPARK_PYTHON is set explicitly because the bde2020 Spark images can have
# more than one interpreter on PATH; a driver/executor mismatch produces a
# confusing "Python in worker has different version" error.
fraudlens_spark_submit() {
  local app="$1"; shift
  local name="$1"; shift
  local SM="${SM_SERVICE:-spark-master}"
  local NUM_EXEC="${SPARK_NUM_EXEC:-2}"
  local EXEC_MEM="${SPARK_EXEC_MEM:-2g}"
  local EXEC_CORES="${SPARK_EXEC_CORES:-2}"
  local DRIVER_MEM="${SPARK_DRIVER_MEM:-2g}"

  echo " app       : $app"
  echo " executors : $NUM_EXEC x $EXEC_MEM / $EXEC_CORES cores"
  echo " watch     : http://localhost:8088"
  echo "--------------------------------------------------------------"

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
      --name '$name' \
      --num-executors $NUM_EXEC \
      --executor-memory $EXEC_MEM \
      --executor-cores $EXEC_CORES \
      --driver-memory $DRIVER_MEM \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.driver.maxResultSize=512m \
      $app $*
  "
}
