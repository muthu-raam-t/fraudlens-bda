#!/usr/bin/env bash
# PHASE 12a -- publish transaction events to Kafka.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
SAMPLE="data/stream_src/sample.csv"

echo "=============================================================="
echo " PHASE 12a: Kafka transaction producer"
echo "=============================================================="

if [ ! -s "$SAMPLE" ]; then
  echo ">>> Pulling the preprocessed sample out of HDFS"
  mkdir -p data/stream_src
  docker compose exec -T "$NN" bash -c \
    "hdfs dfs -cat $HDFS_BASE/dataset/preprocessed_sample_csv/part-*.csv" \
    > "$SAMPLE" 2>/dev/null
fi
if [ ! -s "$SAMPLE" ]; then
  echo "ERROR: could not fetch the sample. Run scripts/run_preprocess.sh first." >&2
  exit 1
fi
echo "    $(( $(wc -l < "$SAMPLE") - 1 )) rows available"

python3 -c "import kafka" 2>/dev/null || {
  echo ">>> Installing kafka-python"
  pip install -q kafka-python || { echo "pip install failed -- is the venv active?" >&2; exit 1; }
}

echo
exec python3 scripts/kafka_producer.py "$@"
