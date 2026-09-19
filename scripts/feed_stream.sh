#!/usr/bin/env bash
# Drip CSV files into the watched HDFS directory so the streaming job has
# something to consume. Run this in a SECOND terminal while run_streaming.sh
# is live.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
SRC="$HDFS_BASE/dataset/preprocessed_sample_csv"
DEST="$HDFS_BASE/streaming/input"
BATCHES="${BATCHES:-12}"
ROWS="${ROWS_PER_BATCH:-500}"
DELAY="${DELAY_SECONDS:-6}"

echo "=============================================================="
echo " Feeding the stream: $BATCHES files x $ROWS rows, every ${DELAY}s"
echo "=============================================================="

echo ">>> Pulling the sample out of HDFS"
mkdir -p data/stream_src
docker compose exec -T "$NN" bash -c "hdfs dfs -cat $SRC/part-*.csv" \
  > data/stream_src/sample.csv 2>/dev/null
TOTAL=$(wc -l < data/stream_src/sample.csv)
if [ "$TOTAL" -lt 100 ]; then
  echo "ERROR: sample CSV is empty. Run scripts/run_preprocess.sh first." >&2
  exit 1
fi
head -1 data/stream_src/sample.csv > data/stream_src/header.csv
tail -n +2 data/stream_src/sample.csv > data/stream_src/body.csv
echo "    $((TOTAL - 1)) rows available"

for i in $(seq 1 "$BATCHES"); do
  START=$(( (i - 1) * ROWS + 1 ))
  OUT="data/stream_src/live_$(printf '%03d' "$i").csv"
  cp data/stream_src/header.csv "$OUT"
  tail -n +"$START" data/stream_src/body.csv | head -n "$ROWS" >> "$OUT"
  LINES=$(( $(wc -l < "$OUT") - 1 ))
  if [ "$LINES" -le 0 ]; then
    echo ">>> sample exhausted after $((i - 1)) batches"
    break
  fi
  docker compose exec -T "$NN" bash -c \
    "hdfs dfs -put -f - $DEST/live_$(printf '%03d' "$i").csv" < "$OUT"
  echo "  [$i/$BATCHES] pushed $LINES rows -> $DEST/live_$(printf '%03d' "$i").csv"
  rm -f "$OUT"
  sleep "$DELAY"
done

echo
echo "Feed complete. The streaming job's batch counter should have advanced."
