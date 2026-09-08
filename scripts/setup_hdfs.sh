#!/usr/bin/env bash
# Create the HDFS directory layout for the project.
set -euo pipefail

NN="${NN_SERVICE:-namenode}"
BASE="${HDFS_BASE:-/fraudlens}"

echo "Waiting for the NameNode to leave safe mode..."
until docker compose exec -T "$NN" hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  echo "  ...still starting up"
  sleep 5
done

echo "Creating HDFS layout under $BASE"
docker compose exec -T "$NN" hdfs dfs -mkdir -p \
  "$BASE/dataset/unprocessed" \
  "$BASE/dataset/preprocessed" \
  "$BASE/dataset/preprocessed_sample_csv" \
  "$BASE/lookup" \
  "$BASE/mapreduce_output" \
  "$BASE/models" \
  "$BASE/aggregates" \
  "$BASE/streaming/input" \
  "$BASE/streaming/verdicts" \
  "$BASE/streaming/checkpoint"

docker compose exec -T "$NN" hdfs dfs -chmod -R 777 "$BASE"

if [ -f data/source/zip_coords.csv ]; then
  echo "Uploading the ZIP coordinate lookup table (broadcast-join table)..."
  docker compose exec -T "$NN" hdfs dfs -put -f /source/zip_coords.csv "$BASE/lookup/"
else
  echo "NOTE: data/source/zip_coords.csv not built yet -- scripts/ingest.sh will build it."
fi

echo
docker compose exec -T "$NN" hdfs dfs -ls -R "$BASE" | sed 's/^/  /'
echo
echo "HDFS layout ready."
