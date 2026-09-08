#!/usr/bin/env bash
# Generate -> upload -> delete, one chunk at a time.
# Local disk therefore never holds more than a single chunk.
set -euo pipefail

TOTAL_CHUNKS="${TOTAL_CHUNKS:-5}"
TARGET_GB_PER_CHUNK="${TARGET_GB_PER_CHUNK:-5.6}"
SOURCE_CSV="${SOURCE_CSV:-data/source/credit_card_transactions-ibm_v2.csv}"
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
DEST="$HDFS_BASE/dataset/unprocessed"

if [ ! -f "$SOURCE_CSV" ]; then
  echo "ERROR: source dataset not found at $SOURCE_CSV" >&2
  exit 1
fi

if [ ! -f data/source/zip_coords.csv ]; then
  echo ">>> Building the ZIP coordinate lookup table (one-off)"
  python3 scripts/build_zip_lookup.py --source "$SOURCE_CSV" --out data/source/zip_coords.csv
fi

START=$(date +%s)
for i in $(seq 1 "$TOTAL_CHUNKS"); do
  echo
  echo "==================== CHUNK $i / $TOTAL_CHUNKS ===================="

  if docker compose exec -T "$NN" hdfs dfs -test -e "$DEST/chunk_$i.csv" 2>/dev/null; then
    echo ">>> chunk_$i.csv is already in HDFS -- skipping"
    continue
  fi

  rm -f "data/raw/chunk_$i.csv" "data/raw/chunk_$i.manifest.json"

  echo ">>> Generating"
  python3 scripts/generate_chunk.py \
    --chunk "$i" \
    --total-chunks "$TOTAL_CHUNKS" \
    --target-gb "$TARGET_GB_PER_CHUNK" \
    --source "$SOURCE_CSV"

  echo ">>> Uploading to HDFS"
  docker compose exec -T "$NN" hdfs dfs -put -f "/staging/chunk_$i.csv" "$DEST/"

  echo ">>> Verifying"
  docker compose exec -T "$NN" hdfs dfs -du -h "$DEST/chunk_$i.csv"

  echo ">>> Freeing local disk"
  rm -f "data/raw/chunk_$i.csv"
done

echo
echo "==================== INGESTION COMPLETE ===================="
docker compose exec -T "$NN" hdfs dfs -du -s -h "$DEST"
docker compose exec -T "$NN" hdfs dfs -ls "$DEST"
echo
echo "Elapsed: $((($(date +%s) - START) / 60)) min"
