#!/usr/bin/env bash
# PHASE 7b -- pull the exported sample out of HDFS, then train CatBoost on the
# HOST (in the venv). CatBoost/SHAP need Python 3.7+, and the Spark containers
# ship 3.5, so this part deliberately runs outside the cluster.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"
SRC="$HDFS_BASE/dataset/catboost_sample"
DEST="data/catboost_sample"

echo "=============================================================="
echo " PHASE 7b: CatBoost serving model + SHAP"
echo "=============================================================="

if [ -z "${VIRTUAL_ENV:-}" ]; then
  echo "WARNING: the venv does not look active."
  echo "Run:  source .venv/bin/activate"
  echo
fi

echo ">>> Checking catboost and scikit-learn are installed"
CHECK=$(python3 -c "
import importlib.util as u
missing = [m for m in ('catboost', 'sklearn') if u.find_spec(m) is None]
print(','.join(missing))
" 2>/dev/null)

if [ -n "$CHECK" ]; then
  echo "    MISSING: $CHECK" >&2
  echo >&2
  echo "Install them first:" >&2
  echo "  source .venv/bin/activate" >&2
  echo "  pip install catboost shap scikit-learn" >&2
  exit 1
fi
echo "    catboost and scikit-learn present"

echo ">>> Pulling the exported sample out of HDFS"
mkdir -p "$DEST"
for split in train test; do
  rm -rf "$DEST/$split"
  mkdir -p "$DEST/$split"
  if ! docker compose exec -T "$NN" hdfs dfs -test -d "$SRC/$split"; then
    echo "ERROR: $SRC/$split not in HDFS." >&2
    echo "Run scripts/run_export_sample.sh first." >&2
    exit 1
  fi
  # Stream each part file out of HDFS to the host.
  docker compose exec -T "$NN" bash -c "hdfs dfs -ls '$SRC/$split'" \
    | awk '{print $NF}' | grep -E 'part-.*\.csv$' \
    | while read -r remote; do
        local_name="$DEST/$split/$(basename "$remote")"
        echo "    $remote -> $local_name"
        docker compose exec -T "$NN" bash -c "hdfs dfs -cat '$remote'" > "$local_name"
      done
done
du -sh "$DEST"/* 2>/dev/null

# ---------------------------------------------------------------------------
# CatBoost runs on the HOST, not in the cluster. Leaving 6 GB of YARN
# containers running while pandas loads millions of rows is what pushes a
# 16 GB machine into OOM. The Spark cluster is not needed from here on, so
# stop it and hand the memory back.
# ---------------------------------------------------------------------------
echo
echo ">>> Free memory before training:"
free -g | awk 'NR<=2'
if [ "${KEEP_CLUSTER_UP:-0}" != "1" ]; then
  echo ">>> Stopping the Spark/Hadoop containers to free RAM"
  echo "    (HDFS data is in named volumes and is NOT affected)"
  echo "    restart later with:  docker compose start"
  docker compose stop
  sleep 3
  echo ">>> Free memory after stopping containers:"
  free -g | awk 'NR<=2'
fi

echo
echo ">>> Training"
echo "--------------------------------------------------------------"
START=$(date +%s)
python3 scripts/train_catboost.py "$@"
STATUS=$?
ELAPSED=$(( $(date +%s) - START ))
echo "--------------------------------------------------------------"

if [ $STATUS -ne 0 ]; then
  echo "FAILED (exit $STATUS) after $((ELAPSED/60))m $((ELAPSED%60))s" >&2
  exit $STATUS
fi

printf 'WALL CLOCK: %dm %ds\n' $((ELAPSED/60)) $((ELAPSED%60))
echo
echo ">>> Artifacts"
ls -lh artifacts/catboost_model.cbm artifacts/catboost_metrics.json \
       artifacts/catboost_feature_importance.json 2>/dev/null
echo
echo "Next: bash scripts/run_aggregates.sh"
