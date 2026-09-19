#!/usr/bin/env bash
# PHASE 9 -- start the FastAPI inference service on the HOST.
# The Spark cluster is NOT needed for this; the CatBoost model is a local file.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PORT="${API_PORT:-8000}"

echo "=============================================================="
echo " PHASE 9: FastAPI inference service"
echo "=============================================================="

if [ ! -f artifacts/catboost_model.cbm ]; then
  echo "ERROR: artifacts/catboost_model.cbm missing." >&2
  echo "Run scripts/run_catboost.sh first." >&2
  exit 1
fi

MISSING=$(python3 -c "
import importlib.util as u
print(','.join(m for m in ('fastapi','uvicorn','catboost') if u.find_spec(m) is None))
" 2>/dev/null)
if [ -n "$MISSING" ]; then
  echo ">>> Installing missing packages: $MISSING"
  pip install -q -r api/requirements.txt || {
    echo "ERROR: pip install failed. Is the venv active?" >&2; exit 1; }
fi

echo ">>> Model   : artifacts/catboost_model.cbm"
echo ">>> Aggregates: $(ls artifacts/aggregates/*.json 2>/dev/null | wc -l) files"
echo ">>> API     : http://localhost:$PORT"
echo ">>> Swagger : http://localhost:$PORT/docs"
echo "--------------------------------------------------------------"
exec python3 -m uvicorn api.main:app --host 0.0.0.0 --port "$PORT" --reload
