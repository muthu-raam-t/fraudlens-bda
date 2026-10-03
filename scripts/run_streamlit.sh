#!/usr/bin/env bash
# PHASE 12c -- live operations dashboard.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PORT="${STREAMLIT_PORT:-8501}"

echo "=============================================================="
echo " PHASE 12c: Streamlit live dashboard"
echo "=============================================================="

if [ -z "${VIRTUAL_ENV:-}" ]; then
  echo "WARNING: the venv does not look active."
  echo "Run:  source .venv/bin/activate"
  echo
fi

python3 -c "import streamlit" 2>/dev/null || {
  echo ">>> Installing streamlit"
  pip install -q streamlit || { echo "pip install failed" >&2; exit 1; }
}

mkdir -p artifacts/stream_verdicts
echo ">>> Dashboard: http://localhost:$PORT"
echo ">>> It refreshes every 3 s and fills in as batches are scored."
echo "--------------------------------------------------------------"

exec python3 -m streamlit run streamlit/app.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
