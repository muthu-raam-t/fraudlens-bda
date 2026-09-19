#!/usr/bin/env bash
# PHASE 10 -- React analytics dashboard (Vite dev server).
# The API must already be running: bash scripts/run_api.sh
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

echo "=============================================================="
echo " PHASE 10: React analytics dashboard"
echo "=============================================================="

if ! command -v npm >/dev/null 2>&1; then
  echo "ERROR: npm not found. Install Node.js first:" >&2
  echo "  sudo apt install -y nodejs npm" >&2
  exit 1
fi
echo ">>> node $(node --version)  npm $(npm --version)"

if ! curl -s -o /dev/null -m 3 http://localhost:8000/health; then
  echo
  echo "WARNING: the API is not answering on :8000."
  echo "Start it in another terminal:  bash scripts/run_api.sh"
  echo
fi

cd ui || exit 1
if [ ! -d node_modules ]; then
  echo ">>> Installing dependencies (first run only)"
  npm install || { echo "ERROR: npm install failed" >&2; exit 1; }
fi

echo ">>> Dashboard: http://localhost:5173"
echo "--------------------------------------------------------------"
exec npm run dev
