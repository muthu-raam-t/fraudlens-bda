#!/usr/bin/env bash
# =============================================================================
# ONE COMMAND to run the whole product: API + dashboard.
#
# WHY THIS EXISTS
#   The API (uvicorn) and the dashboard (vite) are both long-running
#   foreground servers. Each normally occupies a terminal. This script starts
#   the API in the background, waits for it to answer, then runs the dashboard
#   in the foreground -- so you get one terminal and Ctrl-C stops both.
#
# Usage:  bash scripts/start_app.sh
#         Ctrl-C to stop everything.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

API_PORT="${API_PORT:-8000}"
LOG="/tmp/fraudlens-api.log"
API_PID=""

cleanup() {
  echo
  echo ">>> Shutting down"
  if [ -n "$API_PID" ] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null
    wait "$API_PID" 2>/dev/null
    echo "    API stopped"
  fi
}
trap cleanup EXIT INT TERM

echo "=============================================================="
echo " FraudLens — starting API and dashboard"
echo "=============================================================="

if [ ! -f artifacts/catboost_model.cbm ]; then
  echo "ERROR: artifacts/catboost_model.cbm missing." >&2
  echo "Run scripts/run_catboost.sh first." >&2
  exit 1
fi
if [ ! -d artifacts/aggregates ]; then
  echo "WARNING: artifacts/aggregates missing -- the analytics tab will be"
  echo "         empty. Run scripts/run_aggregates.sh to populate it."
fi

# --- API in the background -------------------------------------------------
if curl -s -o /dev/null -m 2 "http://localhost:$API_PORT/health"; then
  echo ">>> API already running on :$API_PORT — reusing it"
else
  MISSING=$(python3 -c "
import importlib.util as u
print(','.join(m for m in ('fastapi','uvicorn','catboost') if u.find_spec(m) is None))
" 2>/dev/null)
  if [ -n "$MISSING" ]; then
    echo ">>> Installing missing packages: $MISSING"
    pip install -q -r api/requirements.txt || {
      echo "ERROR: pip install failed. Activate the venv first:" >&2
      echo "  source .venv/bin/activate" >&2; exit 1; }
  fi

  echo ">>> Starting the API on :$API_PORT   (log: $LOG)"
  python3 -m uvicorn api.main:app --host 0.0.0.0 --port "$API_PORT" \
    > "$LOG" 2>&1 &
  API_PID=$!

  for i in $(seq 1 30); do
    if curl -s -o /dev/null -m 2 "http://localhost:$API_PORT/health"; then
      echo "    API is up"
      break
    fi
    if ! kill -0 "$API_PID" 2>/dev/null; then
      echo "ERROR: the API exited. Last lines of $LOG:" >&2
      tail -20 "$LOG" >&2
      exit 1
    fi
    sleep 1
  done

  if ! curl -s -o /dev/null -m 2 "http://localhost:$API_PORT/health"; then
    echo "ERROR: the API did not come up in 30s. See $LOG" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
fi

# --- dashboard in the foreground ------------------------------------------
if ! command -v npm >/dev/null 2>&1; then
  echo "ERROR: npm not found. Install Node.js:  sudo apt install -y nodejs npm" >&2
  exit 1
fi

cd ui || exit 1
if [ ! -d node_modules ]; then
  echo ">>> Installing dashboard dependencies (first run only, ~1 min)"
  npm install || { echo "ERROR: npm install failed" >&2; exit 1; }
fi

echo
echo "=============================================================="
echo "  Dashboard : http://localhost:5173"
echo "  API docs  : http://localhost:$API_PORT/docs"
echo "  Ctrl-C stops both."
echo "=============================================================="
echo
npm start
