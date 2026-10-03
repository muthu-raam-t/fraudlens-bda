#!/usr/bin/env bash
# One-command live streaming demo.
# Spark and the producer run in the background; the dashboard runs in front.
# Ctrl-C stops everything.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

STREAM_LOG=/tmp/fraudlens-stream.log
PROD_LOG=/tmp/fraudlens-producer.log
PIDS=()

cleanup() {
  echo
  echo ">>> Stopping background jobs"
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null; done
  docker compose exec -T namenode yarn application -list 2>/dev/null \
    | grep -i "FraudLens-Phase12" | awk '{print $1}' \
    | xargs -r -n1 docker compose exec -T namenode yarn application -kill 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "=============================================================="
echo " FraudLens live streaming demo"
echo "=============================================================="

echo ">>> 1/4  Starting the Spark Kafka consumer"
nohup bash scripts/run_kafka_stream.sh > "$STREAM_LOG" 2>&1 &
PIDS+=($!)

echo "         waiting for it to subscribe (up to 3 min)"
for i in $(seq 1 180); do
  if grep -q "CONSUMING FROM KAFKA" "$STREAM_LOG" 2>/dev/null; then
    echo "         consumer ready"
    break
  fi
  printf "."
  sleep 2
done
echo

if ! grep -q "CONSUMING FROM KAFKA" "$STREAM_LOG" 2>/dev/null; then
  echo "ERROR: the consumer did not start. Last 25 lines:" >&2
  tail -25 "$STREAM_LOG" >&2
  exit 1
fi

echo ">>> 2/4  Starting the transaction producer"
nohup bash scripts/run_producer.sh --rate 4 --count 1200 --fraud-boost 0.03 \
  > "$PROD_LOG" 2>&1 &
PIDS+=($!)
sleep 3

echo ">>> 3/4  Watch the batches arrive:"
echo "         tail -f $STREAM_LOG"
echo "         tail -f $PROD_LOG"
echo
echo ">>> 4/4  Starting the dashboard"
echo
echo "=============================================================="
echo "  Dashboard : http://localhost:8501"
echo "  Spark UI  : http://localhost:8088  -> RUNNING -> ApplicationMaster"
echo "  Ctrl-C stops everything."
echo "=============================================================="
echo

bash scripts/run_streamlit.sh
