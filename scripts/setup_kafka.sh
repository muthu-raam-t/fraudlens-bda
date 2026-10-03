#!/usr/bin/env bash
# PHASE 12 -- bring up Kafka and create the topics.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

IN_TOPIC="${KAFKA_TOPIC:-transactions}"
OUT_TOPIC="${KAFKA_OUT_TOPIC:-verdicts}"

echo "=============================================================="
echo " PHASE 12: Kafka setup"
echo "=============================================================="

if ! grep -q "confluentinc/cp-kafka" docker-compose.yml 2>/dev/null; then
  echo ">>> Adding Kafka and Zookeeper to docker-compose.yml"
  python3 scripts/patch_compose_kafka.py || exit 1
fi

echo ">>> Starting Zookeeper and Kafka"
docker compose up -d zookeeper kafka

echo ">>> Waiting for the broker to accept connections"
for i in $(seq 1 40); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    echo "    broker is up"
    break
  fi
  printf "."
  sleep 3
done
echo

if ! docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
  echo "ERROR: the broker did not come up." >&2
  docker compose logs --tail=30 kafka >&2
  exit 1
fi

for t in "$IN_TOPIC" "$OUT_TOPIC"; do
  echo ">>> Creating topic '$t'"
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1 >/dev/null 2>&1
done

echo
echo ">>> Topics now on the broker"
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list

echo
echo ">>> Installing the host-side Kafka client"
python3 -c "import kafka" 2>/dev/null \
  && echo "    kafka-python already installed" \
  || pip install -q kafka-python && echo "    kafka-python installed"

echo
echo "Kafka ready."
echo "  inside the compose network : kafka:9092"
echo "  from this host             : localhost:29092"
echo
echo "Next: bash scripts/run_kafka_stream.sh"
