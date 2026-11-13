#!/usr/bin/env python3
"""
PHASE 12a -- Kafka transaction producer.

Publishes transactions one at a time to a Kafka topic, simulating a payment
switch emitting events. This replaces the file-drop mechanism: instead of
writing a 500-row CSV and waiting for Spark to notice it, each transaction is
an individual message on a broker.

Source rows come from the preprocessed CSV sample already in HDFS, so the
schema matches exactly what the trained PipelineModel expects.

Runs on the HOST, connecting to localhost:29092 (the HOST listener).
Spark, running inside the compose network, connects to kafka:9092 instead.

Usage:
    python3 scripts/kafka_producer.py                      # 600 events, 2/s
    python3 scripts/kafka_producer.py --rate 10 --count 2000
    python3 scripts/kafka_producer.py --fraud-boost 0.05   # inject more fraud
"""
import argparse
import csv
import json
import os
import random
import sys
import time

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
SAMPLE = "data/stream_src/sample.csv"

# Columns the model pipeline needs. Anything else in the CSV is dropped so the
# message stays small.
NUMERIC = [
    "user", "card", "day", "month", "year", "hour", "minute",
    "day_of_week", "is_weekend",
    "day_of_year", "mcc", "error_flag", "vpn_flag", "geo_missing",
    "hw_missing", "imputed_city", "imputed_state", "imputed_zip",
    "is_online", "is_foreign_or_unknown", "is_night", "is_outlier",
    "is_refund", "is_fraud",
]
DOUBLES = [
    "amount", "amount_abs", "amount_log", "amount_capped",
    "device_lat", "device_lon", "merchant_lat", "merchant_lon",
    "device_merchant_distance_km",
]
STRINGS = [
    "use_chip", "merchant_name", "merchant_city", "merchant_state",
    "zip", "error_type", "device_ip", "device_os",
]


def log(m):
    print("[producer] %s" % m, flush=True)


def coerce(row):
    """CSV gives strings; Kafka messages carry typed JSON."""
    out = {}
    for c in NUMERIC:
        try:
            out[c] = int(float(row.get(c) or 0))
        except (TypeError, ValueError):
            out[c] = 0
    for c in DOUBLES:
        try:
            out[c] = float(row.get(c) or 0.0)
        except (TypeError, ValueError):
            out[c] = 0.0
    for c in STRINGS:
        out[c] = (row.get(c) or "UNKNOWN").strip() or "UNKNOWN"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--sample", default=SAMPLE)
    ap.add_argument("--count", type=int, default=600,
                    help="How many events to publish")
    ap.add_argument("--rate", type=float, default=2.0,
                    help="Events per second")
    ap.add_argument("--fraud-boost", type=float, default=0.0,
                    help="Extra probability of flipping a row to fraud, so a "
                         "short demo shows some positives. 0 = untouched data.")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("kafka-python not installed. Run:\n"
                 "  source .venv/bin/activate\n"
                 "  pip install kafka-python")

    if not os.path.exists(args.sample):
        sys.exit("Sample not found: %s\n"
                 "Pull it out of HDFS first:\n"
                 "  mkdir -p data/stream_src\n"
                 "  docker compose exec -T namenode bash -c "
                 "\"hdfs dfs -cat /fraudlens/dataset/preprocessed_sample_csv/part-*.csv\" "
                 "> data/stream_src/sample.csv" % args.sample)

    rng = random.Random(args.seed)

    log("reading %s" % args.sample)
    with open(args.sample) as fh:
        rows = [coerce(r) for r in csv.DictReader(fh)]
    if not rows:
        sys.exit("Sample CSV is empty.")
    log("%s rows available" % format(len(rows), ","))

    log("connecting to Kafka at %s" % args.bootstrap)
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks=1,
            retries=3,
            request_timeout_ms=10000,
        )
    except Exception as exc:                                   # noqa: BLE001
        sys.exit("Could not reach Kafka at %s\n%s\n\n"
                 "Is the broker up?   docker compose ps kafka"
                 % (args.bootstrap, exc))

    log("publishing %s events to topic '%s' at %.1f/s"
        % (format(args.count, ","), args.topic, args.rate))
    log("press Ctrl-C to stop early")
    log("-" * 58)

    delay = 1.0 / args.rate if args.rate > 0 else 0
    sent = frauds = 0
    t0 = time.time()

    try:
        for i in range(args.count):
            row = dict(rows[i % len(rows)])

            # Give the demo some positives without touching the base dataset.
            if args.fraud_boost > 0 and rng.random() < args.fraud_boost:
                row["is_fraud"] = 1
                row["amount_abs"] = round(row["amount_abs"] * rng.uniform(4, 12), 2)
                row["amount"] = row["amount_abs"]
                row["vpn_flag"] = 1
                row["is_night"] = 1

            row["event_id"] = i + 1
            row["emitted_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

            producer.send(args.topic, key=row["user"], value=row)
            sent += 1
            frauds += row["is_fraud"]

            if sent % 50 == 0:
                producer.flush()
                log("%6s sent | %4s labelled fraud | %.1f s elapsed"
                    % (format(sent, ","), format(frauds, ","), time.time() - t0))
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        log("interrupted")
    finally:
        producer.flush()
        producer.close()

    log("-" * 58)
    log("published %s events (%s labelled fraud) in %.1f s"
        % (format(sent, ","), format(frauds, ","), time.time() - t0))
    log("topic: %s" % args.topic)


if __name__ == "__main__":
    main()
