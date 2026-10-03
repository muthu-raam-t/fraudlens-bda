#!/usr/bin/env python3
"""
Adds Kafka and Zookeeper to docker-compose.yml.

WHY A BROKER AT ALL
    The existing streaming job watches an HDFS directory: a script writes
    live_001.csv and Spark notices the file. That is file-polling, not event
    streaming. With Kafka a producer publishes individual transaction events
    to a topic and Spark subscribes to that stream, which is how real payment
    systems deliver transactions.

TWO LISTENERS, ON PURPOSE
    kafka:9092      for clients inside the compose network (Spark)
    localhost:29092 for clients on the host (the producer, Streamlit)
    A single listener cannot serve both, because the advertised hostname
    differs depending on where the client runs.

Run once:  python3 scripts/patch_compose_kafka.py
"""
import sys

P = "docker-compose.yml"

KAFKA = '''  # ===================== KAFKA (event streaming) =====================
  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.1
    hostname: zookeeper
    restart: unless-stopped
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - fraudlens-net

  kafka:
    image: confluentinc/cp-kafka:7.0.1
    hostname: kafka
    restart: unless-stopped
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 2
    networks:
      - fraudlens-net
    depends_on:
      - zookeeper

'''

s = open(P).read()
if "confluentinc/cp-kafka" in s:
    print("Kafka already present in docker-compose.yml")
    sys.exit(0)

anchor = "networks:\n  fraudlens-net:\n    driver: bridge"
if anchor not in s:
    sys.exit("ANCHOR NOT FOUND -- paste the last 10 lines of docker-compose.yml")

open(P, "w").write(s.replace(anchor, KAFKA + anchor))
print("Kafka and Zookeeper added to docker-compose.yml")
print("Next:  docker compose up -d zookeeper kafka")
