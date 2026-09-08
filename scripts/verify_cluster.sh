#!/usr/bin/env bash
# Evidence-gathering script: run this before each review and screenshot it.
set -uo pipefail
NN="${NN_SERVICE:-namenode}"
HDFS_BASE="${HDFS_BASE:-/fraudlens}"

line() { printf '\n===== %s =====\n' "$1"; }

line "CONTAINERS"
docker compose ps

line "HDFS REPORT (expect: 1 live DataNode, replication 1)"
docker compose exec -T "$NN" hdfs dfsadmin -report | head -25

line "BLOCK / HEALTH CHECK (expect: HEALTHY, 0 under-replicated)"
docker compose exec -T "$NN" hdfs fsck "$HDFS_BASE" | tail -25

line "DATASET SIZES"
docker compose exec -T "$NN" hdfs dfs -du -h "$HDFS_BASE/dataset" || true

line "YARN NODES"
docker compose exec -T "$NN" yarn node -list 2>/dev/null || echo "(yarn CLI unavailable in this container)"

line "DASHBOARDS"
cat <<'URLS'
  HDFS NameNode ...... http://localhost:9870
  YARN ResourceManager http://localhost:8088
  Spark Master ....... http://localhost:8080
  Spark Application .. http://localhost:4040   (only while a job runs)
URLS
