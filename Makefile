.PHONY: help up down nuke logs hdfs-init ingest verify shell-nn shell-spark

help:
	@echo "make up         - start the cluster"
	@echo "make hdfs-init  - create the HDFS directory layout"
	@echo "make ingest     - generate + upload the 28GB dataset (chunk by chunk)"
	@echo "make verify     - print cluster evidence for the report"
	@echo "make down       - stop containers (KEEPS HDFS data)"
	@echo "make nuke       - stop containers AND DELETE all HDFS data"
	@echo "make shell-nn   - shell into the namenode"
	@echo "make shell-spark- shell into spark-master"

up:
	docker compose up -d
	@echo "HDFS  http://localhost:9870"
	@echo "YARN  http://localhost:8088"
	@echo "Spark http://localhost:8080"

down:
	docker compose down

nuke:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f --tail=50

hdfs-init:
	bash scripts/setup_hdfs.sh

ingest:
	bash scripts/ingest.sh

verify:
	bash scripts/verify_cluster.sh

shell-nn:
	docker compose exec namenode bash

shell-spark:
	docker compose exec spark-master bash
