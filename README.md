# FraudLens — Distributed Credit Card Fraud Analytics Framework

Big Data Analytics (23AID302) course project — Team 16
School of Artificial Intelligence, Amrita Vishwa Vidyapeetham, AY 2026-27

An end-to-end distributed pipeline that ingests ~28 GB of credit card
transaction data into HDFS, preprocesses it in Scala on Spark under YARN,
trains class-imbalance-aware classifiers with Spark MLlib, and serves
explainable real-time verdicts through a React dashboard.

---

## Cluster topology

| Service | Role | UI |
|---|---|---|
| `namenode` | HDFS metadata | http://localhost:9870 |
| `datanode1` | HDFS block storage (single node, **replication = 1**) | http://localhost:9864 |
| `resourcemanager` | YARN scheduling | http://localhost:8088 |
| `nodemanager` | YARN container execution | http://localhost:8042 |
| `spark-master` | Spark driver host / job submission | http://localhost:8080 |
| `spark-worker` | Spark executor | http://localhost:8081 |

**Replication factor is 1.** This is a single-DataNode deployment on one host,
so `dfs.replication` is set to 1 rather than leaving blocks permanently
under-replicated. The architecture supports N DataNodes; only the deployment is
single-node. State this in the report rather than claiming fault tolerance the
cluster cannot demonstrate.

YARN is the cluster manager for **every** distributed job — the MapReduce job,
the Scala preprocessing job, and all PySpark training runs.

## HDFS layout

```
/fraudlens/
├── dataset/
│   ├── unprocessed/                 # 28 GB raw CSV, 5 chunks
│   ├── preprocessed/                # Parquet, partitioned by Year/Month
│   └── preprocessed_sample_csv/     # 20k-row human-readable sample
├── lookup/zip_coords.csv            # broadcast-join table
├── mapreduce_output/                # Hadoop Streaming job output
├── models/                          # saved Spark PipelineModel
├── aggregates/                      # small JSON for the dashboard
└── streaming/{input,verdicts,checkpoint}/
```

## Quick start

```bash
cp .env.example .env          # tune YARN/Spark memory to your machine
make up                       # start the six containers
make hdfs-init                # create the HDFS directory tree
# place the Kaggle CSV at data/source/credit_card_transactions-ibm_v2.csv
make ingest                   # generate + upload 28 GB, chunk by chunk
make verify                   # print evidence for the report
```

`make down` stops the containers but keeps HDFS data.
`make nuke` deletes the containers **and all HDFS data** — only this project's
volumes, never another folder's.

## Dataset

Base: Erik Altman et al., *Credit Card Transactions* (Kaggle), ~24M rows,
~2.3 GB. Scaled to 28 GB by sampling with jitter.

**What is real vs. what is synthetic** — state this openly in the report:

| Column(s) | Provenance |
|---|---|
| `user`, `card`, `year`, `month`, `use_chip`, `merchant_name`, `merchant_city`, `merchant_state`, `zip`, `mcc`, `is_fraud` | real, from the source dataset |
| `amount`, `day`, `time` | real values, jittered (±3% / ±2 days / resampled) |
| `device_metadata` | **fabricated** — nested JSON (VPN flag, device IP, geo, OS) |

`device_metadata` exists because the source dataset is entirely flat CSV, and
the Variety requirement needs a semi-structured component. Its correlation with
fraud is deliberately weak so the model learns the data rather than the
generator — check the SHAP importances and confirm `vpn_flag` is not the top
feature.

`mcc` is the ISO 18245 Merchant Category Code (5411 grocery, 5812 restaurants,
5541 fuel, 7995 gambling), and is a genuine column in the source data.

### Leakage prevention

Users are partitioned by `user % 5`: chunks 1-4 contain only training users,
chunk 5 only held-out users. Because rows are sampled with replacement, a
random split would place duplicates of the same transaction on both sides and
inflate every metric. The user-level split makes that structurally impossible.

### Injected messiness (Veracity)

- `amount` keeps `$` and thousands separators → real regex cleanup in Scala
- `is_fraud` stays `Yes`/`No` → real casting
- ~2% nulls injected into `merchant_city`, `merchant_state`, `zip`, `errors`
- ~4% of JSON blobs are missing `geo`, ~3% missing `hardware`

Preprocessing **imputes rather than drops**, so `rows in == rows out`. That
equality is the number to defend, not the byte count: Parquet + Snappy will
shrink 28 GB to roughly 7 GB on disk, which is columnar compression working as
intended, not data loss.

## Requirement coverage

| Requirement (23AID302) | Where it is satisfied |
|---|---|
| Volume | 28 GB, ~220 HDFS blocks |
| Velocity | Phase 11 — Spark Structured Streaming scorer |
| Variety | structured CSV + nested `device_metadata` JSON |
| Veracity | injected nulls / dirty amounts, imputed in Phase 4 |
| Hadoop HDFS | Phase 1 |
| Hadoop MapReduce | Phase 3 — Hadoop Streaming job on YARN |
| Spark DataFrame/Dataset API | Phases 4, 5 |
| Scala | Phase 4 (all preprocessing) |
| Python | Phases 1, 3, 5, 7-9 |
| Spark ML | Phase 5 — LogisticRegression, RandomForest, GBT |
| Performance tuning | Phase 6 — partitioning, caching, broadcast join |
| Visualization / dashboarding | Phase 10 — React analytics dashboard |

## Phases

| # | Phase | Status |
|---|---|---|
| 0 | Repo scaffold | done |
| 1 | Cluster (HDFS + YARN + Spark, RF=1) | done |
| 2 | Dataset generation and ingestion | done |
| 3 | MapReduce: fraud rate by merchant state | todo |
| 4 | Scala preprocessing on Spark/YARN | todo |
| 5 | PySpark MLlib training (LR / RF / GBT) | todo |
| 6 | Performance tuning benchmarks | todo |
| 7 | CatBoost serving model + SHAP | todo |
| 8 | Dashboard aggregates | todo |
| 9 | FastAPI inference service | todo |
| 10 | React dashboard | todo |
| 11 | Structured Streaming scorer | todo |
| 12 | Documentation and review deck | todo |

### Why both MapReduce and Spark

They answer different questions and are not redundant:

| | MapReduce (Phase 3) | GBT model (Phase 5) |
|---|---|---|
| Question | "What happened?" — fraud rate per state | "Is *this* transaction fraud?" |
| Method | counting / summing | supervised learning |
| Input | raw unprocessed CSV | preprocessed feature vectors |
| Output | a table of aggregates | a probability per transaction |

The MapReduce job also doubles as the **baseline** for the performance
comparison: Phase 4 runs the identical aggregation in Spark, and the two
wall-clock times give the "Big Data vs. Conventional" slide real measured
numbers instead of quoted claims.

## Models

Trained distributed on the full dataset (Spark MLlib):

- **GBTClassifier** — primary. Handles the categorical-heavy tabular data and
  supports `weightCol` in Spark 3.0, so cost-sensitive training handles the
  <0.2% fraud rate without resampling 130M rows.
- **RandomForestClassifier** — ensemble baseline with native feature importances.
- **LogisticRegression** — deliberately simple baseline, `weightCol` weighted.

Trained on a stratified sample (Phase 7):

- **CatBoost** — the *serving* model only. Spark MLlib models cannot produce
  TreeSHAP explanations and cost seconds per single-row prediction; CatBoost
  gives millisecond latency with per-transaction SHAP. Report the sample size
  openly — this is not the distributed result.

Evaluation uses **PR-AUC**, precision, recall and F1 on the fraud class, plus a
threshold sweep. Never accuracy: predicting "never fraud" scores 99.8%.

## Repository layout

```
fraudlens-bda/
├── docker-compose.yml       # 6 services, no MongoDB, no Jupyter
├── Makefile                 # up / hdfs-init / ingest / verify / nuke
├── .env.example
├── hadoop-conf/             # core, hdfs, yarn, mapred site XMLs
├── scripts/
│   ├── build_zip_lookup.py  # ZIP -> lat/lon broadcast table
│   ├── generate_chunk.py    # synthetic scaler, one chunk at a time
│   ├── ingest.sh            # generate -> put -> delete, looped
│   ├── setup_hdfs.sh        # HDFS directory tree
│   └── verify_cluster.sh    # evidence for the report
├── mapreduce/               # Phase 3
├── spark-apps/              # Phases 4, 5, 8, 11
├── api/                     # Phase 9
├── ui/                      # Phase 10
├── artifacts/               # models + metrics (gitignored)
├── data/                    # source + staging (gitignored)
└── docs/GIT_SETUP.md
```

Jupyter is intentionally absent: `jupyter/pyspark-notebook` ships Spark 3.5.x,
which cannot submit to this Spark 3.0.0 cluster. All PySpark runs through
`spark-submit` from the `spark-master` container. MongoDB is absent because
nothing needed it — streaming verdicts go to HDFS Parquet, and dashboard
aggregates are small JSON files served by FastAPI.
