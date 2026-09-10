# FraudLens — Distributed Credit Card Fraud Analytics Framework

**Scalable Feature Engineering and Class-Imbalance Classification on Containerized HDFS and Apache Spark**

## Table of Contents

1. [Project Status at a Glance](#1-project-status-at-a-glance)
2. [Abstract](#2-abstract)
3. [Introduction and Background](#3-introduction-and-background)
4. [Problem Statement](#4-problem-statement)
5. [Objectives](#5-objectives)
6. [Literature Survey](#6-literature-survey)
7. [Proposed System and Methodology](#7-proposed-system-and-methodology)
8. [Implementation Details](#8-implementation-details)
9. [Results and Analysis](#9-results-and-analysis)
10. [Models Used and Why](#10-models-used-and-why)
11. [Challenges and Limitations](#11-challenges-and-limitations)
12. [Big Data Tools vs Conventional Processing](#12-big-data-tools-vs-conventional-processing)
13. [Requirement Coverage Matrix](#13-requirement-coverage-matrix)
14. [Repository Layout](#14-repository-layout)
15. [How to Reproduce](#15-how-to-reproduce)
16. [Conclusion and Future Work](#16-conclusion-and-future-work)
17. [Conference Paper Status](#17-conference-paper-status)
18. [References](#18-references)

---

## 1. Project Status at a Glance

| Phase | Description | Status |
|---|---|---|
| 0 | Repository scaffold, `.gitignore`, environment config | **COMPLETED** |
| 1 | Containerized cluster: HDFS + YARN + Spark | **COMPLETED** |
| 2 | Synthetic dataset generation and HDFS ingestion (28.1 GB) | **COMPLETED** |
| 3 | Hadoop MapReduce job — fraud rate by merchant state | **COMPLETED** |
| 3b | Spark equivalent of the same aggregation (engine benchmark) | **COMPLETED** |
| 4 | Scala preprocessing pipeline on Spark / YARN | **COMPLETED** |
| 5 | Distributed model training (Spark MLlib: LR, RF, GBT) | **PENDING** |
| 6 | Performance tuning benchmarks (cache, broadcast, pruning) | **PENDING** |
| 7a | Stratified sample export for the serving model | **PENDING** |
| 7b | CatBoost serving model with TreeSHAP explanations | **PENDING** |
| 8 | Dashboard aggregates (precomputed JSON) | **PENDING** |
| 9 | FastAPI inference service | **NOT STARTED** |
| 10 | React analytics dashboard | **NOT STARTED** |
| 11 | Spark Structured Streaming scorer (Velocity requirement) | **NOT STARTED** |
| 12 | Documentation, screenshots, final review deck | **IN PROGRESS** |

**Code for Phases 5–8 is written, syntax-checked and committed. It has not yet been executed end-to-end.** Phases 9–11 are designed but not implemented.

---

## 2. Abstract

**Status: COMPLETED — describes work already executed**

Credit card fraud causes an estimated USD 24–30 billion in annual losses globally, and detecting it requires processing transaction volumes far beyond the capacity of a single machine. This project builds an end-to-end distributed analytics pipeline for large-scale credit card fraud detection on a containerized Hadoop and Apache Spark cluster.

A real base dataset of 24.4 million transactions (2.2 GB) was scaled through user-disjoint sampling with jitter into a synthetic corpus of **132,500,000 transactions occupying 28.1 GB**, stored across **230 HDFS blocks** with replication factor 1 on a single-DataNode deployment. Two execution engines were then run over the identical dataset for a controlled comparison: a Hadoop Streaming **MapReduce** job completed a per-state fraud aggregation in **613 seconds**, while the **Apache Spark** implementation of the same aggregation completed in **65.3 seconds** — a measured **9.4x speedup** attributable to in-memory shuffling rather than MapReduce's 2.09 GB of intermediate disk spill.

A native **Spark–Scala** preprocessing pipeline, resource-managed by YARN, then cleaned the full dataset in 19 minutes 8 seconds, performing regex sanitisation, nested-JSON parsing, sentinel imputation and feature derivation with **zero row loss** (132,500,000 in, 132,500,000 out), writing 6.6 GB of Snappy-compressed Parquet partitioned by year and month.

The remaining pipeline — distributed ensemble training with Spark MLlib under cost-sensitive class weighting for the extreme 0.165 % fraud rate, explainable serving via CatBoost with TreeSHAP, and a real-time Structured Streaming scorer — is implemented and pending execution.

---

## 3. Introduction and Background

**Status: COMPLETED**

### Domain

Financial Data Engineering (FinTech) combined with distributed Big Data Analytics.

### Industry Motivation

The rapid growth of global digital payments has been accompanied by rising fraud losses, estimated at USD 24–30 billion annually. Financial institutions need automated detection that operates at transaction scale and, increasingly, in real time.

### Technical Challenge

Single-machine environments built on Pandas and scikit-learn load entire datasets into RAM. On a 28 GB transaction corpus this produces immediate Out-Of-Memory failures on typical hardware, and even where memory suffices, single-threaded row-by-row transformation produces multi-hour latencies.

### Why Big Data Frameworks

- **Hadoop HDFS** partitions the dataset into fixed-size blocks distributed across DataNodes, providing fault tolerance through replication and enabling parallel reads.
- **Hadoop YARN** schedules compute resources as containers, allowing multiple execution engines to share one cluster.
- **Apache Spark** executes transformations in memory across partitions, avoiding the disk materialisation that MapReduce performs between every stage.

This project deliberately implements the aggregation **twice** — once in MapReduce, once in Spark — so that the performance claim rests on measurement rather than assertion.

---

## 4. Problem Statement

**Status: COMPLETED**

Build a distributed analytics system that detects fraudulent credit card transactions at big-data scale, satisfying all four V's of Big Data.

### Volume — COMPLETED

**28.1 GB** of raw transaction data comprising **132,500,000 rows**, stored across **230 HDFS blocks** of 128 MB each. Verified via `hdfs fsck`: average block size 131,360,676 bytes, average block replication 1.0, filesystem HEALTHY.

### Velocity — NOT STARTED (Phase 11)

The batch pipeline is complete. A Spark Structured Streaming job that watches an HDFS directory, applies the saved PipelineModel to arriving micro-batches, and writes checkpointed verdicts is designed but not yet implemented.

### Variety — COMPLETED

Each record combines **structured** tabular fields (numeric identifiers, dates, categorical strings) with a **semi-structured** nested JSON blob (`device_metadata`) embedded in the same CSV column. This forces the ingestion pipeline to handle two distinct data models — flat/columnar and nested/schema-less — within a single parsing step.

### Veracity — COMPLETED

The dataset is deliberately messy and the pipeline handles it explicitly:

| Defect | Volume | Treatment |
|---|---|---|
| Amounts with currency symbols and separators (`"$1,204.55"`) | all rows | regex strip, cast to double |
| Labels as text (`"Yes"` / `"No"`) | all rows | cast to binary 0/1 |
| ZIP codes stored as floats, leading zeros lost (`"4917.0"`) | all rows | split and left-pad to 5 digits |
| Missing merchant city | 2,650,183 | sentinel `"UNKNOWN"` + `imputed_city` flag |
| Missing merchant state | 17,130,754 | sentinel `"UNKNOWN"` + `imputed_state` flag |
| Missing ZIP | 17,974,284 | sentinel `"00000"` + `imputed_zip` flag |
| Missing JSON `geo` sub-object | 5,300,156 | null-safe parse + `geo_missing` flag |
| Missing JSON `hardware` sub-object | 3,977,162 | null-safe parse + `hw_missing` flag |

**Every imputation is recorded in a flag column rather than silently applied.** No row is dropped at any stage.

### Additional problem: extreme class imbalance — MEASURED

Fraud constitutes **218,622 of 132,500,000 transactions — 0.1650 %**. Accuracy is therefore useless as a metric: a classifier predicting "never fraud" scores 99.835 %. The project uses PR-AUC, precision, recall, F1 and a threshold sweep instead.

---

## 5. Objectives

| # | Objective | Status |
|---|---|---|
| 1 | Deploy a containerized Hadoop HDFS cluster storing and blocking a multi-gigabyte transaction corpus with zero corruption | **COMPLETED** — 34.7 GB DFS used, 230 blocks, fsck HEALTHY |
| 2 | Implement a genuine Hadoop MapReduce job on YARN over the raw corpus | **COMPLETED** — 613 s, 225 map tasks, 222 output groups |
| 3 | Quantify the MapReduce vs Spark performance difference on identical work | **COMPLETED** — 9.4x measured |
| 4 | Implement a native Spark–Scala ETL performing parallel cleansing, imputation and feature derivation entirely in memory | **COMPLETED** — 19m 8s, 0 rows dropped |
| 5 | Build and benchmark distributed ensemble classifiers under cost-sensitive weighting | **PENDING** — code written, not executed |
| 6 | Measure the effect of Spark performance-tuning techniques | **PENDING** — code written, not executed |
| 7 | Deliver explainable, low-latency per-transaction predictions | **PENDING** |
| 8 | Serve results through an analytical dashboard | **NOT STARTED** |
| 9 | Demonstrate real-time streaming ingestion and scoring | **NOT STARTED** |

---

## 6. Literature Survey

**Status: COMPLETED**

| # | Journal / Conference & Year | Technique Adopted | Limitations |
|---|---|---|---|
| 1 | **IEEE ICSCNA (2024)** — *base paper* | Distributed Spark architecture with MLlib (XGBoost, Random Forest, SVM) and PowerBI | Standalone ML incurs severe latency (RF: 1.8 hrs); SVM fails to scale on large datasets and suffers lower recall (93 %) |
| 2 | IEEE SCOPES (2024) | Supervised and deep learning classifiers (CNN+LSTM, Voting Classifier, Random Forest) | Prone to severe overfitting (Decision Tree and Voting reach 100 % on static data); SVM recall drops to 29 % on imbalanced sets |
| 3 | MDPI Electronics (2025) | PySpark, XGBoost and CatBoost with Bayesian hyperparameter optimisation (Optuna) | High cluster memory demands; sensitive to concept drift over time |
| 4 | IEEE iCAST (2025) | Hybrid K-Means and Isolation Forest outlier filtering with K-NN classification | Multi-stage execution delay; error propagation across pipeline stages |
| 5 | Int. Journal on Robotics, Automation & Sciences (2026) | Hadoop ecosystem vs Spark Streaming with Explainable AI (XAI) and Federated Learning | Hadoop 2–4 hr batch latency; black-box models clash with GDPR |

### Gaps identified, and how this project addresses them

| Gap in the literature | This project's response |
|---|---|
| Performance claims are asserted rather than measured on a controlled workload | Identical aggregation executed on both MapReduce and Spark over the same 28.1 GB — 613 s vs 65.3 s |
| Row-dropping preprocessing silently discards dirty records, inflating downstream metrics | Imputation with explicit flag columns; rows-in equals rows-out is verified and printed |
| Duplicated or resampled synthetic data leaks between train and test splits | User-disjoint partitioning (`user % 5`) makes cross-split leakage structurally impossible |
| Black-box scores cannot be audited | TreeSHAP contributions attached to every served prediction |
| Accuracy reported on 0.1 % positive classes | PR-AUC, recall and threshold sweeps used throughout; accuracy explicitly labelled as not-to-be-quoted |

---

## 7. Proposed System and Methodology

**Status: design COMPLETED — implementation status shown per stage**

### 7.1 Architecture Diagram

```
================================================================================
                       PROPOSED SYSTEM ARCHITECTURE
        Distributed Credit Card Fraud Analytics on Containerized
                        Hadoop HDFS + YARN + Apache Spark
================================================================================

+------------------------------------------------------------------------------+
|  0. SOURCE DATA ACQUISITION                                   [COMPLETED]     |
|------------------------------------------------------------------------------|
|  Erik Altman et al., "Credit Card Transactions" (Kaggle)                      |
|  24,386,900 real transactions  |  2.2 GB CSV  |  15 columns                   |
|  Columns: User, Card, Year, Month, Day, Time, Amount, Use Chip,               |
|           Merchant Name/City/State, Zip, MCC, Errors?, Is Fraud?              |
+------------------------------------------------------------------------------+
                                     |
                                     v
+------------------------------------------------------------------------------+
|  1. SYNTHETIC SCALING  (Python, host machine)                 [COMPLETED]     |
|------------------------------------------------------------------------------|
|  scripts/generate_chunk.py                                                    |
|                                                                               |
|  * Pool sampling across the ENTIRE source file (17.7 % of every chunk)        |
|    -> full user coverage; a naive head-of-file read yielded only 248 users    |
|  * Jitter:  amount +/-3 %,  day +/-2,  time resampled                         |
|  * APPENDED: device_metadata -> nested JSON  {network{vpn,ip},                |
|                                               geo{lat,lon},                   |
|                                               hardware{os}}                   |
|    (fabricated; supplies the SEMI-STRUCTURED half of the Variety requirement) |
|  * Injected nulls (~2 %) into merchant_city / merchant_state / zip / errors   |
|  * LEAKAGE CONTROL: user % 5 == 0 -> holdout pool; chunks 1-4 train,          |
|    chunk 5 holdout. Disjoint user sets across the split.                      |
|                                                                               |
|  OUTPUT: 5 chunks x ~6.04 GB  =  28.1 GB  /  132,500,000 rows                 |
+------------------------------------------------------------------------------+
                                     |
                          generate -> put -> delete
                        (local disk never exceeds 1 chunk)
                                     v
+------------------------------------------------------------------------------+
|  2. DISTRIBUTED STORAGE - HADOOP HDFS                         [COMPLETED]     |
|------------------------------------------------------------------------------|
|  NameNode (metadata)  +  1 DataNode (blocks)                                  |
|  Block size 128 MB  |  Replication factor 1  |  230 blocks                    |
|  fsck: HEALTHY, 0 under-replicated, avg block 131,360,676 B                   |
|                                                                               |
|  /fraudlens/                                                                  |
|    dataset/unprocessed/            28.1 GB raw CSV      <-- MapReduce + Spark |
|    dataset/preprocessed/            6.6 GB Parquet      <-- all ML phases     |
|    dataset/preprocessed_sample_csv/ 20k readable rows                         |
|    lookup/zip_coords.csv           800 KB               <-- broadcast table   |
|    mapreduce_output/                                                          |
|    models/                                                                    |
|    aggregates/                                                                |
|    streaming/{input,verdicts,checkpoint}/                                     |
+------------------------------------------------------------------------------+
              |                                          |
              |  (raw CSV -- both engines get            |
              |   the SAME input, so the comparison      |
              |   measures the ENGINE, not the format)   |
              v                                          v
+--------------------------------------+   +----------------------------------+
| 3A. HADOOP MAPREDUCE     [COMPLETED] |   | 3B. APACHE SPARK    [COMPLETED]  |
|--------------------------------------|   |----------------------------------|
| Hadoop Streaming on YARN             |   | PySpark DataFrame API on YARN    |
| mapper.py   -> state \t fraudflag    |   | groupBy(state).agg(count, sum)   |
| reducer.py  -> per-state totals      |   |                                  |
|                                      |   |                                  |
| 225 map tasks, 4 reducers            |   | in-memory shuffle                |
| 2.09 GB SPILLED TO DISK              |   | no intermediate materialisation  |
| 265,000,000 spilled records          |   |                                  |
|                                      |   |                                  |
| ELAPSED: 613 s  (10m 13s)            |   | ELAPSED: 65.3 s                  |
+--------------------------------------+   +----------------------------------+
              |                                          |
              +--------------------+---------------------+
                                   v
                    +--------------------------------+
                    |  MEASURED RESULT: 9.4x SPEEDUP |
                    |  Identical input, identical    |
                    |  output (222 states,           |
                    |  132,500,000 reconciled)       |
                    +--------------------------------+
                                   |
                                   v
+------------------------------------------------------------------------------+
|  4. DISTRIBUTED PREPROCESSING - NATIVE SPARK-SCALA on YARN    [COMPLETED]     |
|------------------------------------------------------------------------------|
|  spark-apps/preprocess.scala   (spark-shell --master yarn -i)                 |
|                                                                               |
|  a) Explicit StructType schema      -- no inferSchema (saves a full pass)     |
|  b) regexp_replace "[$,]"           -- "$1,204.55" -> 1204.55                 |
|  c) Label cast                      -- "Yes"/"No"  -> 1/0                     |
|  d) lpad(split(zip,"."),5,"0")      -- "4917.0"    -> "04917"                 |
|  e) from_json(device_metadata, declared schema) -- null-safe nested parse     |
|  f) SENTINEL IMPUTATION + FLAG COLUMNS  -- never .na.drop()                   |
|  g) BROADCAST JOIN: zip_coords (27,321 rows) x 132.5M rows                    |
|     -> merchant_lat / merchant_lon                                            |
|  h) Haversine distance device <-> merchant                                    |
|  i) approxQuantile p99 -> is_outlier flag + amount_capped                     |
|     (original amount PRESERVED: large amounts are genuine fraud signal)       |
|  j) Assembled txn_timestamp -> day_of_week, is_weekend, day_of_year           |
|  k) Derived: amount_log, is_refund, is_online, is_night,                      |
|              is_foreign_or_unknown                                            |
|  l) repartition(120, year, month) then partitionBy(year, month)               |
|     -> avoids the small-files problem                                         |
|                                                                               |
|  VERIFICATION (printed and written to JSON):                                  |
|     rows in  = 132,500,000                                                    |
|     rows out = 132,500,000                                                    |
|     dropped  = 0            <-- the number to defend                          |
|     null amounts / states / zips / timestamps = 0                             |
|                                                                               |
|  ELAPSED: 19m 8s   OUTPUT: 6.6 GB Snappy Parquet, 43 columns                  |
+------------------------------------------------------------------------------+
                                     |
                                     v
+------------------------------------------------------------------------------+
|  5. DISTRIBUTED TRAINING - SPARK MLlib on YARN                  [PENDING]     |
|------------------------------------------------------------------------------|
|  spark-apps/train.py                                                          |
|                                                                               |
|  FEATURE ENGINEERING (distributed over all 132.5M rows):                      |
|    groupBy(user) -> user_amount_mean / std / txn_count  (~2k rows)            |
|                     -> BROADCAST back                                         |
|    groupBy(mcc)  -> mcc_frequency                       -> BROADCAST back     |
|    derived       -> amount_vs_user_mean, amount_zscore                        |
|    (no feature uses is_fraud -> no target leakage)                            |
|                                                                               |
|  SPLIT: user % 5 == 0 -> holdout.  Same rule as generation, so no user        |
|         appears on both sides. A random split would leak, because rows        |
|         were sampled WITH REPLACEMENT during scaling.                         |
|                                                                               |
|  CACHE train_df and test_df  (re-read by all three models)                    |
|                                                                               |
|  CLASS WEIGHT: weightCol = negatives/positives  (~605x for fraud rows)        |
|                                                                               |
|  TWO FEATURE PIPELINES -- deliberately different:                             |
|                                                                               |
|    TREES:  StringIndexer -> VectorAssembler -> RF / GBT                       |
|            (trees split on thresholds; integer codes are harmless)            |
|                                                                               |
|    LINEAR: StringIndexer -> OneHotEncoder -> VectorAssembler                  |
|                          -> StandardScaler(withMean=false) -> LR              |
|            (without one-hot, LR reads merchant_state as an ORDERED number;    |
|             without scaling, amount 0-30,000 swamps binary flags;             |
|             withMean=false preserves sparsity of the 222-column block)        |
|                                                                               |
|  MODELS TRAINED (all three, sequentially, in ONE run):                        |
|    1. LogisticRegression     -- deliberately simple baseline                  |
|    2. RandomForestClassifier -- ensemble baseline, feature importances        |
|    3. GBTClassifier          -- primary distributed model                     |
|                                                                               |
|  EVALUATION: PR-AUC (primary), ROC-AUC, precision, recall, F1,                |
|              confusion matrix, 9-point threshold sweep.                       |
|              Accuracy computed but explicitly labelled not-to-be-quoted.      |
|                                                                               |
|  Metrics persisted after EACH model -> a late failure cannot lose             |
|  earlier results.                                                             |
+------------------------------------------------------------------------------+
        |                          |                            |
        v                          v                            v
+---------------------------+ +------------------------+ +----------------------+
| 6. TUNING BENCHMARKS      | | 7. SERVING MODEL       | | 8. AGGREGATES        |
|              [PENDING]    | |          [PENDING]     | |         [PENDING]    |
|---------------------------| |------------------------| |----------------------|
| A. cache vs no-cache      | | 7a export_sample.py:   | | aggregates.py        |
|    (repeat aggregation)   | |   keep ALL 218,622     | |                      |
| B. broadcast vs shuffle   | |   fraud rows, down-    | | fraud rate by:       |
|    join (800 KB x 132.5M) | |   sample legitimate    | |   state / MCC / hour |
| C. partition pruning      | |   60:1; user-disjoint  | |   device / VPN /     |
|    (filter on year, a     | |                        | |   amount bucket      |
|     partition column, vs  | | 7b train_catboost.py:  | |                      |
|     hour, which is not)   | |   runs on HOST venv    | | confusion matrix,    |
|                           | |   (containers have     | | PR curve points,     |
| adaptive execution        | |    Python 3.5; Cat-    | | model comparison     |
| DISABLED so Spark cannot  | |    Boost needs 3.7+)   | |                      |
| optimise the comparison   | |   scale_pos_weight     | | 6.6 GB Parquet ->    |
| into equivalence          | |   TreeSHAP per row     | | a few hundred KB     |
+---------------------------+ +------------------------+ +----------------------+
                                        |                          |
                                        v                          v
+------------------------------------------------------------------------------+
|  9. FastAPI INFERENCE SERVICE                               [NOT STARTED]     |
|------------------------------------------------------------------------------|
|  Loads the CatBoost model ONCE at startup (a SparkSession would cost          |
|  seconds per call; CatBoost answers in milliseconds)                          |
|    POST /predict          -> verdict, P(fraud), top-5 SHAP contributions      |
|    GET  /aggregates/*     -> precomputed JSON from Phase 8                    |
|    GET  /stream/recent    -> latest streaming verdicts                        |
+------------------------------------------------------------------------------+
                                     |
                                     v
+------------------------------------------------------------------------------+
|  10. REACT ANALYTICS DASHBOARD (Vite + React + Recharts)    [NOT STARTED]     |
|------------------------------------------------------------------------------|
|  TAB 1 -- Live scoring:  transaction form -> verdict badge, probability       |
|                          gauge, SHAP waterfall                                |
|  TAB 2 -- Analytics:     fraud-rate map by state, MCC bars, hourly curve,     |
|                          amount distribution, model comparison table,         |
|                          MapReduce vs Spark benchmark, live verdict feed      |
+------------------------------------------------------------------------------+
                                     ^
                                     |
+------------------------------------------------------------------------------+
|  11. SPARK STRUCTURED STREAMING SCORER  (VELOCITY)          [NOT STARTED]     |
|------------------------------------------------------------------------------|
|  readStream on an HDFS file source (explicit StructType -- streaming file     |
|  sources CANNOT infer schema)                                                 |
|    -> loads the saved GBT PipelineModel                                       |
|    -> scores each micro-batch                                                 |
|    -> writes checkpointed verdicts to HDFS as Parquet                         |
|  feed.sh drips CSV files into the watched directory for a live demonstration  |
+------------------------------------------------------------------------------+

================================================================================
 RESOURCE MANAGEMENT: every distributed job above -- MapReduce, Scala ETL,
 MLlib training, tuning, aggregates, streaming -- is submitted to and
 scheduled by YARN. YARN performs no computation itself; it allocates
 containers (memory + vCores) in which the execution engines run.
 This shared scheduler is what makes the MapReduce/Spark comparison controlled.
================================================================================
```

### 7.2 Layer Model — how the technologies relate

These are not four competing systems. They are layers:

```
HADOOP (the ecosystem)
├── HDFS ........... distributed STORAGE      (34.7 GB, 230 blocks)
├── YARN ........... resource MANAGEMENT      (6 GB, 8 vCores allocatable)
└── MapReduce ...... execution ENGINE, disk-based, older

APACHE SPARK ....... execution ENGINE, in-memory, newer
                     runs ON YARN, reads and writes HDFS
```

| Phase | Engine | Scheduled by | Storage | Language |
|---|---|---|---|---|
| 2 — Ingestion | HDFS CLI | — | HDFS | Bash / Python |
| 3A — Aggregation | **MapReduce** | YARN | HDFS | Python (Streaming) |
| 3B — Aggregation | **Spark** | YARN | HDFS | Python (PySpark) |
| 4 — Preprocessing | **Spark** | YARN | HDFS | **Scala** |
| 5 — Training | **Spark MLlib** | YARN | HDFS | Python (PySpark) |
| 6 — Tuning | **Spark** | YARN | HDFS | Python (PySpark) |
| 7b — Serving model | CatBoost (single node) | — | local | Python |
| 8 — Aggregates | **Spark** | YARN | HDFS | Python (PySpark) |
| 11 — Streaming | **Spark Structured Streaming** | YARN | HDFS | Python (PySpark) |

---

## 8. Implementation Details

### 8.1 Cluster Configuration — COMPLETED

Six Docker containers on a single Ubuntu 24.04 host (15 GB RAM, 16 cores, 491 GB NVMe):

| Service | Image | Role | Web UI |
|---|---|---|---|
| `namenode` | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1 | HDFS metadata | :9870 |
| `datanode1` | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1 | HDFS blocks | :9864 |
| `resourcemanager` | bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1 | YARN scheduling | :8088 |
| `nodemanager` | bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1 | YARN containers | :8042 |
| `spark-master` | bde2020/spark-master:3.0.0-hadoop3.2 | Spark driver host | :8080 |
| `spark-worker` | bde2020/spark-worker:3.0.0-hadoop3.2 | Spark executor | :8081 |

**Resource allocation** (`.env`):

```
YARN_NM_MEMORY_MB=6144      # memory YARN may allocate to containers
YARN_NM_VCORES=8
YARN_MAX_ALLOC_MB=4096      # ceiling for a single container
SPARK_WORKER_MEMORY=4g
SPARK_WORKER_CORES=4
```

Roughly 6 GB of the host's 15 GB is offered to YARN; each Spark executor requests 2 GB, so about three run concurrently. The remainder is left to the operating system, Docker and the browser.

**Replication factor is 1.** This is a single-DataNode deployment, so setting `dfs.replication=1` yields a clean `fsck` report rather than permanently under-replicated blocks. The architecture supports N DataNodes; only the deployment is single-node. This is stated openly rather than claiming fault tolerance the cluster cannot demonstrate.

### 8.2 Dataset Provenance — COMPLETED

Being explicit about what is real and what is synthetic:

| Column(s) | Provenance |
|---|---|
| `user`, `card`, `year`, `month`, `use_chip`, `merchant_name`, `merchant_city`, `merchant_state`, `zip`, `mcc`, `errors`, `is_fraud` | **Real**, from the Kaggle source |
| `amount`, `day`, `time` | **Real values, jittered** (±3 %, ±2 days, resampled) |
| `device_metadata` (VPN flag, device IP, geo, OS) | **Fabricated** |

`device_metadata` exists because the base dataset is entirely flat CSV, and the Variety requirement needs a semi-structured component. Its correlation with fraud is injected, not real-world.

> **Disclosure for the report:** device metadata was synthetically appended to introduce a nested semi-structured field. VPN rows carry approximately 4x the base fraud rate as a direct consequence of the generator. If SHAP analysis in Phase 7b ranks `vpn_flag` among the top features, that partly measures the generator rather than genuine fraud behaviour, and the feature should be dropped or the finding explicitly qualified.

`mcc` is the ISO 18245 Merchant Category Code (5411 grocery, 5812 restaurants, 5541 fuel, 7995 gambling) and is a genuine source column.

### 8.3 Leakage Prevention — COMPLETED

Because rows were sampled **with replacement** during scaling, a random train/test split would place duplicates of the same underlying transaction on both sides and inflate every metric. Instead, users are partitioned by `user % 5`: chunks 1–4 contain only training users, chunk 5 only held-out users. The same rule is reapplied at training time. Cross-split contamination is therefore structurally impossible, not merely unlikely.

An earlier version of the generator filled its sampling pool from the head of the source file. Because the source is sorted by user, this produced only **248 distinct users** across 22 GB of training data — a model would have learned 248 people's habits, not fraud. The fix samples 17.7 % of every chunk across the entire file, yielding **1,600 users** in the pool and 1,575 in a single 500,000-row batch.

### 8.4 Engineering Problems Encountered and Resolved — COMPLETED

| Problem | Diagnosis | Resolution |
|---|---|---|
| `ClassNotFoundException: -D` on MapReduce submit | `find` matched `hadoop-streaming-3.2.1-**sources**.jar`, which has no `Main-Class` manifest | target `tools/lib/` directly |
| `InvalidAuxServiceException: mapreduce_shuffle does not exist` | bde2020 env-var naming converts `___`→`-`, `__`→`_`, `_`→`.`; writing `aux__services` produced the silently-ignored property `yarn.nodemanager.aux_services` | corrected all env vars to `___`; added the required `ShuffleHandler` class property |
| `.env` values not applied (YARN showed 8 GB instead of 6 GB) | the same naming bug on `resource_memory___mb` | same fix; verified in the live `yarn-site.xml` |
| `apt-get` 404s inside containers | images are Debian 9 (stretch), now end-of-life; packages moved to `archive.debian.org` | rewrite `sources.list`, disable `Check-Valid-Until` |
| `docker build` DNS failure | buildkit network isolation cannot resolve external hosts | abandoned the Dockerfile; install at runtime via `docker compose exec`, where DNS works |
| `SyntaxError` on f-strings | containers ship Python 3.5; f-strings require 3.6+ | converted to `.format()`; verified by AST walk that no `JoinedStr` nodes remain |
| `ModuleNotFoundError: numpy` | `pyspark.ml` imports numpy at load; images ship no scientific stack | distro package (`apt-get install python3-numpy`), not pip — no cp35 wheel exists, so pip would attempt a source build |
| `not found: value make_date` | `make_date` entered the Scala API in Spark 3.3; this cluster runs 3.0 | `to_date(format_string(...))` instead |
| `nullif` unresolved in Scala | SQL-only until Spark 3.5 | `when/otherwise` |
| `is_outlier` all zeros | `approxQuantile` relative error 0.01 returned approximately the maximum, so nothing exceeded it | tightened to 0.0001 |

---

## 9. Results and Analysis

### 9.1 Storage and Ingestion — COMPLETED

| Metric | Value |
|---|---|
| Raw dataset | **28.1 GB** CSV, 5 chunks |
| Rows | **132,500,000** |
| HDFS blocks | **230** |
| Average block size | 131,360,676 B (128 MB) |
| Average block replication | **1.0** |
| fsck status | **HEALTHY**, 0 under-replicated |
| Preprocessed dataset | **6.6 GB** Snappy Parquet |
| Total DFS used | **34.7 GB** |
| Ingestion time | 20 minutes (generate → put → delete, chunk by chunk) |
| Generation throughput | ~30 MB/s |

**On the storage reduction (28.1 GB → 6.6 GB):** this is columnar compression, not data loss. Contributing factors: binary typing (a double is 8 bytes versus 9 characters for `"$1,204.55"`), columnar layout (132.5M `merchant_state` values stored contiguously compress far better than interleaved rows), dictionary encoding (222 distinct states stored once with small references), and Snappy on top. The row-count reconciliation is the evidence that nothing was discarded.

### 9.2 MapReduce vs Spark — COMPLETED

Identical aggregation (fraud rate per merchant state), identical 28.1 GB raw CSV input, identical YARN cluster.

| Engine | Elapsed | Configuration |
|---|---|---|
| Hadoop MapReduce | **613 s** (10m 13s) | 225 map tasks, 4 reducers |
| Apache Spark | **65.3 s** | 2 executors x 2 GB / 2 cores |
| **Speedup** | **9.4x** | |

**Why MapReduce is slower — taken from its own job counters:**

| Counter | Value |
|---|---|
| FILE: bytes written | **2,087,890,445** (2.09 GB spilled to local disk) |
| Spilled records | **265,000,000** |
| Reduce shuffle bytes | 1,017,514,866 |
| HDFS bytes read | 30,213,882,459 |
| CPU time | 360,500 ms |

MapReduce materialises every intermediate key/value pair to disk between the map and reduce phases. Spark performs the same shuffle in memory.

**Raw CSV was used for both runs deliberately.** MapReduce cannot read Parquet without a custom input format, so feeding Spark the 6.6 GB Parquet would have measured the *file format* rather than the *engine*, and the resulting speedup would be uninterpretable.

**Both engines agree**, which confirms the comparison is like-for-like:

| | MapReduce | Spark |
|---|---|---|
| Distinct states | 222 | 222 |
| Transactions counted | 132,500,000 | 132,500,000 |
| Fraud transactions | 218,622 | 218,622 |
| Overall fraud rate | 0.1650 % | 0.1650 % |

### 9.3 Descriptive Findings — COMPLETED

**Top states by transaction volume:**

| State | Transactions | Fraud | Rate % |
|---|---|---|---|
| UNKNOWN | 17,130,754 | 107,864 | 0.6297 |
| CA | 13,803,212 | 10,005 | 0.0725 |
| TX | 9,521,486 | 5,685 | 0.0597 |
| FL | 7,773,136 | 4,907 | 0.0631 |
| NY | 7,726,232 | 4,685 | 0.0606 |
| OH | 4,752,649 | 6,922 | 0.1456 |
| IL | 4,526,102 | 2,528 | 0.0559 |
| PA | 4,475,489 | 2,847 | 0.0636 |

**Highest fraud rates (states with more than 100k transactions):**

| State | Transactions | Fraud | Rate % |
|---|---|---|---|
| UNKNOWN | 17,130,754 | 107,864 | 0.6297 |
| MEXICO | 251,210 | 1,575 | 0.6270 |
| OH | 4,752,649 | 6,922 | 0.1456 |
| NE | 520,489 | 424 | 0.0815 |
| IA | 1,451,626 | 1,102 | 0.0759 |

**Interpretation.** `UNKNOWN` is dominated by online transactions, which carry no merchant state, combined with the injected nulls; its 0.63 % fraud rate is roughly 4x the national average, consistent with card-not-present fraud being genuinely higher. `MEXICO` at 0.627 % reflects elevated cross-border fraud. Both are plausible signals rather than artefacts.

### 9.4 Preprocessing — COMPLETED

| Metric | Value |
|---|---|
| Elapsed | **19 m 08 s** |
| Rows in | 132,500,000 |
| Rows out | **132,500,000** |
| **Rows dropped** | **0** |
| Output columns | 43 |
| Output size | 6.6 GB Parquet |
| Partition directories | 30 years x months |
| Null amounts after cleanup | 0 |
| Null states after cleanup | 0 |
| Malformed ZIPs | 0 |
| Null timestamps | 0 |
| Broadcast lookup entries | 27,321 |

**Imputation counts (recorded, not hidden):** city 2,650,183; state 17,130,754; ZIP 17,974,284; device geo 5,300,156; device hardware 3,977,162.

### 9.5 Pending Results

- **Phase 5** — PENDING. Will produce PR-AUC / ROC-AUC / precision / recall / F1 and a threshold sweep for LR, RF and GBT, plus tree feature importances.
- **Phase 6** — PENDING. Will produce three measured speedup ratios (caching, broadcast join, partition pruning).
- **Phase 7b** — PENDING. Will produce CatBoost PR-AUC, Brier score and global and per-row TreeSHAP attributions.
- **Phase 8** — PENDING. Will produce six dashboard JSON files.

---

## 10. Models Used and Why

**Status: code written and committed; execution PENDING**

Four models across two tiers, with different jobs.

### Tier 1 — Distributed, trained on all 132,500,000 rows (Spark MLlib)

All three are trained **sequentially in a single run** of `spark-apps/train.py`, sharing the same cached feature DataFrames and the same user-disjoint split. Metrics are written after each model completes.

| # | Model | Role | Configuration |
|---|---|---|---|
| 1 | **LogisticRegression** | Deliberately simple baseline — establishes why an ensemble is needed | `maxIter=20`, `regParam=0.01`, `weightCol`, one-hot + standardised features |
| 2 | **RandomForestClassifier** | Ensemble baseline with native feature importances; trains faster than GBT | `numTrees=30`, `maxDepth=6`, `maxBins=256`, `weightCol` |
| 3 | **GBTClassifier** | **Primary distributed model** — sequential boosting on categorical-heavy tabular data | `maxIter=15`, `maxDepth=6`, `maxBins=256`, `weightCol` |

`weightCol` is set to `negatives/positives` (approximately 605 for fraud rows), giving cost-sensitive training without resampling 132.5M rows.

### Tier 2 — Serving model, trained on a stratified sample (single machine)

| # | Model | Role |
|---|---|---|
| 4 | **CatBoost** | **Serving model only** — millisecond single-row latency with exact TreeSHAP explanations |

**Why a second model at all.** Spark MLlib models cannot produce TreeSHAP explanations (SHAP has no MLlib support), and instantiating a SparkSession to score one transaction costs seconds, not milliseconds. A production fraud API needs both. CatBoost provides both, but only on a single machine.

**This is stated honestly rather than implied away:** CatBoost is trained on a stratified sample that keeps **all 218,622 fraud rows** and downsamples legitimate rows at 60:1. The sample size, the `scale_pos_weight` and the sampling rule are all recorded in `artifacts/catboost_sample_meta.json`. The **distributed** result is the Spark MLlib GBT; CatBoost is the serving layer.

### Models deliberately not used

| Model | Reason for exclusion |
|---|---|
| **SVM** | Does not scale to this transaction volume; the base paper (IEEE ICSCNA 2024) reports 93 % recall and scaling failure, and IEEE SCOPES 2024 reports recall collapsing to 29 % on imbalanced sets |
| **CNN / LSTM** | Deep learning requires far more data and tuning to beat tree ensembles on tabular features, and severely degrades explainability, which is a core requirement here |
| **Plain Decision Tree** | Overfits badly alone; IEEE SCOPES 2024 reports 100 % training accuracy, a clear overfit signature |
| **XGBoost** | Not natively distributed in Spark without `xgboost4j-spark`; adding a JAR dependency to a Spark 3.0.0 image is a version-compatibility risk. Spark's GBTClassifier is the natively distributed equivalent |

### Evaluation methodology

**Accuracy is not used as a headline metric.** At 0.1650 % positives, a classifier that predicts "never fraud" achieves 99.835 % accuracy while catching zero fraud. The metrics used are:

- **PR-AUC (`areaUnderPR`)** — primary; the correct metric under extreme imbalance
- ROC-AUC, precision, recall, F1 on the fraud class
- Full confusion matrix (TP / FP / FN / TN)
- A 9-point threshold sweep from 0.1 to 0.9, showing how precision and recall trade off as the decision point moves
- Brier score for CatBoost — calibration matters because the fraud *probability*, not just the label, determines the operational threshold

---

## 11. Challenges and Limitations

**Status: documented**

### Resolved challenges

See §8.4 for the ten concrete engineering failures encountered and their resolutions. The recurring themes were: (a) the bde2020 images are built on end-of-life Debian 9 with Python 3.5, which breaks modern Python syntax and package installation; (b) Spark 3.0.0 lacks several API functions that exist in later versions; (c) environment-variable-to-XML naming conventions fail silently when written incorrectly.

### Honest limitations

| Limitation | Impact | Mitigation / disclosure |
|---|---|---|
| **Single physical host** | The "distributed" cluster runs six containers on one machine, so network shuffle costs are unrealistically low and true node-failure tolerance cannot be demonstrated | Architecture is unchanged for N nodes; replication factor set to 1 and stated openly rather than claiming fault tolerance |
| **Replication factor 1** | No data redundancy | Deliberate, given one DataNode; RF=2 with a single DataNode would leave every block permanently under-replicated |
| **Synthetic scaling** | The 132.5M rows derive from 24.4M real rows sampled with replacement, so effective information content is that of the smaller set | User-disjoint split prevents leakage; jitter prevents exact duplicates; disclosed in §8.2 |
| **Fabricated device metadata** | `vpn_flag` carries about 4x the base fraud rate purely because the generator injected it | Disclosed; Phase 7b prints a warning if `vpn_flag` ranks in the top 3 SHAP features |
| **CatBoost trained on a sample** | Not a distributed result | Explicitly labelled as the serving model; sample size recorded in a metadata file |
| **6 GB YARN allocation** | Training is slower than on a production cluster; GBT iterations limited to 15 | A `--sample-frac` flag exists; any sampling would be reported |
| **Velocity is micro-batch** | Structured Streaming processes micro-batches, not true per-event streams | This is standard Spark behaviour and will be described accurately |
| **`spark-shell -i` rather than a compiled sbt artifact** | Less production-like than a packaged JAR | Still genuine Scala executing distributed on YARN; avoids installing sbt in a container whose build network cannot resolve DNS |

---

## 12. Big Data Tools vs Conventional Processing

**Status: COMPLETED for storage and engine comparison; PENDING for tuning benchmarks**

### 12.1 Conceptual comparison

| Dimension | Conventional (Pandas / scikit-learn, single node) | Big Data (Hadoop + Spark) |
|---|---|---|
| **Storage** | Single local disk; no replication; single point of failure | HDFS partitions into 128 MB blocks across DataNodes; replication configurable |
| **Memory model** | Entire dataset loaded into one machine's RAM → immediate OOM on 28 GB with 15 GB RAM | Partitions streamed and processed across executors; spills to disk gracefully rather than crashing |
| **Execution** | Single-threaded row-by-row transformation | Concurrent schema validation, filtering and feature extraction across partitions |
| **Scaling** | Vertical only — buy a bigger machine | Horizontal — add DataNodes and NodeManagers |
| **Fault tolerance** | Process dies, work is lost | YARN reattempts failed tasks; Spark recomputes lost partitions from lineage |

### 12.2 Measured engine comparison — COMPLETED

The claim "Spark is faster than MapReduce" is not asserted here; it was measured on identical work:

| Engine | Same aggregation over 28.1 GB / 132.5M rows | Time | Intermediate disk I/O |
|---|---|---|---|
| Hadoop MapReduce | 225 map tasks, 4 reducers | **613 s** | **2.09 GB spilled**, 265M records |
| Apache Spark | in-memory shuffle | **65.3 s** | none materialised |
| | | **9.4x faster** | |

### 12.3 Measured storage comparison — COMPLETED

| Format | Size | Rows |
|---|---|---|
| Raw CSV (row-oriented text) | 28.1 GB | 132,500,000 |
| Snappy Parquet (columnar binary) | 6.6 GB | 132,500,000 |
| | **4.3x reduction, 0 rows lost** | |

### 12.4 Spark tuning benchmarks — PENDING

`spark-apps/tuning_benchmark.py` will measure, with adaptive query execution **disabled** so that Spark cannot silently optimise the comparisons into equivalence:

- **Caching** — the same aggregation run twice, uncached versus cached
- **Broadcast versus shuffle join** — the 800 KB ZIP lookup joined against 132.5M rows, with `autoBroadcastJoinThreshold` forced to −1 for the shuffle case
- **Partition pruning** — filtering on `year` (a partition column, so directories are skipped) versus `hour` (not a partition column, forcing a full scan)

---

## 13. Requirement Coverage Matrix

| Course requirement (23AID302) | Where satisfied | Status |
|---|---|---|
| **Volume** — very large datasets (GB/TB) | 28.1 GB, 132,500,000 rows, 230 HDFS blocks | DONE |
| **Velocity** — streaming or real-time processing | Phase 11 — Structured Streaming scorer | NOT STARTED |
| **Variety** — any two of structured / semi-structured / unstructured | Structured CSV + nested `device_metadata` JSON | DONE |
| **Veracity** — noisy / incomplete / inconsistent data | Regex sanitisation, sentinel imputation with flag columns, null-safe JSON parsing | DONE |
| **Hadoop — HDFS** | Phases 1–2 | DONE |
| **Hadoop — MapReduce** | Phase 3, Hadoop Streaming on YARN, 613 s | DONE |
| **Apache Spark — DataFrame / Dataset API** | Phases 3b, 4, 5, 6, 8 | Phases 3b and 4 DONE |
| **Scala** | Phase 4 — all preprocessing | DONE |
| **Python** | Phases 1, 3, 3b, 5, 6, 7, 8 | DONE |
| **Spark ML** | Phase 5 — LR, RF, GBT | PENDING |
| **Data acquisition and pre-processing** | Phases 1–2, 4 | DONE |
| **Batch or streaming processing** | Batch DONE (Phases 3–8); streaming NOT STARTED | PARTIAL |
| **Analytical models (clustering / classification)** | Phase 5 classification | PENDING |
| **Performance tuning (partitioning, caching)** | Phase 4 partitioning DONE; Phase 6 benchmarks PENDING | PARTIAL |
| **Visualization and dashboarding** | Phase 10 React dashboard | NOT STARTED |
| **Large open dataset from Kaggle** | Erik Altman et al., Credit Card Transactions | DONE |

---

## 14. Repository Layout

```
fraudlens-bda/
├── docker-compose.yml              # 6 services: HDFS, YARN, Spark
├── .env.example                    # cluster memory / dataset sizing
├── Makefile                        # shortcuts for common commands
├── hadoop-conf/
│   ├── core-site.xml               # fs.defaultFS
│   ├── hdfs-site.xml               # dfs.replication = 1, 128 MB blocks
│   ├── yarn-site.xml               # ResourceManager, ShuffleHandler
│   └── mapred-site.xml             # mapreduce.framework.name = yarn
├── scripts/
│   ├── build_zip_lookup.py         # ZIP -> lat/lon broadcast table
│   ├── generate_chunk.py           # synthetic scaler, user-disjoint
│   ├── ingest.sh                   # generate -> put -> delete, looped
│   ├── setup_hdfs.sh               # HDFS directory tree
│   ├── setup_nodemanager_python.sh # python3 for Hadoop Streaming
│   ├── setup_pyspark_deps.sh       # numpy for pyspark.ml
│   ├── verify_cluster.sh           # evidence gathering for the report
│   ├── _spark_submit.sh            # shared spark-submit helper
│   ├── run_preprocess.sh           # Phase 4
│   ├── run_spark_agg.sh            # Phase 3b
│   ├── run_train.sh                # Phase 5
│   ├── run_tuning.sh               # Phase 6
│   ├── run_export_sample.sh        # Phase 7a
│   ├── run_catboost.sh             # Phase 7b
│   ├── run_aggregates.sh           # Phase 8
│   └── train_catboost.py           # Phase 7b (host, not container)
├── mapreduce/
│   ├── mapper.py                   # state -> fraud flag
│   ├── reducer.py                  # per-state totals and rate
│   └── run_mr.sh                   # Hadoop Streaming submit + timing
├── spark-apps/
│   ├── preprocess.scala            # Phase 4 — Scala ETL
│   ├── spark_same_agg.py           # Phase 3b — benchmark
│   ├── train.py                    # Phase 5 — MLlib training
│   ├── tuning_benchmark.py         # Phase 6
│   ├── export_sample.py            # Phase 7a
│   └── aggregates.py               # Phase 8
├── api/                            # Phase 9  (NOT STARTED)
├── ui/                             # Phase 10 (NOT STARTED)
├── artifacts/                      # models + metrics (gitignored)
├── results/                        # committed metrics for the report
├── data/                           # source + staging (gitignored)
└── docs/GIT_SETUP.md
```

**Deliberately absent:** MongoDB (nothing required it — streaming verdicts go to HDFS Parquet, dashboard aggregates are small JSON served by FastAPI) and Jupyter (`jupyter/pyspark-notebook` ships Spark 3.5.x, which cannot submit to this Spark 3.0.0 cluster; all PySpark runs through `spark-submit`).

---

## 15. How to Reproduce

### Prerequisites

Docker with Compose V2, Python 3.10+ on the host, roughly 80 GB free disk, 16 GB RAM.

### Setup

```bash
git clone git@github.com:muthu-raam-t/fraudlens-bda.git
cd fraudlens-bda
python3 -m venv .venv && source .venv/bin/activate
pip install pandas numpy
cp .env.example .env          # tune YARN memory to your machine
```

### Phases 1–2 — cluster and data

```bash
docker compose up -d
bash scripts/setup_hdfs.sh
bash scripts/setup_nodemanager_python.sh    # python3 for Hadoop Streaming
bash scripts/setup_pyspark_deps.sh          # numpy for pyspark.ml
# place credit_card_transactions-ibm_v2.csv in data/source/
bash scripts/ingest.sh                      # ~20 min, 28.1 GB
bash scripts/verify_cluster.sh
```

### Phase 3 — MapReduce and the benchmark

```bash
bash mapreduce/run_mr.sh          # ~10 min   -> 613 s
bash scripts/run_spark_agg.sh     # ~1 min    -> 65.3 s
```

### Phase 4 — Scala preprocessing

```bash
bash scripts/run_preprocess.sh    # ~19 min
cat artifacts/data_quality_profile.json
```

### Phases 5–8

```bash
bash scripts/run_train.sh         # 1-3 hrs
bash scripts/run_tuning.sh        # ~20 min
bash scripts/run_export_sample.sh # ~10 min
pip install catboost shap scikit-learn
bash scripts/run_catboost.sh      # ~15 min
bash scripts/run_aggregates.sh    # ~15 min
```

### Dashboards

HDFS http://localhost:9870 · YARN http://localhost:8088 · Spark http://localhost:8080

### Teardown

`docker compose down` stops the containers and **keeps** HDFS data.
`docker compose down -v` deletes the containers **and all HDFS data**, scoped to this project's volumes only.

---

## 16. Conclusion and Future Work

### Conclusion — what has been demonstrated

**Distributed storage at scale.** A containerized HDFS cluster stores 34.7 GB across 230 blocks with a HEALTHY fsck report and zero corruption, holding both the 28.1 GB raw corpus and the 6.6 GB preprocessed dataset.

**A genuine MapReduce implementation.** A Hadoop Streaming job processed all 132,500,000 rows in 613 seconds across 225 map tasks, visible in the YARN ResourceManager as an application of type MAPREDUCE.

**A measured, controlled engine comparison.** The same aggregation in Spark completed in 65.3 seconds — a 9.4x speedup — with the mechanism identified from job counters (2.09 GB of MapReduce disk spill versus an in-memory Spark shuffle). Both engines reconciled to the same 132,500,000 transactions and 222 states.

**Lossless distributed preprocessing in Scala.** A native Spark–Scala pipeline on YARN cleaned the full dataset in 19 minutes 8 seconds with **zero rows dropped**, replacing the row-discarding `.na.drop()` approach with sentinel imputation and explicit flag columns, and producing 43 engineered columns as partitioned Parquet.

**Honest methodology.** Synthetic components are disclosed, the leakage-prevention scheme is structural rather than probabilistic, accuracy is explicitly rejected as a headline metric for a 0.165 % positive class, and the single-host limitation is stated rather than dressed up as fault tolerance.

### Future Work

**Immediate — this project, pending execution:**
- Phase 5 — distributed LR / RF / GBT training with cost-sensitive weighting
- Phase 6 — measured tuning benchmarks for caching, broadcast joins and partition pruning
- Phase 7 — CatBoost serving model with TreeSHAP attributions
- Phase 8 — precomputed dashboard aggregates
- Phases 9–10 — FastAPI inference service and React analytics dashboard
- Phase 11 — Spark Structured Streaming scorer, completing the Velocity requirement

**Beyond this project:**
- Multi-node deployment with replication factor 2 or higher, enabling genuine fault-tolerance demonstration and realistic network shuffle costs
- Kafka ingestion in place of the HDFS file source, for true event-driven streaming
- Concept-drift monitoring, identified as a limitation in MDPI Electronics (2025)
- Federated learning across institutions, so fraud patterns can be shared without sharing customer data
- Graph-based features (shared device or merchant across users) using GraphFrames
- Automated hyperparameter search with Optuna across the Spark cluster

---

## 17. Conference Paper Status

**Status: NOT STARTED**

No paper has been drafted or submitted. If pursued, the most defensible contribution from this work is the **controlled engine benchmark with mechanistic attribution** — 9.4x measured on identical input, with the 2.09 GB disk-spill figure as the explanation — combined with the **lossless-imputation methodology** (rows-in equals rows-out, with per-field imputation counters), which addresses a genuine gap in the surveyed literature where preprocessing row loss is rarely quantified.

Target venues consistent with the literature survey would be IEEE ICSCNA, IEEE SCOPES or IEEE iCAST. This is a possibility to evaluate once Phases 5–11 are complete and results are available, not a current commitment.

---

## 18. References

[1] **Apache Hadoop Architecture & HDFS** — K. Shvachko, H. Kuang, S. Radia, and R. Chansler, "The Hadoop Distributed File System," in *IEEE 26th Symposium on Mass Storage Systems and Technologies (MSST)*, 2010, pp. 1–10.

[2] **Distributed In-Memory Processing** — M. Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing," in *9th USENIX Symposium on Networked Systems Design and Implementation (NSDI)*, 2012, pp. 15–28.

[3] **Machine Learning at Scale (Spark MLlib)** — X. Meng et al., "MLlib: Machine Learning in Apache Spark," *Journal of Machine Learning Research (JMLR)*, vol. 17, no. 34, pp. 1–7, 2016.

[4] **Explainable Artificial Intelligence (SHAP)** — S. M. Lundberg and S.-I. Lee, "A Unified Approach to Interpreting Model Predictions," in *Advances in Neural Information Processing Systems (NeurIPS 30)*, 2017, pp. 4765–4774.

[5] **Credit Card Fraud & Class Imbalance** — A. Dal Pozzolo et al., "Learnings from Credit Card Fraud Detection," in *IEEE Computational Intelligence Magazine*, vol. 10, no. 4, pp. 16–29, 2015.

[6] **Dataset** — E. Altman et al., "Credit Card Transactions: Fraud Detection and Other Analyses," Kaggle.

[7] **CatBoost** — L. Prokhorenkova et al., "CatBoost: unbiased boosting with categorical features," in *Advances in Neural Information Processing Systems (NeurIPS 31)*, 2018.

[8] **Base Paper** — IEEE ICSCNA, "Distributed Spark Architecture with MLlib for Credit Card Fraud Detection," 2024.

---

*Last updated: 10 September 2026, after Phase 4 completion. Phases 5–8 implemented and committed, pending execution.*
