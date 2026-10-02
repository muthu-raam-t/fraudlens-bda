# FraudLens — Presentation Content

Slide-by-slide content. Bullet points only.
All figures measured on the project cluster.

---

## SLIDE 1 — Title

**FraudLens**

**Distributed Credit Card Fraud Analytics Framework**

Scalable Feature Engineering and Class-Imbalance Classification on
Containerized HDFS and Apache Spark

- Name, Roll Number
- Department, Faculty In Charge
- Institution, Academic Year

---

## SLIDE 2 — Abstract

- End-to-end distributed fraud detection pipeline on Hadoop and Apache Spark
- Dataset: 132,500,000 transactions · 28.1 GB · 230 HDFS blocks
- Identical aggregation benchmarked on two engines: MapReduce 613 s vs Spark 65.3 s → **9.4× speedup**
- Native Spark–Scala preprocessing on YARN: 19 m 08 s, **zero rows dropped**
- Three classifiers trained distributed on the full dataset: LR, Random Forest, GBT
- Best model: **GBT — PR-AUC 0.2196, recall 0.912, 135× lift over random**
- Explainable serving: CatBoost with exact TreeSHAP, millisecond latency
- Real-time layer: Spark Structured Streaming — 12 micro-batches, 6,000 rows scored
- Every performance claim measured under controlled conditions, not asserted

---

## SLIDE 3 — Introduction and Background

**Domain**
- Financial data engineering + distributed big data analytics

**Industry motivation**
- Global card fraud losses: USD 24–30 billion annually
- Transaction volumes make manual review impossible
- Detection must be automated and increasingly real-time

**Why conventional tooling fails**
- Pandas and scikit-learn load the entire dataset into one machine's RAM
- 28 GB on a 15 GB host → immediate Out-Of-Memory failure
- Single-threaded row-by-row transformation is prohibitively slow

**Why Hadoop and Spark**
- HDFS partitions data into blocks across nodes → parallel reads
- YARN allocates compute as containers → multiple engines share one cluster
- Spark keeps intermediates in memory → no disk write between stages

---

## SLIDE 4 — Problem Statement

**Four V's of Big Data**

| V | Requirement | Implementation | Evidence |
|---|---|---|---|
| Volume | GB–TB scale | 28.1 GB, 132,500,000 rows | 230 HDFS blocks, fsck HEALTHY |
| Velocity | Streaming / real-time | Structured Streaming scorer | 12 micro-batches, 6,000 rows |
| Variety | Two or more data shapes | Structured CSV + nested JSON | `from_json` schema parsing |
| Veracity | Noisy / incomplete data | Regex cleanup + flagged imputation | 0 rows dropped, 0 nulls |

**The fifth and hardest problem — extreme class imbalance**
- Fraud: 218,622 of 132,500,000 transactions = **0.1650%**
- Approximately 16 fraud cases per 10,000 transactions
- A model predicting "never fraud" achieves **99.835% accuracy**
- Accuracy is therefore unusable as an evaluation metric
- PR-AUC, precision, recall and threshold sweeps used instead

---

## SLIDE 5 — Objectives

1. Deploy a containerized HDFS cluster with zero data corruption
2. Implement a genuine Hadoop MapReduce job on YARN
3. Quantify MapReduce vs Spark on identical workloads
4. Implement native Spark–Scala ETL with in-memory cleansing
5. Train and benchmark distributed ensembles under cost-sensitive weighting
6. Measure Spark tuning techniques rather than assert them
7. Deliver explainable, low-latency per-transaction predictions
8. Serve results through an analytical dashboard
9. Demonstrate real-time streaming ingestion and scoring

**Status: all nine completed and evidenced**

---

## SLIDE 6 — Literature Survey

| # | Venue & Year | Technique | Limitation |
|---|---|---|---|
| 1 | IEEE ICSCNA 2024 *(base paper)* | Spark + MLlib (XGBoost, RF, SVM), PowerBI | RF latency 1.8 hrs; SVM fails to scale, recall 93% |
| 2 | IEEE SCOPES 2024 | CNN+LSTM, Voting Classifier, RF | Overfitting to 100%; SVM recall drops to 29% |
| 3 | MDPI Electronics 2025 | PySpark, XGBoost, CatBoost, Optuna | High cluster memory; concept-drift sensitive |
| 4 | IEEE iCAST 2025 | K-Means + Isolation Forest + K-NN | Multi-stage delay; error propagation |
| 5 | IJRAS 2026 | Hadoop vs Spark Streaming, XAI, Federated | 2–4 hr batch latency; GDPR conflict |

---

## SLIDE 7 — Research Gaps Addressed

| Gap identified in literature | Response in this project |
|---|---|
| Performance claims asserted, not measured | Identical aggregation on both engines: 613 s vs 65.3 s |
| Row-dropping preprocessing inflates metrics | Imputation with flag columns; rows-in = rows-out, printed |
| Resampled synthetic data leaks across splits | User-disjoint split makes leakage structurally impossible |
| Black-box scores cannot be audited | TreeSHAP attached to every served prediction |
| Accuracy quoted on ~0.1% positive classes | PR-AUC and threshold sweeps throughout |

---

## SLIDE 8 — Proposed System Architecture

**[Insert architecture diagram — `docs/fraudlens_architecture.png`]**

**Twelve phases**

| # | Phase | Result |
|---|---|---|
| 0 | Kaggle source acquisition | 24.4 M rows, 2.2 GB |
| 1 | Synthetic scaling | 132,500,000 rows, 28.1 GB |
| 2 | HDFS ingestion | 230 blocks, replication 1 |
| 3 | MapReduce aggregation | 613 s |
| 3b | Spark aggregation | 65.3 s → 9.4× |
| 4 | Scala preprocessing | 19 m 08 s, 0 rows dropped |
| 5 | Spark MLlib training | LR, RF, GBT on full dataset |
| 6 | Tuning benchmarks | 11.73× / 3.79× / 1.22× |
| 7 | CatBoost + TreeSHAP | Serving model |
| 8 | Dashboard aggregates | 6.6 GB → 18.7 KB |
| 9 | FastAPI inference service | Calibrated scoring |
| 10 | React risk console | Two-tab dashboard |
| 11 | Structured Streaming scorer | Velocity requirement |

---

## SLIDE 9 — Technology Stack Relationships

```
HADOOP
├── HDFS ......... distributed STORAGE      (34.7 GB, 230 blocks)
├── YARN ......... resource MANAGEMENT      (6144 MB, 8 vcores)
└── MapReduce .... execution ENGINE (disk-based)

APACHE SPARK ..... execution ENGINE (in-memory)
                   runs ON YARN, reads/writes HDFS
```

- Not competing systems — layered responsibilities
- HDFS stores; YARN allocates; MapReduce and Spark compute
- Both engines run as YARN applications on the same cluster
- Shared scheduler is what makes the performance comparison controlled

| Phase | Engine | Scheduler | Language |
|---|---|---|---|
| 3 | MapReduce | YARN | Python (Streaming) |
| 3b | Spark | YARN | Python (PySpark) |
| 4 | Spark | YARN | **Scala** |
| 5, 6, 8 | Spark | YARN | Python (PySpark) |
| 11 | Spark Structured Streaming | YARN | **Scala** |

---

## SLIDE 10 — Apache Spark Architecture Used

**API selected: DataFrame**

| API | Schema | Catalyst optimizer | Type safety | Used |
|---|---|---|---|---|
| RDD | No | No | Compile-time | No |
| **DataFrame** | Yes | **Yes** | Runtime | **Yes** |
| Dataset | Yes | Yes | Compile-time | No (Scala/Java only) |

- DataFrame operations pass through Catalyst and Tungsten; RDDs do not
- Explicit `StructType` declared — avoids a full inference pass over 28 GB
- `DataFrame` in Scala is an alias for `Dataset[Row]`
- Dataset API unavailable in PySpark (Python has no compile-time types)

**Cluster manager: YARN**
- Options: Standalone, YARN, Mesos, Kubernetes
- All jobs submitted with `--master yarn --deploy-mode client`

**Execution model**
- **Transformations** (lazy): `withColumn`, `select`, `filter`, `join`, `groupBy`
- **Actions** (trigger execution): `count`, `collect`, `write`, `show`
- **Narrow** transformations: no shuffle — `map`, `filter`, `withColumn`, broadcast join
- **Wide** transformations: shuffle → stage boundary — `groupBy`, `repartition`, `join`
- One action = one **job**; jobs split into **stages** at shuffle boundaries
- One **task** per partition per stage — 28.1 GB ÷ 128 MB ≈ 225 tasks in the read stage

**Spark MLlib**
- `spark.ml` (DataFrame-based, current) used — not `spark.mllib` (RDD-based, maintenance)
- Components: `Pipeline`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`, `VectorAssembler`

---

## SLIDE 11 — Implementation: Cluster and Resources

**Six Docker containers, single Ubuntu host (15 GB RAM, 16 cores)**

| Service | Role | Port |
|---|---|---|
| namenode | HDFS metadata | 9870 |
| datanode1 | HDFS block storage | 9864 |
| resourcemanager | YARN scheduling | 8088 |
| nodemanager | YARN container execution | 8042 |
| spark-master | Spark driver host | 8080 |
| spark-worker | Spark executor | 8081 |

**Resource allocation — three layers**

| Layer | Parameter | Value |
|---|---|---|
| Physical host | — | 15 GB RAM, 16 cores |
| Offered to YARN | `yarn.nodemanager.resource.memory-mb` | 6144 MB |
| | `yarn.nodemanager.resource.cpu-vcores` | 8 |
| | `yarn.scheduler.maximum-allocation-mb` | 4096 MB |
| Requested per job | `--num-executors` | 2 |
| | `--executor-memory` | 2g |
| | `--executor-cores` | 2 |
| | `--driver-memory` | 2g |

- YARN performs no computation — it allocates containers in which engines run
- Replication factor 1: single DataNode; RF=2 would leave every block under-replicated
- Limitation stated rather than claiming undemonstrable fault tolerance

---

## SLIDE 12 — Implementation: Dataset

**Source**
- Erik Altman et al., *Credit Card Transactions*, Kaggle
- 24,386,900 real transactions, 2.2 GB, 15 columns

**Scaling to big-data volume**
- Pool-sampled 17.7% of every chunk across the entire source file
- Jitter applied: amount ±3%, day ±2, time resampled
- Output: 5 chunks × 6.04 GB = 28.1 GB, 132,500,000 rows

**Data provenance — explicitly declared**

| Columns | Provenance |
|---|---|
| user, card, year, month, use_chip, merchant fields, zip, mcc, is_fraud | **Real** (Kaggle) |
| amount, day, time | Real values, **jittered** |
| `device_metadata` (VPN, IP, geo, OS) | **Fabricated** — supplies semi-structured component |

**Leakage prevention**
- Rows sampled with replacement → random split would duplicate across train/test
- Users partitioned by `user % 5`: chunks 1–4 train, chunk 5 held out
- Cross-split contamination structurally impossible, not merely unlikely

---

## SLIDE 13 — Implementation: Preprocessing (Scala on YARN)

**Cleansing operations**

| Operation | Detail |
|---|---|
| Explicit schema | `StructType` declared — no `inferSchema` pass |
| Amount cleanup | `"$1,204.55"` → `1204.55` via `regexp_replace` |
| Label cast | `"Yes"/"No"` → `1/0` |
| ZIP repair | `"4917.0"` → `"04917"` via `lpad` |
| Nested JSON | `from_json` against declared schema, null-safe |
| Imputation | Sentinels **plus flag columns** recording affected rows |
| Broadcast join | 27,321 ZIP rows against 132,500,000 |

**Derived features**
- `txn_timestamp`, `day_of_week`, `is_weekend`, `day_of_year`
- `amount_log`, `amount_capped`, `is_outlier`, `is_refund`
- `device_merchant_distance_km` (haversine)
- `is_online`, `is_night`, `is_foreign_or_unknown`

**Output**
- Parquet + Snappy, `partitionBy("year", "month")`
- `repartition(120, year, month)` prevents the small-files problem

**Verification**
```
rows in  : 132,500,000
rows out : 132,500,000
dropped  : 0
null amounts / states / zips / timestamps : 0
```

**Imputation counts (recorded, not hidden)**

| Field | Rows imputed |
|---|---|
| merchant_state | 17,130,754 |
| zip | 17,974,284 |
| device geo | 5,300,156 |
| device hardware | 3,977,162 |
| merchant_city | 2,650,183 |

---

## SLIDE 14 — Results: Storage

| Metric | Value |
|---|---|
| Raw dataset | 28.1 GB CSV |
| Rows | 132,500,000 |
| HDFS blocks | 230 |
| Average block size | 131,360,676 B (128 MB) |
| Average replication | 1.0 |
| fsck status | HEALTHY, 0 under-replicated |
| Preprocessed dataset | 6.6 GB Snappy Parquet |
| Total DFS used | 34.7 GB |
| Ingestion time | 20 minutes |

**Storage reduction: 28.1 GB → 6.6 GB (4.3×)**
- Binary typing: 8-byte double vs 9-character `"$1,204.55"`
- Columnar layout: 132.5 M contiguous values compress far better than interleaved rows
- Dictionary encoding: 222 distinct states stored once with references
- Snappy compression applied on top
- **Row count proves nothing was lost** — compression, not data loss

---

## SLIDE 15 — Results: MapReduce vs Spark Benchmark

**Identical job, identical data, identical cluster**

| Engine | Time | Configuration |
|---|---|---|
| Hadoop MapReduce | **613 s** (10 m 13 s) | 225 map tasks, 4 reducers |
| Apache Spark | **65.3 s** | 2 executors × 2 GB / 2 cores |
| | **9.4× speedup** | |

**Mechanism — from MapReduce job counters, not from literature**

| Counter | Value |
|---|---|
| FILE bytes written | **2,087,890,445** (2.09 GB spilled to disk) |
| Spilled records | 265,000,000 |
| Reduce shuffle bytes | 1,017,514,866 |
| HDFS bytes read | 30,213,882,459 |

- MapReduce materialises every intermediate key/value pair to disk between map and reduce
- Spark performs the same shuffle in memory

**Both engines agree — confirming a like-for-like comparison**

| Output | MapReduce | Spark |
|---|---|---|
| Distinct states | 222 | 222 |
| Transactions counted | 132,500,000 | 132,500,000 |
| Fraud transactions | 218,622 | 218,622 |
| Overall fraud rate | 0.1650% | 0.1650% |

- Raw CSV used for both: MapReduce cannot read Parquet
- Using Parquet for Spark would have measured file format, not engine

---

## SLIDE 16 — Results: Descriptive Analytics

**Top merchant states by volume**

| State | Transactions | Fraud | Rate % |
|---|---|---|---|
| UNKNOWN | 17,130,754 | 107,864 | 0.6297 |
| CA | 13,803,212 | 10,005 | 0.0725 |
| TX | 9,521,486 | 5,685 | 0.0597 |
| FL | 7,773,136 | 4,907 | 0.0631 |
| NY | 7,726,232 | 4,685 | 0.0606 |
| OH | 4,752,649 | 6,922 | 0.1456 |

**Fraud rate by merchant category (MCC)**

| MCC | Category | Transactions | Rate % |
|---|---|---|---|
| 7995 | Gambling | 317,238 | **0.2783** |
| 4829 | Money transfer | 6,128,906 | 0.1846 |
| 5411 | Grocery | 15,539,263 | 0.0773 |
| 5812 | Restaurant | 9,769,410 | 0.0645 |
| 5541 | Fuel | 14,339,493 | 0.0572 |

**Interpretation**
- UNKNOWN dominated by online transactions carrying no merchant state
- Card-not-present fraud genuinely higher — 4× the national average
- Gambling 3.6× grocery — consistent with domain expectation

---

## SLIDE 17 — Evaluation Metrics: Definitions

**Confusion matrix — the basis of all classification metrics**

| | Predicted FRAUD | Predicted CLEAN |
|---|---|---|
| **Actually FRAUD** | TP — caught | FN — missed |
| **Actually CLEAN** | FP — false alarm | TN — correctly ignored |

| Metric | Formula | Question answered |
|---|---|---|
| Accuracy | (TP+TN) / total | How often right overall? |
| Precision | TP / (TP+FP) | When flagged, how often correct? |
| Recall | TP / (TP+FN) | What fraction of fraud is caught? |
| F1 | 2·(P·R)/(P+R) | Balance of both, at one threshold |
| PR-AUC | Area under precision-recall curve | Model quality across all thresholds |
| ROC-AUC | Area under TPR–FPR curve | Same, but flattering when positives are rare |
| Brier | mean((predicted − actual)²) | Are predicted probabilities calibrated? |
| RMSE | √mean((predicted − actual)²) | Average numeric error — **regression only** |

**Classification vs regression**
- Classification predicts a **category** — this project
- Regression predicts a **number** — RMSE, MAE, R² apply there, not here

**Why accuracy is excluded from all results**
- 99.835% of transactions are legitimate
- Predicting "never fraud" achieves 99.835% accuracy and catches zero fraud

---

## SLIDE 18 — Results: Distributed Model Training

**Trained on all 132,500,000 rows — total 135 m 50 s**

| Model | PR-AUC | ROC-AUC | Recall | F1 | Train time |
|---|---|---|---|---|---|
| **GBTClassifier** | **0.2196** | 0.9477 | 0.9121 | 0.0200 | 4,740 s |
| RandomForest | 0.1832 | 0.9276 | 0.8938 | 0.0185 | 1,020 s |
| LogisticRegression | 0.0223 | 0.9060 | 0.8727 | 0.0135 | 1,524 s |

**Training configuration**
- Train 106,000,000 rows (175,424 fraud) · Holdout 26,500,000 rows (43,198 fraud)
- User-disjoint split · `weightCol` = 603.3 for fraud rows
- Two feature pipelines: indexed for trees; one-hot + standardised for LR

**Key observation**
- ROC-AUC separates the models by 4 percentage points → suggests near-equivalence
- PR-AUC separates them by a **factor of ten**
- On a 0.165% positive class, ROC-AUC is dominated by the true-negative pool
- This gap is the empirical justification for reporting PR-AUC

---

## SLIDE 19 — Model Roles

| Model | Learning mechanism | Role |
|---|---|---|
| **LogisticRegression** | Single weighted sum; no interactions | Baseline — establishes that complexity is required |
| **RandomForest** | Many independent trees, averaged | Ensemble baseline — 83% of GBT performance at 4.6× speed |
| **GBTClassifier** | Sequential trees, each correcting the last | **Primary, deployed** — captures feature interactions |
| **CatBoost** | Ordered boosting, single machine | **Serving only** — TreeSHAP + millisecond latency |

**Why GBT wins — feature interactions**
- $1,450 at a grocery store is anomalous; $1,450 at a gambling merchant is routine
- LR cannot represent this — one weight per feature
- RF can find it only if a tree splits on both in the right order
- GBT builds toward it: early trees learn amount, later trees learn the exception

**Models deliberately excluded**

| Model | Reason |
|---|---|
| SVM | Does not scale; base paper reports recall failure |
| CNN / LSTM | Requires more data; degrades explainability |
| Decision Tree | Overfits alone — 100% training accuracy in literature |
| XGBoost | Not natively distributed in Spark without external JAR |

---

## SLIDE 20 — Results: Threshold Selection

**CatBoost threshold sweep**

| Threshold | Precision | Recall | Transactions flagged |
|---|---|---|---|
| 0.1 | 13.1% | 97.6% | 320,905 |
| 0.3 | 20.4% | 94.0% | 199,099 |
| 0.5 | 23.1% | 91.6% | 171,198 |
| 0.7 | 29.7% | 78.8% | 114,717 |
| 0.8 | 59.4% | 61.3% | 44,534 |
| **0.9** | **77.6%** | **55.0%** | **30,629** |

- At 0.5 — catch 92% of fraud; three of four flags incorrect
- At 0.9 — catch 55% of fraud; three of four flags correct
- Neither is objectively correct — depends on cost of missed fraud vs cost of review

**Dashboard uses lift, not absolute probability**
- Base fraud rate: 0.165%
- "30× baseline" = flag above 0.165% × 30 = 4.95%
- An absolute 50% threshold would essentially never fire on this class balance

---

## SLIDE 21 — Results: Performance Tuning

**Measured with adaptive query execution disabled**

| Technique | Baseline | Tuned | Speedup |
|---|---|---|---|
| **Partition pruning** — filter on `year` vs `hour` | 1.2 s | 0.1 s | **11.73×** |
| **Broadcast vs shuffle join** — 800 KB × 132.5 M rows | 15.7 s | 4.2 s | **3.79×** |
| **Caching** — repeat access | 2.6 s | 2.1 s | 1.22× |

**Caching result reported as measured**
- Benchmark selects only two columns
- Parquet columnar layout already reduced the read to 2.6 s
- Little I/O remained for caching to eliminate
- Caching pays off on wide reads and expensive recomputation
- Measurement correct; understates the technique for this query

**Partition pruning validates the `partitionBy("year","month")` decision**

---

## SLIDE 22 — Results: Explainability

**TreeSHAP feature importances (mean absolute contribution)**

| Rank | Feature | Value |
|---|---|---|
| 1 | `mcc` | 0.8649 |
| 2 | **`vpn_flag`** | **0.8540** |
| 3 | `merchant_state` | 0.3404 |
| 4 | `device_merchant_distance_km` | 0.2865 |
| 5 | `use_chip` | 0.2489 |
| 6 | `amount_log` | 0.2163 |

**Critical disclosure**
- `vpn_flag` ranks second, statistically tied with `mcc`
- The column is **entirely synthetic** — fabricated by the data generator
- Generator injected a correlation making VPN transactions ~4× more likely fraud
- The models learn the generator as much as they learn fraud
- Same pattern in Spark ensembles: RF rank 1 (0.212), GBT rank 2 (0.174)
- Disclosed rather than omitted; `--drop-features` path exists to quantify

**Genuinely real signals ranking highly**
- `mcc`, `merchant_state`, `device_merchant_distance_km`, amount features

---

## SLIDE 23 — Results: Score Calibration

**Problem identified**
- CatBoost trained on downsampled negatives with `scale_pos_weight`
- Effective training prior ≈ balanced; real base rate 0.165%
- Raw scores inflated by ~1:600 in odds terms
- An ordinary grocery transaction returned ~50% and would be flagged

**Correction applied**
```
odds_true = odds_model × base_odds
base_odds = 0.00165 / (1 − 0.00165)
```
- Ranking unchanged; only the scale corrected
- Raw 0.6155 → calibrated **0.264%** = 1.6× base rate

**SHAP is conditional, not a category ranking**
- Gambling marginal fraud rate 0.2783% vs grocery 0.0773%
- Yet $1,450 grocery scores higher than $1,450 gambling
- Model states $1,450 is anomalous for grocery, unremarkable for gambling
- Marginal rates and conditional attributions answer different questions

---

## SLIDE 24 — Results: Streaming (Velocity)

| Metric | Value |
|---|---|
| Micro-batches processed | 12 |
| Rows scored | 6,000 |
| Flagged as fraud | 894 (14.9%) |
| Trigger interval | 5 seconds |
| Model | Spark MLlib GBT, 6 pipeline stages, loaded from HDFS |
| Broadcast join | 2,000 user rows + 109 MCC rows |

**Where the distributed model is deployed**
- GBT trained on all 132.5 M rows loaded from HDFS
- Applied to arriving micro-batches, verdicts written back as Parquet

**Engineering constraints handled**
- Streaming file sources **cannot infer schema** → explicit `StructType` declared
- PipelineModel requires engineered features a micro-batch cannot derive
- Batch statistics computed once and broadcast as a stream-static join

**Honest framing**
- Structured Streaming is **micro-batch, not per-event**
- Written in Scala: spark-master ships Python 3.7, nodemanager 3.5
- PySpark refuses to run across minor versions; Scala launches no Python workers

---

## SLIDE 25 — Challenges Encountered

| Problem | Root cause | Resolution |
|---|---|---|
| `ClassNotFoundException: -D` | `find` matched `-sources.jar` with no Main-Class | Target `tools/lib/` directly |
| `auxService:mapreduce_shuffle does not exist` | bde2020 env vars convert `___`→`-`; wrote `__` | Corrected naming, added ShuffleHandler |
| `.env` values silently ignored | Same naming convention error | Verified in live `yarn-site.xml` |
| apt 404 inside containers | Debian 9 end-of-life | Rewrote sources to `archive.debian.org` |
| `docker build` DNS failure | Buildkit network isolation | Runtime install via `docker compose exec` |
| f-string `SyntaxError` | Containers ship Python 3.5 | Converted to `.format()`, verified by AST walk |
| `ModuleNotFoundError: numpy` | `pyspark.ml` imports numpy at load | Distro package, not pip (no cp35 wheel) |
| `make_date` unresolved | Added to Scala API in Spark 3.3, cluster runs 3.0 | `to_date(format_string(...))` |
| `nullif` unresolved | SQL-only until Spark 3.5 | `when/otherwise` |
| `is_outlier` all zeros | `approxQuantile` error 0.01 returned near-maximum | Tightened to 0.0001 |
| Python 3.5 vs 3.7 worker mismatch | Alpine driver, Debian executors | Streaming job rewritten in Scala |
| Host froze during CatBoost | 10.6 M rows in pandas with 6 GB containers resident | Row caps, dtype downcasting, stop cluster first |
| Grocery scored ~50% fraud | Weighted training inflates raw scores | Prior-shift calibration in API |

---

## SLIDE 26 — Limitations

| Limitation | Disclosure |
|---|---|
| Single physical host | Architecture supports N nodes; RF=1 stated openly |
| Replication factor 1 | RF=2 with one DataNode leaves blocks under-replicated |
| Synthetic scaling | 132.5 M rows derive from 24.4 M real; user-disjoint split prevents leakage |
| Fabricated feature ranks 2nd | `vpn_flag` disclosed with measured SHAP importances |
| PR-AUC not comparable across test sets | Lift over random reported instead |
| CatBoost trained on a sample | Labelled as serving model; sample size recorded |
| Velocity is micro-batch | Described accurately, not as per-event streaming |
| Interactive endpoint uses CatBoost, not Spark | Spark GBT deployed in streaming layer; both documented |

---

## SLIDE 27 — Critical Methodological Finding

**CatBoost PR-AUC 0.6943 is NOT better than GBT PR-AUC 0.2196**

| Model | Test rows | Test fraud | Base rate | PR-AUC | Lift over random |
|---|---|---|---|---|---|
| Spark GBT | 26,500,000 | 43,198 | 0.163% | 0.2196 | **135×** |
| CatBoost | 1,000,000 | 43,198 | 4.32% | 0.6943 | 16× |

- A random classifier scores PR-AUC equal to the base rate
- Correct comparison is lift over that baseline
- Both models evaluated on the **identical 43,198 fraud cases**; only negatives differ
- CatBoost's higher figure is an artefact of a 26× easier class balance
- **The distributed GBT is substantially the stronger model**
- Reporting raw PR-AUC values would have reversed the conclusion

---

## SLIDE 28 — Big Data vs Conventional Processing

**Conceptual comparison**

| Dimension | Conventional (single node) | Big Data (Hadoop + Spark) |
|---|---|---|
| Storage | One local disk, no replication | HDFS blocks across DataNodes |
| Memory | Entire dataset into RAM → OOM at 28 GB | Partitions streamed, spills gracefully |
| Execution | Single-threaded, row by row | Concurrent across partitions |
| Scaling | Vertical — larger machine | Horizontal — more nodes |
| Fault tolerance | Process dies, work lost | YARN retries; Spark recomputes from lineage |

**Measured comparison**

| Comparison | Baseline | Optimised | Gain |
|---|---|---|---|
| MapReduce vs Spark | 613 s | 65.3 s | **9.4×** |
| CSV vs Snappy Parquet | 28.1 GB | 6.6 GB | **4.3× smaller, 0 rows lost** |
| Full scan vs partition pruning | 1.2 s | 0.1 s | **11.73×** |
| Shuffle vs broadcast join | 15.7 s | 4.2 s | **3.79×** |
| Parquet vs precomputed aggregates | 6.6 GB | 18.7 KB | Dashboard never queries big data |

---

## SLIDE 29 — Requirement Coverage

| Requirement | Implementation | Status |
|---|---|---|
| Volume | 28.1 GB, 132.5 M rows, 230 blocks | Complete |
| Velocity | Structured Streaming, 12 micro-batches | Complete |
| Variety | Structured CSV + nested JSON | Complete |
| Veracity | Regex cleanup, flagged imputation, 0 dropped | Complete |
| Hadoop HDFS | Distributed storage, fsck HEALTHY | Complete |
| Hadoop MapReduce | Streaming job on YARN, 613 s | Complete |
| Spark DataFrame API | Phases 3b, 4, 5, 6, 8, 11 | Complete |
| Scala | Preprocessing and streaming | Complete |
| Python | Generation, MapReduce, training, serving | Complete |
| Spark ML | LR, RF, GBT on 132.5 M rows | Complete |
| Performance tuning | 11.73× / 3.79× / 1.22× measured | Complete |
| Visualization | React console + FastAPI | Complete |

---

## SLIDE 30 — Demonstration

**Live risk console — http://localhost:5173**
- Score an ordinary transaction: grocery, CA, chip, no VPN, 2 pm, $45
- Score a suspicious transaction: gambling, Mexico, online, 3 am, bad PIN, $1,450
- Observe lift figure and SHAP waterfall change between them
- Dataset analytics tab: fraud by state, MCC, hour; model comparison; benchmark

**Cluster evidence**
- **:9870** — HDFS: 1 live node, 34.7 GB, 230 blocks, HEALTHY
- **:8088 → FINISHED** — MAPREDUCE and SPARK applications on one page, all scheduled by YARN
- **:4040 → Structured Streaming** — live batch counter during streaming run
- **:18080** — Spark History Server: persisted DAG, stages, tasks, SQL plans

---

## SLIDE 31 — Conclusion

**Demonstrated**
- Distributed storage at scale — 34.7 GB, 230 blocks, fsck HEALTHY
- Genuine MapReduce implementation — 613 s over 132.5 M rows on YARN
- Controlled engine comparison — 9.4× with 2.09 GB disk spill as mechanism
- Lossless distributed preprocessing in Scala — 0 rows dropped, 43 columns
- Distributed model training — GBT PR-AUC 0.2196, 135× lift over random
- Measured tuning — 11.73× / 3.79× / 1.22×
- Explainable real-time serving — TreeSHAP per transaction, calibrated scores
- Streaming deployment — Spark GBT scoring live micro-batches

**Methodological position**
- Synthetic components disclosed
- Leakage prevention structural, not probabilistic
- Accuracy explicitly rejected as a headline metric
- Results that could have been reported flatteringly reported correctly instead

---

## SLIDE 32 — Future Work

**Directly indicated by results**
- Retrain without `vpn_flag`; quantify performance on real signal alone
- Re-evaluate CatBoost on the full 26.5 M holdout for direct PR-AUC comparability
- Tune operating threshold against an explicit cost model
- Add per-user time-window features using Spark window functions

**Architecture extensions**
- Kafka ingestion replacing the HDFS file source — true event-driven streaming
- Streamlit live dashboard consuming scored events
- Multi-node deployment with replication ≥ 2
- Spark History Server for permanent DAG and stage inspection

**Research directions**
- Concept-drift monitoring
- Federated learning across institutions
- Graph features via GraphFrames — shared device or merchant across users
- Automated hyperparameter search with Optuna

---

## SLIDE 33 — Conference Paper Status

**Status: not started**

**Defensible contributions if pursued**
1. Controlled engine benchmark with mechanistic attribution — 9.4× with disk-spill counter as explanation
2. Lossless-imputation methodology — rows-in = rows-out with per-field counters
3. Demonstration that PR-AUC is not comparable across test distributions — ranking reverses under lift correction

**Candidate venues**
- IEEE ICSCNA · IEEE SCOPES · IEEE iCAST

---

## SLIDE 34 — References

[1] K. Shvachko, H. Kuang, S. Radia, R. Chansler, "The Hadoop Distributed File System," *IEEE 26th MSST*, 2010, pp. 1–10.

[2] M. Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing," *9th USENIX NSDI*, 2012, pp. 15–28.

[3] X. Meng et al., "MLlib: Machine Learning in Apache Spark," *JMLR*, vol. 17, no. 34, pp. 1–7, 2016.

[4] S. M. Lundberg, S.-I. Lee, "A Unified Approach to Interpreting Model Predictions," *NeurIPS 30*, 2017, pp. 4765–4774.

[5] A. Dal Pozzolo et al., "Learnings from Credit Card Fraud Detection," *IEEE Computational Intelligence Magazine*, vol. 10, no. 4, pp. 16–29, 2015.

[6] L. Prokhorenkova et al., "CatBoost: unbiased boosting with categorical features," *NeurIPS 31*, 2018.

[7] E. Altman et al., "Credit Card Transactions: Fraud Detection and Other Analyses," Kaggle.

[8] IEEE ICSCNA, "Distributed Spark Architecture with MLlib for Credit Card Fraud Detection," 2024.

---

## APPENDIX — Viva Preparation

**Q: Which Spark API — RDD, DataFrame or Dataset?**
- DataFrame throughout; never called `.rdd` or `sc.textFile`
- DataFrames pass through Catalyst and Tungsten; RDDs do not
- Dataset unavailable in PySpark; `DataFrame` = `Dataset[Row]` in Scala

**Q: Which cluster manager?**
- YARN, all jobs `--master yarn --deploy-mode client`
- Standalone containers present but unused; spark-master hosts the driver only

**Q: How many jobs, stages, tasks?**
- One action = one job; `preprocess.scala` has ~13 actions
- Stages split at wide transformations: `repartition`, `groupBy`, `join`
- Tasks = one per partition per stage; 28.1 GB ÷ 128 MB ≈ 225 read tasks

**Q: Narrow vs wide transformations used?**
- Narrow: 30+ `withColumn`, `select`, `filter`, broadcast join
- Wide: `repartition(120, year, month)`, `groupBy`, shuffle joins

**Q: Is 6.6 GB really big data?**
- Input is 28.1 GB across 230 blocks and 132.5 M rows
- Output is compressed columnar Parquet; row count proves no loss

**Q: Why is precision only 1% in Spark models?**
- `weightCol` = 603 — a missed fraud costs 603× a false alarm
- Threshold is a deployment decision; sweep shows the trade-off

**Q: Did you use the Spark model or only CatBoost?**
- Both. Spark GBT is the distributed result, deployed in the streaming scorer
- CatBoost serves the interactive endpoint — SHAP has no Spark MLlib support

**Q: Why replication factor 1?**
- Single DataNode; RF=2 leaves every block permanently under-replicated

**Q: Why run the same job twice?**
- To make the performance claim measurable under controlled conditions
- Same input, same output, only the engine varied

**Q: Why is accuracy absent from every slide?**
- Predicting "never fraud" achieves 99.835% while catching zero fraud
