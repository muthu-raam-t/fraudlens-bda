# Final Project Review — PPT Content

**Course:** Big Data Analytics (23AID302) · **Presentation:** 15 min · **Viva:** 10 min

**26 slides.** Every slide sized to fit. Image file named on each slide that needs one.

---

## Slide map

| # | Slide | Image |
|---|---|---|
| 1 | Title | — |
| 2 | Problem statement | `fig5_class_imbalance.png` |
| 3 | Literature review | — |
| 4 | Research gaps identified | `fig8_research_gaps.png` |
| 5 | Gaps addressed in this work | `fig9_gaps_filled.png` |
| 6 | Objectives | — |
| 7 | Why big data is needed | `fig7_why_bigdata.png` |
| 8 | Dataset | — |
| 9 | Scala implementation | — |
| 10 | System architecture diagram | `fig0_architecture.png` |
| 11–13 | Architecture walkthrough | — |
| 14 | Spark workflow diagram | `spark_workflow.png` |
| 15–17 | Spark workflow walkthrough | — |
| 18 | Spark ML pipeline | — |
| 19 | Results — engine benchmark | `fig1_engine_benchmark.png` |
| 20 | Results — model comparison | `fig2_model_benchmark.png` |
| 21 | Results — query optimisation | `fig3_optimisation.png` |
| 22 | Results — data reduction | `fig4_data_funnel.png` |
| 23 | Results — streaming & system | screenshot S7 or S8 |
| 24 | Business insights | — |
| 25 | Challenges and limitations | — |
| 26 | Future work | — |
| 27 | References and GitHub | — |

---

# 1 · Title

**Distributed Credit Card Fraud Analytics Framework**

*Benchmarked MapReduce vs Spark Execution, Imbalance-Aware Distributed Training, and Kafka-Backed Real-Time Inference*

Team members · register numbers · faculty · school · academic year.

---

# 2 · Problem Statement

**Image:** `fig5_class_imbalance.png` — right half or lower band.

- **Volume:** A 28.1 GB ledger exceeds the memory of any single machine.
- **Rarity:** Fraud is 0.1650% of records — 218,622 in 132.5 million.
- **Misleading metrics:** Predicting "legitimate" always scores 99.83% accuracy.
- **Latency:** Overnight batch scoring records a loss; it cannot prevent one.
- **Skew:** Training and serving built separately drift apart over time.

---

# 3 · Literature Review

*(Your existing table.)*

---

# 4 · Research Gaps Identified

**Image:** `fig8_research_gaps.png` — full width.

- **Scale:** Results reported on sampled or sub-gigabyte extracts, not full volume.
- **Engine choice:** Spark adopted on reputation, without a measured baseline.
- **Latency:** Batch pipelines report hours; none scores inside the authorisation window.
- **Transparency:** High accuracy reported, but no per-transaction justification.
- **Consequence:** No reviewed work addresses all four simultaneously.

> **Verify the matrix against your own reading of each paper before presenting.** It is drawn from the limitations you recorded in Review 1.

---

# 5 · Gaps Addressed in This Work

**Image:** `fig9_gaps_filled.png` — full width.

| Gap | Our response |
|---|---|
| Sub-scale evaluation | 28.1 GB · 132.5 M rows processed in full |
| Unmeasured engine choice | MapReduce vs Spark, identical work — 9.4×, cross-validated |
| Batch-only latency | Kafka micro-batch scoring in continuous operation |
| Opaque verdicts | SHAP attribution on every flagged transaction |

> **One line:** each gap is answered by a measurement, not by a claim.

---

# 6 · Objectives

| # | Objective | Success criterion |
|---|---|---|
| 1 | Deploy a containerised HDFS + YARN cluster | 28.1 GB ingested, `fsck` HEALTHY |
| 2 | Benchmark two execution engines on identical work | Both engines return identical counts |
| 3 | Build all preprocessing natively in Spark–Scala | Rows in = rows out, zero dropped |
| 4 | Train imbalance-aware distributed classifiers | Recall ≥ 0.90 on unseen cardholders |
| 5 | Score a live transaction stream | Continuous micro-batches, no restart |
| 6 | Explain every flagged transaction | SHAP contributions per verdict |

> **Primary objective:** curate once, consume twice — one distributed dataset serving both batch learning and real-time inference, with the distributed advantage measured rather than assumed.

---

# 7 · Why Big Data Is Needed Here

**Image:** `fig7_why_bigdata.png` — full width or right half.

- **It does not fit.** 28.1 GB against a 15 GB machine ceiling — 1.9× over, before overhead.
- **Sampling destroys the signal.** At 0.165%, a 1% sample leaves too few fraud cases to learn from.
- **Rare patterns need full volume.** Fraud behaviour appears across users, states and merchant categories at once.
- **One machine reads sequentially.** 230 HDFS blocks are read in parallel by many tasks.
- **Measured proof.** The same job took 613 s on MapReduce and 65.3 s on Spark — distribution is the design, not a convenience.

> **Say aloud:** distribution here is not an optimisation. Without it the job does not run at all.

---

# 8 · Dataset

### Source and acquisition

- **Dataset:** *Credit Card Transactions* — Erik Altman (IBM), Kaggle.
- **Downloaded size:** 2.2 GB CSV · 24,386,900 transactions · 15 columns.
- **Key attributes:** User, Card, Year, Month, Day, Time, Amount, Use Chip, Merchant Name / City / State, Zip, MCC, Errors?, Is Fraud?

### Scaling to big-data volume

| Step | Detail |
|---|---|
| Pool sampling | 17.7% drawn from **every** chunk — full user coverage, 1,600 users |
| Jitter | Amount ±3%, day ±2, time resampled |
| Semi-structured field | `device_metadata` appended as nested JSON — network, geo, hardware |
| Null injection | ~2% nulls in merchant city / state / zip |
| Leakage control | `user % 5 == 0` → holdout; whole users, never rows |
| **Output** | **28.1 GB CSV · 132,500,000 rows** |

### Storage in Hadoop HDFS

| Parameter | Value |
|---|---|
| NameNode | 1 (metadata) |
| DataNode | 1 (blocks) |
| Block size | 128 MB |
| Blocks | 230 |
| Replication factor | 1 |
| Integrity | `fsck` HEALTHY, 0 under-replicated |
| Raw path | `/fraudlens/dataset/unprocessed/` — 28.1 GB CSV |
| Curated path | `/fraudlens/dataset/preprocessed/` — 6.6 GB Parquet |

### The five V's

| V | In this project |
|---|---|
| **Volume** | 28.1 GB · 132.5 M rows · 230 blocks |
| **Velocity** | Kafka micro-batches scored continuously |
| **Variety** | Flat CSV + nested JSON + lookup dimension table |
| **Veracity** | Row conservation verified, `fsck` HEALTHY, both engines agreed |
| **Value** | 6.6 GB distilled to 18.7 KB of decision-ready aggregates |

> **Note:** one format on disk — CSV. The JSON is a column *inside* it, not a separate file.

---

# 9 · Scala Implementation — Distributed Preprocessing

**File:** `spark-apps/preprocess.scala` · run with `spark-shell --master yarn`

### Key operations

| # | Operation | Spark–Scala function |
|---|---|---|
| 1 | Declare schema explicitly | `StructType` — avoids a full 28 GB inference pass |
| 2 | Clean currency strings | `regexp_replace(col("amount"), "[$,]", "")` → `"$1,204.55"` to `1204.55` |
| 3 | Repair ZIP codes | `lpad(split(col("zip"), "\\.")(0), 5, "0")` → `"4917.0"` to `"04917"` |
| 4 | Parse nested JSON | `from_json(col("device_metadata"), deviceSchema)` |
| 5 | Enrich geography | `join(broadcast(zipDf), ...)` — small table shipped, no shuffle |
| 6 | Flag outliers | `stat.approxQuantile("amount_clean", Array(0.99), 0.0001)` |
| 7 | Derive behaviour features | `amount_vs_user_mean`, `amount_zscore`, `is_weekend`, `is_night` |
| 8 | Build the date column | `to_date(format_string("%04d-%02d-%02d", ...))` |
| 9 | Write partitioned output | `.repartition(120, $"year", $"month").write.partitionBy("year","month").parquet(...)` |

### Decisions behind the code

- **Sentinel imputation, never `.na.drop()`** — dropping rows would silently lose fraud cases. Nulls become flag columns instead.
- **Broadcast join, not shuffle join** — the ZIP table is 27,321 rows against 132.5 M; shipping the small side is 3.79× faster.
- **Scala, not PySpark** — runs natively in the Spark JVM, so a 28 GB pass carries no Python serialisation cost.

> **Verified:** rows in 132,500,000 = rows out 132,500,000 · dropped 0 · elapsed 19 m 08 s · output 6.6 GB Parquet, 43 columns.

---

# 10 · System Architecture

**Image:** `fig0_architecture.png` — full slide.

- Twelve phases, every figure measured on this cluster.
- Nine containerised services: HDFS, YARN, Spark, Kafka, Zookeeper.
- HDFS is the only persistence tier — no external database.

---

# 11 · Architecture Walkthrough (1 of 3)

**Phases 0 – 3**

| Phase | What it does | Measured |
|---|---|---|
| **0 · Source acquisition** | Kaggle ledger downloaded | 2.2 GB · 24.4 M rows |
| **1 · Synthetic scaling** | Pool-sampled, jittered, JSON appended | 28.1 GB · 132.5 M rows |
| **2 · Distributed storage** | Written to HDFS, split into blocks | 230 blocks · RF 1 · HEALTHY |
| **3 · Engine benchmark** | Same aggregation, two engines | 613 s vs 65.3 s → **9.4×** |

> **Point to make:** the same raw CSV went to both engines, so the benchmark measures the engine — not the file format.

---

# 12 · Architecture Walkthrough (2 of 3)

**Phases 4 – 7**

| Phase | What it does | Measured |
|---|---|---|
| **4 · Spark–Scala ETL** | Clean, enrich, derive, partition | 19 m 08 s · 0 rows dropped |
| **5 · Distributed training** | LR, RF, GBT with `weightCol` | GBT recall 0.9121 |
| **6 · Tuning benchmarks** | Pruning, broadcast, caching | 11.73× · 3.79× · 1.22× |
| **7 · Serving model** | CatBoost for millisecond responses | 582 of 600 iterations |

> **Point to make:** two models, two jobs. MLlib proves the pipeline scales to 132 M rows; CatBoost makes the live demo usable.

---

# 13 · Architecture Walkthrough (3 of 3)

**Phases 8 – 11**

| Phase | What it does | Measured |
|---|---|---|
| **8 · Dashboard aggregates** | Pre-computed once in Spark | 6.6 GB → 18.7 KB JSON |
| **9 · FastAPI service** | Model loaded once at startup | Millisecond verdicts + SHAP |
| **10 · React dashboard** | Live scoring and analytics tabs | Probability gauge, SHAP waterfall |
| **11 · Structured Streaming** | Kafka source, micro-batch scoring | 12 batches · 6,000 scored · 894 flagged |

> **Point to make:** FastAPI runs outside Spark deliberately — a SparkSession per request would cost seconds; the API answers in milliseconds.

---

# 14 · Spark Workflow

**Image:** `spark_workflow.png` — full slide.

- DataFrame API throughout; RDDs are the execution layer beneath.
- Narrow operations stay local; wide operations force a shuffle.
- Nothing executes until an action triggers the plan.

---

# 15 · Spark Workflow Walkthrough (1 of 3)

| # | Step | What happens in our pipeline |
|---|---|---|
| **1** | **Read** | `spark.read.schema(...)` builds a DataFrame over HDFS blocks. An explicit `StructType` avoids a full inference pass over 28 GB. |
| **2** | **Narrow transformations** | `select`, `filter`, `withColumn` — each task works on its own partition, no data crosses the network. |
| **3** | **Wide transformations** | `groupBy(state)` and the user/MCC aggregations force a shuffle; this is where distributed cost is incurred. |

> **Viva point:** narrow versus wide is the single most important distinction — every optimisation we achieved worked by avoiding a wide operation.

---

# 16 · Spark Workflow Walkthrough (2 of 3)

| # | Step | What happens in our pipeline |
|---|---|---|
| **4** | **Catalyst optimiser** | Rewrites the logical plan into a physical plan — pushes filters down, selects the broadcast join strategy. |
| **5** | **DAG → stages → tasks** | One job splits at shuffle boundaries into stages, each stage into one task per partition. Our run: 265 jobs, 345 stages, 15,034 tasks. |
| **6** | **Partitioning** | `repartition(120, year, month)` then `partitionBy` on write — this is what later enables 11.73× partition pruning. |

> **Viva point:** 108 of 345 stages were **skipped** — Catalyst reusing shuffle output instead of recomputing it.

---

# 17 · Spark Workflow Walkthrough (3 of 3)

| # | Step | What happens in our pipeline |
|---|---|---|
| **7** | **Caching** | `cache()` on the train and test DataFrames, reused across three model fits. Measured gain: 1.22×. |
| **8** | **Action** | `count`, `write`, `collect` trigger execution. Steps 1–7 only built the plan. |
| **9** | **RDD layer + YARN** | DataFrames compile to RDDs, whose lineage allows a lost partition to be recomputed. YARN allocates the executor containers. |

> **Viva point:** caching returned only 1.22× because partition pruning had already cut the read. We report it as measured, not as expected.

---

# 18 · Spark ML Pipeline

### Features — 43 curated columns

| Group | Examples |
|---|---|
| Numeric | `amount_clean`, `amount_zscore`, `amount_vs_user_mean` |
| Temporal | `hour`, `day_of_week`, `is_weekend`, `is_night`, `day_of_year` |
| Categorical | `mcc`, `merchant_state`, `use_chip`, `device_os` |
| Derived flags | `is_outlier`, `is_refund`, `is_foreign_or_unknown` |
| Geographic | `merchant_lat`, `merchant_lon`, haversine distance |

### Pipeline stages

```
StringIndexer  →  OneHotEncoder*  →  VectorAssembler  →  StandardScaler*  →  Classifier
                  (* linear model only)
```

### Two deliberately different assemblies

- **Trees (RF, GBT):** `StringIndexer → VectorAssembler` — trees split on ordinal codes directly.
- **Linear (LR):** adds `OneHotEncoder` and `StandardScaler(withMean=false)` — without them, unscaled amounts swamp the binary flags.

### Algorithms and tuning

| Item | Setting |
|---|---|
| Algorithms | LogisticRegression · RandomForestClassifier · GBTClassifier |
| Imbalance handling | `weightCol` — minority weighted 623× (negatives / positives) |
| Loss | Log loss (LR) · Gini impurity (RF) · Log loss with gradient boosting (GBT) |
| Split | User-disjoint: `user % 5 == 0` → 26.5 M-row holdout |
| Selection metric | **PR-AUC**, not accuracy or ROC-AUC |
| Serving model | CatBoost + TreeSHAP, prior-shift calibrated |

> **Why PR-AUC:** at a 0.165% positive rate, ROC-AUC separates the three models by 4 points; PR-AUC separates them by 10×.

---

# 19 · Results — Engine Benchmark

**Image:** `fig1_engine_benchmark.png`

| Engine | Runtime | Execution profile |
|---|---|---|
| Hadoop MapReduce | **613 s** | 225 map tasks · 2.09 GB spilled · 265 M spilled records |
| Apache Spark | **65.3 s** | In-memory DAG, no intermediate materialisation |
| **Speed-up** | **9.4×** | Same cluster, same YARN allocation, same input file |

**Three lines to say:**
1. Both engines returned identical output — 222 states, 132,500,000 rows, 218,622 fraud, 0.1650%.
2. The gap is explained by the 2.09 GB MapReduce spilled to local disk between map and reduce.
3. Because the results agree exactly, the 9.4× is a verified measurement rather than a claim.

---

# 20 · Results — Model Comparison

**Image:** `fig2_model_benchmark.png`

| Model | PR-AUC | ROC-AUC | Recall | F1 | Training time |
|---|---|---|---|---|---|
| **Gradient-Boosted Trees** | **0.2196** | **0.9477** | **0.9121** | 0.0200 | 4,740 s |
| Random Forest | 0.1832 | 0.9276 | 0.8938 | 0.0185 | 1,020 s |
| Logistic Regression | 0.0223 | 0.9060 | 0.8727 | 0.0135 | 1,524 s |
| Random baseline | 0.00165 | 0.500 | — | — | — |

**Three lines to say:**
1. ROC-AUC separates these three models by four points; PR-AUC separates them by a factor of ten.
2. GBT's 0.2196 against a 0.165% base rate is roughly 135× better than random.
3. Logistic Regression confirms the imbalance is non-linear — a linear boundary barely beats chance.

---

# 21 · Results — Query Optimisation

**Image:** `fig3_optimisation.png`

| Technique | Speed-up | Mechanism |
|---|---|---|
| Partition pruning | **11.73×** | Reads only the year/month directories the predicate needs |
| Broadcast join | **3.79×** | Ships the 800 KB lookup table to every executor, no shuffle |
| DataFrame caching | **1.22×** | Keeps a reused intermediate in executor memory |

**Three lines to say:**
1. The largest gain came from not reading data at all — the cheapest work is the work never scheduled.
2. Broadcast join removed a shuffle of 132.5 M rows against an 800 KB table.
3. Caching returned only 1.22× because pruning had already cut the read to 2.6 s; we report it as measured.

---

# 22 · Results — Data Reduction

**Image:** `fig4_data_funnel.png`

| Stage | Size | Form |
|---|---|---|
| Raw ledger | 28.1 GB | CSV in HDFS |
| Curated store | 6.6 GB | Parquet + Snappy, 43 columns |
| Dashboard artefacts | 18.7 KB | Pre-aggregated JSON |

**Three lines to say:**
1. Parquet plus Snappy gave 4.26× compression with zero information loss — 132,500,000 rows in, the same out.
2. The columnar layout is what makes partition pruning possible downstream.
3. The dashboard reads 18.7 KB, so it never queries 6.6 GB on page load.

---

# 23 · Results — Streaming and System

**Image:** screenshot S7 (Spark Structured Streaming tab) or S8 (live console).

| Metric | Measured |
|---|---|
| Micro-batches processed | 12, uninterrupted |
| Transactions scored | 6,000 |
| Flagged for review | 894 |
| Jobs / stages / tasks | 265 / 345 (108 skipped) / 15,034 |
| Total input read | 59.9 GiB |

**Three lines to say:**
1. The consumer runs as a YARN application and scores each micro-batch against the saved pipeline model.
2. 108 skipped stages show Catalyst reusing shuffle output rather than recomputing it.
3. The streaming path reads the same curated store as training, so no feature can drift between them.

---

# 24 · Business Insights

- **Catching 91 of every 100 frauds** turns a post-hoc loss report into a prevention control.
- **Risk is concentrated, not uniform** — fraud rate varies sharply by state and merchant category, so review effort can be targeted rather than spread.
- **A verdict inside the authorisation window** is worth more than a more accurate verdict delivered tomorrow.
- **Every flagged case carries its reason** — SHAP contributions let an analyst act without re-investigating from scratch.
- **The threshold is a business decision, not a technical one** — it is set by the cost of a false alarm against the cost of a missed fraud.
- **Scaling is a configuration change** — volume growth is absorbed by adding nodes, not by rewriting the pipeline.

---

# 25 · Challenges and Limitations

### Challenges faced

| Challenge | Resolution |
|---|---|
| YARN rejected the shuffle service | Hadoop property naming convention in container env vars |
| PySpark refused to run | Driver on Python 3.7, executor on 3.5 — streaming rewritten in Scala |
| Outlier flag returned all zeros | `approxQuantile` relative error tightened 0.01 → 0.0001 |
| Host froze during CatBoost | Row caps, dtype downcasting, cluster stopped before host training |
| Spark 3.0 missing newer functions | `make_date` and `nullif` replaced with 3.0-compatible equivalents |

### Limitations acknowledged

- **Single DataNode and NodeManager** — block distribution and YARN scheduling are genuine, but multi-node scaling is not empirically validated.
- **The dataset is synthetic**, and the `device_metadata` fields we added are fabricated — one of them ranks second by SHAP importance.
- **CatBoost's 0.6943 PR-AUC is not comparable** to GBT's 0.2196 — it was measured on a 4.32% base rate, not 0.165%.
- **The retrain loop is methodological, not automated** — iterations were run manually.
- **Replication factor 1** means no redundancy; fault tolerance rests on RDD lineage and YARN retry.

> **Say the second point aloud.** Disclosing a limitation you found yourself is stronger than being asked about it.

---

# 26 · Future Work

- **Unbiased feature audit** — retrain without the synthetically-originated fields to isolate their true contribution.
- **Comparable evaluation protocol** — re-score every model on the identical 26.5 M-row holdout so PR-AUC compares directly.
- **Multi-node scaling study** — add DataNodes and NodeManagers to measure scaling efficiency against this baseline.
- **Concept drift monitoring** — rolling PR-AUC and feature-distribution surveillance on the live stream.
- **Automated retraining loop** — trigger re-weighting and retraining from the recall gate without manual intervention.
- **Cost-matrix threshold optimisation** — tune the decision cut against real false-positive and missed-fraud costs.

---

# 27 · References and Repository

**GitHub:** `github.com/muthu-raam-t/fraudlens-bda`

1. K. Shvachko et al., "The Hadoop Distributed File System," *IEEE MSST*, 2010.
2. M. Zaharia et al., "Resilient Distributed Datasets," *USENIX NSDI*, 2012.
3. X. Meng et al., "MLlib: Machine Learning in Apache Spark," *JMLR*, vol. 17, 2016.
4. M. Armbrust et al., "Spark SQL: Relational Data Processing in Spark," *ACM SIGMOD*, 2015.
5. M. Armbrust et al., "Structured Streaming," *ACM SIGMOD*, 2018.
6. J. Kreps et al., "Kafka: A Distributed Messaging System for Log Processing," *NetDB*, 2011.
7. S. M. Lundberg and S.-I. Lee, "A Unified Approach to Interpreting Model Predictions," *NeurIPS*, 2017.
8. A. Dal Pozzolo et al., "Learnings from Credit Card Fraud Detection," *IEEE CIM*, vol. 10, no. 4, 2015.
9. T. Saito and M. Rehmsmeier, "The Precision-Recall Plot Is More Informative," *PLOS ONE*, vol. 10, no. 3, 2015.
10. E. Altman, "Credit Card Transactions Dataset," Kaggle, 2021.

*Plus your five Review 1 literature-survey papers.*

---

# Timing — 15 minutes

| Section | Slides | Minutes |
|---|---|---|
| Problem, gaps, objectives | 2–6 | 3 |
| Why big data, dataset | 7–8 | 2 |
| Scala, architecture | 9–13 | 3 |
| Spark workflow, ML pipeline | 14–18 | 3 |
| Results | 19–23 | 3 |
| Insights, limitations, future work | 24–27 | 1 |

The architecture and Spark walkthrough slides are where you will overrun. Four tables of three rows each should take 30 seconds per slide, not two minutes. Rehearse those with a timer.
