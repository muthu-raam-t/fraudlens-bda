# Spark execution internals — Phase 5 training

Captured from the Spark History Server (:18080),
application_1790161484681_0003, 2% stratified sample.
DAG structure is identical to the full run; only task counts scale.

| Metric | Value |
|---|---|
| Completed jobs | 265 |
| Completed stages | 345 |
| Skipped stages | 108 |
| Total tasks | 15,034 |
| SQL queries | 72 |
| Executors | 2 (+ driver), 4 cores |
| Cumulative input read | 59.9 GiB |
| Shuffle read / write | 630 MiB / 574 MiB |
| Task time / wall clock | 8.3 min / 2.6 min |
| GC time | 28 s |

## Interpretation

- One job per action; 265 jobs across LR, RF and GBT training.
- Stages split at wide transformations (Exchange nodes in the SQL DAG).
- 108 skipped stages: results already materialised by `train_df.cache()`.
  Three models re-read the same features; only the first computed them.
  This is the caching optimisation visible at DAG level.
- 59.9 GiB read from a ~130 MB cached dataset reflects the iterative nature
  of gradient boosting — one pass per boosting round.
- 8.3 min task time in 2.6 min wall clock = 3.2x parallel speedup on 4 cores;
  short of 4x due to shuffle waits and GC.

## API and cluster manager

- DataFrame API throughout — Catalyst and Tungsten apply; no RDD usage.
- Cluster manager: YARN, `--master yarn --deploy-mode client`.
- Under YARN the Spark UI is served via the ResourceManager proxy;
  direct requests to :4040 are redirected by AmIpFilter.
