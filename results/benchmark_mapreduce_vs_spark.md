# MapReduce vs Spark — identical aggregation, identical data

Job: fraud rate per merchant state
Input: /fraudlens/dataset/unprocessed (28.1 GB raw CSV, 132,500,000 rows)
Cluster: 1 NameNode, 1 DataNode (RF=1), YARN 6 GB / 8 vcores, single host

| Engine | Time | Notes |
|---|---|---|
| Hadoop MapReduce | 613 s (10m 13s) | 225 map tasks, 4 reducers |
| Apache Spark      |  65.3 s        | in-memory shuffle |
| **Speedup**       | **9.4x**       | |

## Why MapReduce is slower — measured, not asserted
From the MapReduce job counters:
- FILE bytes written: 2,087,890,445 (2.09 GB spilled to local disk)
- Spilled records: 265,000,000
- Shuffle bytes: 1,017,514,866

MapReduce materialises every intermediate key/value pair to disk between the
map and reduce phases. Spark performs the same shuffle in memory. Same input,
same output, same cluster — the only variable is the execution engine.

## Both engines agree
- distinct states: 222
- transactions counted: 132,500,000
- fraud transactions: 218,622
- overall fraud rate: 0.1650%

Raw CSV was used for BOTH runs on purpose: MapReduce cannot read Parquet
without a custom input format, so feeding Spark the Parquet would have
measured the file format rather than the engine.
