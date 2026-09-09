#!/usr/bin/env python3
"""
PHASE 3 (part 2) -- the SAME aggregation as the MapReduce job, in Spark.

This exists purely for the benchmark. It reads the identical raw CSV from HDFS
and produces the identical per-state result, so the wall-clock difference
between the two runs is attributable to the ENGINE and nothing else.

MapReduce spills every intermediate key/value pair to local disk between the
map and reduce phases. Spark keeps the shuffle in memory where it fits. That is
the whole point of the comparison, and it is the number your "Big Data vs.
Conventional Processing" slide has been missing.

Deliberately fair:
    - same input path, same 28 GB
    - same grouping key and same two measures
    - no caching, no Parquet, no pre-cleaning -- Spark gets no advantage that
      MapReduce was not also given

Run:
    bash scripts/run_spark_agg.sh
"""
import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

HDFS_BASE = "/fraudlens"
INPUT = "hdfs://namenode:9000" + HDFS_BASE + "/dataset/unprocessed"
OUTPUT = ("hdfs://namenode:9000" + HDFS_BASE +
          "/mapreduce_output/fraud_by_state_spark")

# Every column read as a string: inferSchema would trigger an extra full pass
# over 28 GB and make the timing comparison meaningless.
RAW_SCHEMA = StructType([
    StructField("user", StringType(), True),
    StructField("card", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True),
    StructField("time", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", StringType(), True),
    StructField("errors", StringType(), True),
    StructField("is_fraud", StringType(), True),
    StructField("device_metadata", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("FraudLens-SparkAgg-FraudRateByState")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 62)
    print(" PHASE 3 (part 2): the same aggregation, in Spark")
    print("=" * 62)
    print("input : " + INPUT)
    print("output: " + OUTPUT)
    print()

    start = time.time()

    df = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(INPUT)
    )

    # Same normalisation the mapper applies: uppercase, blank -> UNKNOWN.
    state = F.upper(F.trim(F.col("merchant_state")))
    state = F.when(state.isNull() | (state == ""), F.lit("UNKNOWN")).otherwise(state)
    fraud = F.when(F.lower(F.trim(F.col("is_fraud"))).isin("yes", "1", "true"), 1)\
             .otherwise(0)

    agg = (
        df.select(state.alias("merchant_state"), fraud.alias("is_fraud"))
        .groupBy("merchant_state")
        .agg(
            F.count("*").alias("total_txns"),
            F.sum("is_fraud").alias("fraud_txns"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(100.0 * F.col("fraud_txns") / F.col("total_txns"), 4),
        )
        .orderBy(F.desc("total_txns"))
    )

    # coalesce(1): the result is ~50 rows, so a single output file is right.
    agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT)

    elapsed = time.time() - start

    rows = agg.collect()
    total = sum(r["total_txns"] for r in rows)
    frauds = sum(r["fraud_txns"] for r in rows)

    print()
    print("=" * 62)
    print(" SPARK ELAPSED: {}m {}s ({:.1f} seconds)".format(
        int(elapsed // 60), int(elapsed % 60), elapsed))
    print("=" * 62)
    print()
    print("{:<10}{:>15}{:>12}{:>10}".format(
        "STATE", "TRANSACTIONS", "FRAUD", "RATE %"))
    print("{:<10}{:>15}{:>12}{:>10}".format(
        "-----", "------------", "-----", "------"))
    for r in rows[:15]:
        print("{:<10}{:>15,}{:>12,}{:>10.4f}".format(
            r["merchant_state"], r["total_txns"],
            r["fraud_txns"], r["fraud_rate_pct"]))

    print()
    print("distinct states      : {}".format(len(rows)))
    print("transactions counted : {:,}".format(total))
    print("fraud transactions   : {:,}".format(frauds))
    print("overall fraud rate   : {:.4f}%".format(100.0 * frauds / total))
    print()
    print("Compare this elapsed time against the MapReduce run. Both read the")
    print("same 28 GB and computed the same result.")

    # Persisted so the dashboard (Phase 8) and the report can reuse it.
    summary = {
        "engine": "spark",
        "job": "fraud_rate_by_state",
        "input": INPUT,
        "elapsed_seconds": round(elapsed, 1),
        "distinct_states": len(rows),
        "total_transactions": int(total),
        "fraud_transactions": int(frauds),
        "overall_fraud_rate_pct": round(100.0 * frauds / total, 4),
        "top_states": [
            {
                "state": r["merchant_state"],
                "total_txns": int(r["total_txns"]),
                "fraud_txns": int(r["fraud_txns"]),
                "fraud_rate_pct": float(r["fraud_rate_pct"]),
            }
            for r in rows
        ],
    }
    with open("/artifacts/spark_agg_timing.json", "w") as fh:
        json.dump(summary, fh, indent=2)
    print("Timing and results written to artifacts/spark_agg_timing.json")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
