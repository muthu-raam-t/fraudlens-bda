#!/usr/bin/env python3
"""
PHASE 6 -- Performance tuning benchmarks.

The course requires "performance tuning (e.g. partitioning, caching in
Spark)". This script MEASURES three tuning techniques on the real 132.5M-row
dataset instead of asserting they help.

  A. CACHING          same aggregation twice: uncached vs cached
  B. BROADCAST JOIN   small lookup joined with broadcast vs a shuffle join
  C. PARTITION PRUNING  filtering on the partition column (year) vs a
                        non-partition column

Writes /artifacts/tuning_benchmark.json for the report and the dashboard.

Run:  bash scripts/run_tuning.sh
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
INPUT = HDFS + "/fraudlens/dataset/preprocessed"
ZIP_LOOKUP = HDFS + "/fraudlens/lookup/zip_coords.csv"
OUT = "/artifacts/tuning_benchmark.json"


def log(m):
    print("[phase6] " + str(m), flush=True)


def timed(label, fn):
    t0 = time.time()
    result = fn()
    secs = time.time() - t0
    log("%-46s %8.1f s" % (label, secs))
    return secs, result


def main():
    spark = (SparkSession.builder
             .appName("FraudLens-Phase6-TuningBenchmark")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    # Disable adaptive execution so the comparisons are not silently
    # optimised into equivalence by Spark itself.
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    results = {}
    log("=" * 62)
    log("PHASE 6: performance tuning benchmarks")
    log("=" * 62)

    df = spark.read.parquet(INPUT)

    # ---------------- A. caching ----------------------------------------
    log("")
    log("A. CACHING")
    def agg_once(frame):
        return (frame.groupBy("merchant_state")
                .agg(F.count("*").alias("n"), F.sum("is_fraud").alias("f"))
                .collect())

    cold, _ = timed("uncached, pass 1 (reads Parquet from HDFS)",
                    lambda: agg_once(df))
    cold2, _ = timed("uncached, pass 2 (reads Parquet AGAIN)",
                     lambda: agg_once(df))

    slim = df.select("merchant_state", "is_fraud").cache()
    warm1, _ = timed("cached, pass 1 (populates the cache)",
                     lambda: agg_once(slim))
    warm2, _ = timed("cached, pass 2 (reads from memory)",
                     lambda: agg_once(slim))
    slim.unpersist()

    speedup = cold2 / warm2 if warm2 else 0
    results["caching"] = {
        "uncached_pass1_s": round(cold, 1),
        "uncached_pass2_s": round(cold2, 1),
        "cached_pass1_s": round(warm1, 1),
        "cached_pass2_s": round(warm2, 1),
        "speedup_repeat_access": round(speedup, 2),
        "note": ("compare uncached pass 2 with cached pass 2: both are repeat "
                 "reads of the same data"),
    }
    log("--> repeat-access speedup from caching: %.2fx" % speedup)

    # ---------------- B. broadcast vs shuffle join ----------------------
    log("")
    log("B. BROADCAST JOIN vs SHUFFLE JOIN")
    zip_df = (spark.read.option("header", "true").csv(ZIP_LOOKUP)
              .select(F.lpad(F.col("zip"), 5, "0").alias("zip"),
                      F.col("latitude").cast("double").alias("lat"))
              .dropDuplicates(["zip"]))

    def join_and_count(small):
        return df.select("zip", "amount_abs").join(small, on="zip", how="inner").count()

    # Force a shuffle join by raising the auto-broadcast threshold out of play
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    shuffle_s, _ = timed("shuffle join (broadcast disabled)",
                         lambda: join_and_count(zip_df))

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
    bcast_s, _ = timed("broadcast join (800 KB table broadcast)",
                       lambda: join_and_count(F.broadcast(zip_df)))

    results["join_strategy"] = {
        "shuffle_join_s": round(shuffle_s, 1),
        "broadcast_join_s": round(bcast_s, 1),
        "speedup": round(shuffle_s / bcast_s, 2) if bcast_s else 0,
        "lookup_table_size_kb": 799,
        "note": "small dimension table joined against 132.5M fact rows",
    }
    log("--> broadcast speedup: %.2fx" %
        (shuffle_s / bcast_s if bcast_s else 0))

    # ---------------- C. partition pruning ------------------------------
    log("")
    log("C. PARTITION PRUNING (partitionBy year/month)")
    pruned_s, pruned_n = timed(
        "filter on year (partition column -> prunes files)",
        lambda: df.filter(F.col("year") == 2018).count())
    scan_s, scan_n = timed(
        "filter on hour (NOT a partition column -> full scan)",
        lambda: df.filter(F.col("hour") == 13).count())

    results["partition_pruning"] = {
        "partition_filter_s": round(pruned_s, 1),
        "partition_filter_rows": int(pruned_n),
        "non_partition_filter_s": round(scan_s, 1),
        "non_partition_filter_rows": int(scan_n),
        "speedup": round(scan_s / pruned_s, 2) if pruned_s else 0,
        "note": ("year is a partition column so Spark reads only the matching "
                 "directories; hour is not, so every file is scanned"),
    }
    log("--> pruning speedup: %.2fx" % (scan_s / pruned_s if pruned_s else 0))

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as fh:
        json.dump(results, fh, indent=2)

    log("")
    log("=" * 62)
    log("SUMMARY -- put these numbers on the tuning slide")
    log("=" * 62)
    log("caching (repeat access) : %.2fx faster" %
        results["caching"]["speedup_repeat_access"])
    log("broadcast vs shuffle    : %.2fx faster" %
        results["join_strategy"]["speedup"])
    log("partition pruning       : %.2fx faster" %
        results["partition_pruning"]["speedup"])
    log("written to " + OUT)
    spark.stop()


if __name__ == "__main__":
    main()
