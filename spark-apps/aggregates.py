#!/usr/bin/env python3
"""
PHASE 8 -- Dashboard aggregates.

Spark computes every chart's data ONCE over the full 132.5M rows and writes
small JSON files (kilobytes). The dashboard then reads those instead of
querying 5.9 GB of Parquet on every page load -- which is the whole point of
precomputing aggregates in a big-data pipeline.

Writes:
    /artifacts/aggregates/fraud_by_state.json
    /artifacts/aggregates/fraud_by_mcc.json
    /artifacts/aggregates/fraud_by_hour.json
    /artifacts/aggregates/fraud_by_device.json
    /artifacts/aggregates/amount_distribution.json
    /artifacts/aggregates/dataset_summary.json
    /artifacts/aggregates/index.json         (manifest of all of the above)

Run:  bash scripts/run_aggregates.sh
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
INPUT = HDFS + "/fraudlens/dataset/preprocessed"
OUT_DIR = "/artifacts/aggregates"


def log(m):
    print("[phase8] " + str(m), flush=True)


def write_json(name, payload):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2)
    size_kb = os.path.getsize(path) / 1024.0
    log("wrote %-28s %7.1f KB" % (name, size_kb))
    return {"file": name, "size_kb": round(size_kb, 1)}


def rows_to_dicts(df, limit=None):
    d = df.limit(limit) if limit else df
    return [r.asDict(recursive=True) for r in d.collect()]


def main():
    spark = (SparkSession.builder
             .appName("FraudLens-Phase8-DashboardAggregates")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()
    log("=" * 62)
    log("PHASE 8: dashboard aggregates")
    log("=" * 62)

    df = spark.read.parquet(INPUT)
    # Only the columns the charts need -- Parquet is columnar, so this alone
    # cuts the bytes read dramatically.
    slim = df.select(
        "merchant_state", "mcc", "hour", "device_os", "vpn_flag",
        "amount_abs", "is_online", "is_night", "error_flag",
        "is_foreign_or_unknown", "year", "is_fraud",
    ).cache()

    total = slim.count()
    frauds = slim.filter(F.col("is_fraud") == 1).count()
    log("rows: {:,}   fraud: {:,}  ({:.4f}%)".format(
        total, frauds, 100.0 * frauds / total))

    manifest = []

    def rate_by(col_name, alias=None, min_rows=1000, limit=None, order_desc=True):
        key = alias or col_name
        agg = (slim.groupBy(col_name)
               .agg(F.count("*").alias("total_txns"),
                    F.sum("is_fraud").alias("fraud_txns"))
               .filter(F.col("total_txns") >= min_rows)
               .withColumn("fraud_rate_pct",
                           F.round(100.0 * F.col("fraud_txns") /
                                   F.col("total_txns"), 4))
               .withColumnRenamed(col_name, key))
        agg = agg.orderBy(F.desc("total_txns") if order_desc else F.asc(key))
        return rows_to_dicts(agg, limit)

    # ---- by state (also comparable with the MapReduce output) -----------
    manifest.append(write_json("fraud_by_state.json", {
        "description": "fraud rate by merchant state",
        "rows": rate_by("merchant_state", "state", min_rows=1000, limit=60),
    }))

    # ---- by MCC (merchant category code) --------------------------------
    manifest.append(write_json("fraud_by_mcc.json", {
        "description": "fraud rate by ISO 18245 merchant category code",
        "rows": rate_by("mcc", "mcc", min_rows=5000, limit=40),
    }))

    # ---- by hour of day -------------------------------------------------
    manifest.append(write_json("fraud_by_hour.json", {
        "description": "fraud rate by hour of day (0-23)",
        "rows": rate_by("hour", "hour", min_rows=1, order_desc=False),
    }))

    # ---- device / channel signals ---------------------------------------
    device = {
        "by_os": rate_by("device_os", "os", min_rows=1000),
        "by_vpn": rate_by("vpn_flag", "vpn", min_rows=1),
        "by_online": rate_by("is_online", "online", min_rows=1),
        "by_night": rate_by("is_night", "night", min_rows=1),
        "by_error": rate_by("error_flag", "had_error", min_rows=1),
        "by_foreign": rate_by("is_foreign_or_unknown", "foreign", min_rows=1),
        "caveat": ("vpn and os come from the synthetically appended "
                   "device_metadata column; their fraud correlation is "
                   "injected, not real-world"),
    }
    manifest.append(write_json("fraud_by_device.json", device))

    # ---- amount distribution, fraud vs legitimate ------------------------
    buckets = (slim
               .withColumn("bucket",
                           F.when(F.col("amount_abs") < 10, "0-10")
                           .when(F.col("amount_abs") < 25, "10-25")
                           .when(F.col("amount_abs") < 50, "25-50")
                           .when(F.col("amount_abs") < 100, "50-100")
                           .when(F.col("amount_abs") < 250, "100-250")
                           .when(F.col("amount_abs") < 500, "250-500")
                           .when(F.col("amount_abs") < 1000, "500-1000")
                           .otherwise("1000+"))
               .groupBy("bucket")
               .agg(F.count("*").alias("total_txns"),
                    F.sum("is_fraud").alias("fraud_txns"))
               .withColumn("fraud_rate_pct",
                           F.round(100.0 * F.col("fraud_txns") /
                                   F.col("total_txns"), 4)))
    order = ["0-10", "10-25", "25-50", "50-100", "100-250",
             "250-500", "500-1000", "1000+"]
    rows = {r["bucket"]: r.asDict() for r in buckets.collect()}
    manifest.append(write_json("amount_distribution.json", {
        "description": "transaction count and fraud rate by amount bucket",
        "rows": [rows[b] for b in order if b in rows],
    }))

    # ---- overall summary -------------------------------------------------
    stats = slim.select(
        F.avg("amount_abs").alias("mean_amount"),
        F.expr("percentile_approx(amount_abs, 0.5)").alias("median_amount"),
        F.max("amount_abs").alias("max_amount"),
        F.countDistinct("merchant_state").alias("distinct_states"),
        F.countDistinct("mcc").alias("distinct_mcc"),
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
    ).collect()[0].asDict()

    summary = {
        "total_transactions": int(total),
        "fraud_transactions": int(frauds),
        "fraud_rate_pct": round(100.0 * frauds / total, 4),
        "raw_size_gb": 28.1,
        "preprocessed_size_gb": 5.9,
        "hdfs_blocks": 230,
        "replication_factor": 1,
        "mean_amount": round(float(stats["mean_amount"]), 2),
        "median_amount": round(float(stats["median_amount"]), 2),
        "max_amount": round(float(stats["max_amount"]), 2),
        "distinct_states": int(stats["distinct_states"]),
        "distinct_mcc": int(stats["distinct_mcc"]),
        "year_range": [int(stats["first_year"]), int(stats["last_year"])],
    }
    manifest.append(write_json("dataset_summary.json", summary))

    manifest.append(write_json("index.json", {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source": INPUT,
        "files": [m["file"] for m in manifest],
        "total_size_kb": round(sum(m["size_kb"] for m in manifest), 1),
        "elapsed_seconds": round(time.time() - t0, 1),
    }))

    slim.unpersist()
    log("")
    log("all aggregates in %s" % OUT_DIR)
    log("total size: %.1f KB (vs 5.9 GB of Parquet)" %
        sum(m["size_kb"] for m in manifest))
    log("PHASE 8 COMPLETE in %.1f s" % (time.time() - t0))
    spark.stop()


if __name__ == "__main__":
    main()
