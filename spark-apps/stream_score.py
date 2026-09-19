#!/usr/bin/env python3
"""
PHASE 11 -- Spark Structured Streaming scorer.  (VELOCITY REQUIREMENT)

WHAT THIS CLOSES
    The course requires Velocity: "streaming data or real-time processing".
    Everything else in this project is batch. This job watches an HDFS
    directory and scores transactions as files arrive, applying the SAME GBT
    PipelineModel that Phase 5 trained on all 132,500,000 rows.

HONEST FRAMING
    Structured Streaming is MICRO-BATCH, not per-event. That is how Spark
    works and it is what will be described in the report.

TWO THINGS THAT CATCH PEOPLE OUT, HANDLED HERE
    1. A streaming file source CANNOT infer schema. The StructType below is
       declared explicitly and must match the preprocessed Parquet schema.
    2. The saved PipelineModel expects the engineered features (user amount
       statistics, MCC frequency). Those come from batch aggregates, so they
       are computed ONCE at startup from the preprocessed dataset and
       broadcast into the stream as a stream-static join.

Run:  bash scripts/run_streaming.sh
      bash scripts/feed_stream.sh     (in a second terminal)
"""
import argparse
import json
import os
import time

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

HDFS = "hdfs://namenode:9000"
BATCH = HDFS + "/fraudlens/dataset/preprocessed"
MODEL = HDFS + "/fraudlens/models/gbt"
IN_DIR = HDFS + "/fraudlens/streaming/input"
OUT_DIR = HDFS + "/fraudlens/streaming/verdicts"
CKPT = HDFS + "/fraudlens/streaming/checkpoint"
LOCAL_VERDICTS = "/artifacts/stream_verdicts"

# MUST match the preprocessed schema -- streaming file sources cannot infer it.
SCHEMA = StructType([
    StructField("user", IntegerType(), True),
    StructField("card", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("txn_timestamp", TimestampType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("day_of_year", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("amount_abs", DoubleType(), True),
    StructField("amount_log", DoubleType(), True),
    StructField("amount_capped", DoubleType(), True),
    StructField("is_outlier", IntegerType(), True),
    StructField("is_refund", IntegerType(), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("error_flag", IntegerType(), True),
    StructField("error_type", StringType(), True),
    StructField("vpn_flag", IntegerType(), True),
    StructField("device_ip", StringType(), True),
    StructField("device_os", StringType(), True),
    StructField("device_lat", DoubleType(), True),
    StructField("device_lon", DoubleType(), True),
    StructField("geo_missing", IntegerType(), True),
    StructField("hw_missing", IntegerType(), True),
    StructField("imputed_city", IntegerType(), True),
    StructField("imputed_state", IntegerType(), True),
    StructField("imputed_zip", IntegerType(), True),
    StructField("merchant_lat", DoubleType(), True),
    StructField("merchant_lon", DoubleType(), True),
    StructField("device_merchant_distance_km", DoubleType(), True),
    StructField("is_online", IntegerType(), True),
    StructField("is_foreign_or_unknown", IntegerType(), True),
    StructField("is_night", IntegerType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
])

FILL = ["user_amount_mean", "user_amount_std", "user_txn_count",
        "amount_vs_user_mean", "amount_zscore", "mcc_frequency"]


def log(m):
    print("[phase11] " + str(m), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-files-per-trigger", type=int, default=1)
    ap.add_argument("--timeout-min", type=float, default=30.0)
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("FraudLens-Phase11-StreamingScorer")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    log("=" * 62)
    log("PHASE 11: Structured Streaming fraud scorer")
    log("=" * 62)

    # ---- batch-derived features, computed once and broadcast -------------
    log("computing user and MCC statistics from the batch dataset")
    batch = spark.read.parquet(BATCH)
    user_stats = (batch.groupBy("user")
                  .agg(F.avg("amount_abs").alias("user_amount_mean"),
                       F.stddev("amount_abs").alias("user_amount_std"),
                       F.count("*").alias("user_txn_count"))
                  .fillna({"user_amount_std": 0.0}))
    mcc_freq = batch.groupBy("mcc").agg(F.count("*").alias("mcc_frequency"))
    user_stats = F.broadcast(user_stats.cache())
    mcc_freq = F.broadcast(mcc_freq.cache())
    log("user stats: %d rows | mcc stats: %d rows"
        % (user_stats.count(), mcc_freq.count()))

    log("loading the GBT PipelineModel trained in Phase 5")
    model = PipelineModel.load(MODEL)

    # ---- the stream -------------------------------------------------------
    log("watching %s" % IN_DIR)
    stream = (spark.readStream
              .schema(SCHEMA)
              .option("header", "true")
              .option("maxFilesPerTrigger", args.max_files_per_trigger)
              .csv(IN_DIR))

    enriched = (stream
                .join(user_stats, on="user", how="left")
                .join(mcc_freq, on="mcc", how="left")
                .withColumn("amount_vs_user_mean",
                            F.col("amount_abs") / (F.col("user_amount_mean") + F.lit(1.0)))
                .withColumn("amount_zscore",
                            (F.col("amount_abs") - F.col("user_amount_mean")) /
                            (F.col("user_amount_std") + F.lit(1.0)))
                .fillna(0.0, subset=FILL))

    scored = model.transform(enriched)

    from pyspark.ml.functions import vector_to_array
    verdicts = scored.select(
        F.col("user"),
        F.col("amount"),
        F.col("merchant_state"),
        F.col("mcc"),
        F.col("hour"),
        F.round(vector_to_array(F.col("probability"))[1], 6).alias("fraud_probability"),
        F.col("prediction").cast(IntegerType()).alias("predicted_fraud"),
        F.col("is_fraud").alias("actual_fraud"),
        F.current_timestamp().alias("scored_at"),
    )

    os.makedirs(LOCAL_VERDICTS, exist_ok=True)
    counter = {"batches": 0, "rows": 0, "flagged": 0}

    def sink(df, batch_id):
        """foreachBatch: persist to HDFS and mirror a slice for the API."""
        rows = df.count()
        if rows == 0:
            return
        flagged = df.filter(F.col("predicted_fraud") == 1).count()
        counter["batches"] += 1
        counter["rows"] += rows
        counter["flagged"] += flagged

        (df.write.mode("append").parquet(OUT_DIR))

        sample = df.orderBy(F.desc("fraud_probability")).limit(25).collect()
        path = os.path.join(LOCAL_VERDICTS, "batch_%05d.json" % batch_id)
        with open(path, "w") as fh:
            for r in sample:
                d = r.asDict()
                d["scored_at"] = str(d.get("scored_at"))
                fh.write(json.dumps(d) + "\n")

        log("batch %-4d | %6d rows | %5d flagged | totals: %d rows, %d flagged"
            % (batch_id, rows, flagged, counter["rows"], counter["flagged"]))

    query = (verdicts.writeStream
             .foreachBatch(sink)
             .outputMode("append")
             .option("checkpointLocation", CKPT)
             .trigger(processingTime="5 seconds")
             .start())

    log("")
    log("STREAM RUNNING. Drip files in with:  bash scripts/feed_stream.sh")
    log("Spark UI -> Structured Streaming tab shows the batch counter live.")
    log("Ctrl-C to stop, or it exits after %.0f minutes." % args.timeout_min)
    log("")

    try:
        query.awaitTermination(timeout=args.timeout_min * 60)
    except KeyboardInterrupt:
        log("interrupted")
    finally:
        query.stop()
        log("=" * 62)
        log("micro-batches processed : %d" % counter["batches"])
        log("rows scored             : %d" % counter["rows"])
        log("flagged as fraud        : %d" % counter["flagged"])
        log("verdicts in HDFS        : %s" % OUT_DIR)
        spark.stop()


if __name__ == "__main__":
    main()
