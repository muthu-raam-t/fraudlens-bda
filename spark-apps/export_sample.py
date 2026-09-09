#!/usr/bin/env python3
"""
PHASE 7a -- Export a stratified sample for the CatBoost serving model.

WHY A SAMPLE, AND WHY BE EXPLICIT ABOUT IT
    The Spark MLlib models from Phase 5 are the DISTRIBUTED result, trained on
    all 132.5M rows. But they cannot do two things the product needs:
      - TreeSHAP explanations per prediction (SHAP does not support MLlib)
      - millisecond single-row latency (a SparkSession costs seconds per call)
    So the SERVING model is CatBoost, trained on a sample, on one machine.
    The sample size is printed and stored so the report can state it plainly
    rather than implying CatBoost saw the whole dataset.

STRATIFIED, NOT RANDOM
    At 0.165% fraud, a plain 2% sample keeps only ~4k fraud rows. We keep ALL
    fraud rows and downsample the legitimate ones, which preserves the signal
    while keeping the file small. The resulting class ratio is recorded so
    CatBoost's scale_pos_weight can be set correctly.

USER-DISJOINT
    The same user %% 5 == 0 holdout rule as Phase 5, so the CatBoost holdout
    contains no user seen in training.

Writes: /fraudlens/dataset/catboost_sample/{train,test}  (CSV in HDFS)
Run:    bash scripts/run_export_sample.sh
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
INPUT = HDFS + "/fraudlens/dataset/preprocessed"
OUT = HDFS + "/fraudlens/dataset/catboost_sample"
META = "/artifacts/catboost_sample_meta.json"

# Legit rows kept per fraud row. 60 keeps the file a few hundred MB while
# leaving CatBoost plenty of negatives to learn the boundary.
NEG_PER_POS = 60

COLUMNS = [
    "amount_abs", "amount_log", "is_refund",
    "hour", "day", "month", "minute",
    "mcc", "error_flag",
    "vpn_flag", "geo_missing", "hw_missing",
    "device_merchant_distance_km",
    "is_online", "is_foreign_or_unknown", "is_night",
    "use_chip", "merchant_state", "device_os", "error_type",
    "user", "is_fraud",
]


def log(m):
    print("[phase7a] " + str(m), flush=True)


def main():
    spark = (SparkSession.builder
             .appName("FraudLens-Phase7a-ExportCatBoostSample")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    log("=" * 62)
    log("PHASE 7a: stratified sample export for CatBoost")
    log("=" * 62)

    df = spark.read.parquet(INPUT).select(*COLUMNS)

    total = df.count()
    frauds = df.filter(F.col("is_fraud") == 1).count()
    log("full dataset: {:,} rows, {:,} fraud".format(total, frauds))

    # Keep every fraud row; downsample legitimate rows.
    keep_frac = min(1.0, (frauds * NEG_PER_POS) / max(total - frauds, 1))
    log("keeping all fraud rows and %.4f%% of legitimate rows"
        % (100.0 * keep_frac))

    fraud_rows = df.filter(F.col("is_fraud") == 1)
    legit_rows = df.filter(F.col("is_fraud") == 0).sample(False, keep_frac, seed=42)
    sample = fraud_rows.unionByName(legit_rows)

    sample = sample.withColumn("is_holdout", (F.col("user") % 5) == 0)
    train = sample.filter(~F.col("is_holdout")).drop("is_holdout", "user")
    test = sample.filter(F.col("is_holdout")).drop("is_holdout", "user")

    n_tr, n_te = train.count(), test.count()
    f_tr = train.filter(F.col("is_fraud") == 1).count()
    f_te = test.filter(F.col("is_fraud") == 1).count()

    log("train: {:,} rows ({:,} fraud)".format(n_tr, f_tr))
    log("test : {:,} rows ({:,} fraud)".format(n_te, f_te))

    for name, frame in (("train", train), ("test", test)):
        (frame.coalesce(1).write.mode("overwrite")
         .option("header", "true").csv(OUT + "/" + name))
        log("written to %s/%s" % (OUT, name))

    meta = {
        "source": INPUT,
        "full_rows": int(total),
        "full_fraud": int(frauds),
        "sampling": "all fraud rows kept; legitimate rows downsampled",
        "negatives_per_positive_target": NEG_PER_POS,
        "legit_keep_fraction": round(keep_frac, 6),
        "train_rows": int(n_tr),
        "train_fraud": int(f_tr),
        "test_rows": int(n_te),
        "test_fraud": int(f_te),
        "scale_pos_weight": round((n_tr - f_tr) / max(f_tr, 1), 3),
        "split_rule": "user % 5 == 0 -> holdout (user-disjoint)",
        "honest_note": ("CatBoost is the SERVING model and is trained on this "
                        "sample, not on all 132.5M rows. The distributed "
                        "results are the Spark MLlib models from Phase 5."),
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    os.makedirs(os.path.dirname(META), exist_ok=True)
    with open(META, "w") as fh:
        json.dump(meta, fh, indent=2)
    log("metadata written to " + META)
    log("scale_pos_weight for CatBoost: %.3f" % meta["scale_pos_weight"])
    log("PHASE 7a COMPLETE in %.1f s" % (time.time() - t0))
    spark.stop()


if __name__ == "__main__":
    main()
