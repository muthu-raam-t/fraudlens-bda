#!/usr/bin/env python3
"""
PHASE 5 -- Distributed model training with Spark MLlib on YARN.

Reads   : /fraudlens/dataset/preprocessed   (Parquet, 132.5M rows)
Writes  : /fraudlens/models/<name>          (saved PipelineModel)
          /artifacts/metrics.json           (all model metrics)

WHAT THIS DOES, IN ORDER
  1. Loads the preprocessed Parquet.
  2. Feature engineering, distributed:
       - per-user amount statistics (mean/std/count) computed with groupBy on
         the full 132.5M rows, then BROADCAST back (only ~2k users, so the
         table is tiny). This gives "how unusual is this amount FOR THIS USER"
         without an expensive per-row window sort.
       - MCC frequency, likewise broadcast.
     No column derived here uses is_fraud, so there is no target leakage.
  3. Splits TRAIN / HELD-OUT by user id (user %% 5 == 0 -> holdout).
     This is the same rule the generator used, so no user appears on both
     sides. A random split would leak, because rows were sampled with
     replacement during scaling.
  4. Trains LogisticRegression, RandomForest and GBT with cost-sensitive
     class weights (weightCol) for the 0.165%% fraud rate.
  5. Evaluates on PR-AUC (the right metric for extreme imbalance), ROC-AUC,
     precision, recall, F1 and a threshold sweep. NOT accuracy: predicting
     "never fraud" scores 99.835%%.
  6. Saves each model and appends metrics after EACH model, so a failure on
     the last model does not lose the earlier results.

Run:  bash scripts/run_train.sh
      bash scripts/run_train.sh --sample-frac 0.25    (faster, for iteration)
"""
import argparse
import json
import os
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
INPUT = HDFS + "/fraudlens/dataset/preprocessed"
MODEL_DIR = HDFS + "/fraudlens/models"
METRICS_PATH = "/artifacts/metrics.json"

CATEGORICAL = ["use_chip", "merchant_state", "device_os", "error_type"]

NUMERIC = [
    "amount_abs", "amount_log", "is_refund",
    "hour", "day", "minute", "month",
    "mcc", "error_flag",
    "vpn_flag", "geo_missing", "hw_missing",
    "device_merchant_distance_km",
    "is_online", "is_foreign_or_unknown", "is_night",
    # engineered below
    "user_amount_mean", "user_amount_std", "user_txn_count",
    "amount_vs_user_mean", "amount_zscore",
    "mcc_frequency",
    # added in the Phase 4 rework
    "day_of_week", "is_weekend", "day_of_year",
    "amount_capped", "is_outlier",
    "imputed_city", "imputed_state", "imputed_zip",
]


def log(msg):
    print("[phase5] " + str(msg), flush=True)


def save_metrics(all_metrics):
    """Written after every model, so partial progress survives a failure."""
    os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
    with open(METRICS_PATH, "w") as fh:
        json.dump(all_metrics, fh, indent=2)
    log("metrics written to " + METRICS_PATH)


def evaluate(preds, label_col="is_fraud"):
    """PR-AUC, ROC-AUC, and a confusion matrix at threshold 0.5."""
    pr = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction",
        metricName="areaUnderPR").evaluate(preds)
    roc = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(preds)

    counts = preds.select(
        F.sum(F.when((F.col(label_col) == 1) & (F.col("prediction") == 1), 1)
              .otherwise(0)).alias("tp"),
        F.sum(F.when((F.col(label_col) == 0) & (F.col("prediction") == 1), 1)
              .otherwise(0)).alias("fp"),
        F.sum(F.when((F.col(label_col) == 1) & (F.col("prediction") == 0), 1)
              .otherwise(0)).alias("fn"),
        F.sum(F.when((F.col(label_col) == 0) & (F.col("prediction") == 0), 1)
              .otherwise(0)).alias("tn"),
    ).collect()[0]

    tp, fp, fn, tn = (int(counts["tp"]), int(counts["fp"]),
                      int(counts["fn"]), int(counts["tn"]))
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) else 0.0)
    accuracy = (tp + tn) / (tp + fp + fn + tn)

    return {
        "pr_auc": round(pr, 6),
        "roc_auc": round(roc, 6),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": tn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "accuracy_do_not_quote_this": round(accuracy, 6),
    }


def threshold_sweep(preds, label_col="is_fraud"):
    """Precision/recall at several cutoffs -- how the decision point moves."""
    sweep = []
    scored = preds.select(
        F.col(label_col).alias("y"),
        vector_second_element(F.col("probability")).alias("p"),
    ).cache()
    total_pos = scored.filter(F.col("y") == 1).count()
    for th in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        row = scored.select(
            F.sum(F.when((F.col("y") == 1) & (F.col("p") >= th), 1)
                  .otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("y") == 0) & (F.col("p") >= th), 1)
                  .otherwise(0)).alias("fp"),
        ).collect()[0]
        tp, fp = int(row["tp"]), int(row["fp"])
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / total_pos if total_pos else 0.0
        sweep.append({
            "threshold": th,
            "precision": round(prec, 6),
            "recall": round(rec, 6),
            "flagged": tp + fp,
        })
    scored.unpersist()
    return sweep


def vector_second_element(col):
    """Extract P(fraud) from an MLlib probability vector."""
    from pyspark.ml.functions import vector_to_array
    return vector_to_array(col)[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-frac", type=float, default=1.0,
                    help="Fraction of rows to train on (1.0 = all 132.5M)")
    ap.add_argument("--rf-trees", type=int, default=30)
    ap.add_argument("--gbt-iter", type=int, default=15)
    ap.add_argument("--max-depth", type=int, default=6)
    ap.add_argument("--max-bins", type=int, default=256,
                    help="Must exceed the cardinality of indexed categoricals")
    ap.add_argument("--skip", default="",
                    help="Comma-separated models to skip: lr,rf,gbt")
    args = ap.parse_args()

    skip = {s.strip().lower() for s in args.skip.split(",") if s.strip()}

    spark = (SparkSession.builder
             .appName("FraudLens-Phase5-MLlibTraining")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    t_start = time.time()
    log("=" * 66)
    log("PHASE 5: Spark MLlib training")
    log("=" * 66)

    df = spark.read.parquet(INPUT)
    if args.sample_frac < 1.0:
        df = df.sample(withReplacement=False, fraction=args.sample_frac, seed=42)
        log("SAMPLED to fraction %s -- report this honestly" % args.sample_frac)

    # ---------------- feature engineering (distributed) -------------------
    log("computing per-user amount statistics over the full dataset")
    user_stats = (df.groupBy("user")
                  .agg(F.avg("amount_abs").alias("user_amount_mean"),
                       F.stddev("amount_abs").alias("user_amount_std"),
                       F.count("*").alias("user_txn_count")))
    user_stats = user_stats.fillna({"user_amount_std": 0.0})
    n_users = user_stats.count()
    log("distinct users: %d (small enough to broadcast)" % n_users)

    log("computing MCC frequencies")
    mcc_freq = (df.groupBy("mcc")
                .agg(F.count("*").alias("mcc_frequency")))
    n_mcc = mcc_freq.count()
    log("distinct MCC codes: %d" % n_mcc)

    feat = (df
            .join(F.broadcast(user_stats), on="user", how="left")
            .join(F.broadcast(mcc_freq), on="mcc", how="left")
            .withColumn("amount_vs_user_mean",
                        F.col("amount_abs") / (F.col("user_amount_mean") + F.lit(1.0)))
            .withColumn("amount_zscore",
                        (F.col("amount_abs") - F.col("user_amount_mean")) /
                        (F.col("user_amount_std") + F.lit(1.0)))
            .fillna(0.0, subset=["user_amount_mean", "user_amount_std",
                                 "user_txn_count", "amount_vs_user_mean",
                                 "amount_zscore", "mcc_frequency"]))

    # ---------------- user-disjoint split ---------------------------------
    feat = feat.withColumn("is_holdout", (F.col("user") % 5) == 0)
    train_df = feat.filter(~F.col("is_holdout"))
    test_df = feat.filter(F.col("is_holdout"))

    # Cache: every model below re-reads these. This is the caching step the
    # performance-tuning requirement asks for.
    train_df = train_df.cache()
    test_df = test_df.cache()
    n_train = train_df.count()
    n_test = test_df.count()
    n_fraud_train = train_df.filter(F.col("is_fraud") == 1).count()
    n_fraud_test = test_df.filter(F.col("is_fraud") == 1).count()

    log("train rows   : {:,}  (fraud {:,})".format(n_train, n_fraud_train))
    log("holdout rows : {:,}  (fraud {:,})".format(n_test, n_fraud_test))
    log("train fraud rate: %.4f%%" % (100.0 * n_fraud_train / max(n_train, 1)))

    # ---------------- class weights ---------------------------------------
    # Inverse frequency: fraud rows get weight = (negatives / positives).
    ratio = (n_train - n_fraud_train) / max(n_fraud_train, 1)
    log("class weight for fraud rows: %.1f" % ratio)
    train_w = train_df.withColumn(
        "class_weight",
        F.when(F.col("is_fraud") == 1, F.lit(float(ratio))).otherwise(F.lit(1.0)))

    # ---------------- feature pipelines -----------------------------------
    # TWO pipelines on purpose:
    #
    #   TREES (RF, GBT) -- StringIndexer only. Trees split on thresholds, so an
    #       integer code is harmless and keeps the vector small and dense.
    #
    #   LINEAR (LR) -- StringIndexer -> OneHotEncoder -> StandardScaler.
    #       Without one-hot, LR reads merchant_state as a NUMBER, implying
    #       CA < TX < NY, an order that does not exist. Without scaling,
    #       amount (0-30,000) swamps binary flags (0-1) in the gradient.
    #       Both were handicapping the baseline; fixing them makes the
    #       "why do we need an ensemble" comparison honest.
    indexers = [
        StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
        for c in CATEGORICAL
    ]

    tree_assembler = VectorAssembler(
        inputCols=NUMERIC + [c + "_idx" for c in CATEGORICAL],
        outputCol="features",
        handleInvalid="keep",
    )

    encoder = OneHotEncoder(
        inputCols=[c + "_idx" for c in CATEGORICAL],
        outputCols=[c + "_ohe" for c in CATEGORICAL],
        handleInvalid="keep",
        dropLast=True,
    )
    lr_assembler = VectorAssembler(
        inputCols=NUMERIC + [c + "_ohe" for c in CATEGORICAL],
        outputCol="features_raw",
        handleInvalid="keep",
    )
    # withMean=False keeps the one-hot block sparse; centring it would
    # densify a 200+ column vector across 132.5M rows.
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withStd=True, withMean=False,
    )
    lr_prep = [encoder, lr_assembler, scaler]

    all_metrics = {
        "dataset": {
            "input": INPUT,
            "sample_frac": args.sample_frac,
            "train_rows": n_train,
            "test_rows": n_test,
            "train_fraud": n_fraud_train,
            "test_fraud": n_fraud_test,
            "split_rule": "user %% 5 == 0 -> holdout (user-disjoint)",
            "distinct_users": n_users,
            "class_weight_fraud": round(float(ratio), 2),
        },
        "models": {},
        "feature_count": len(NUMERIC) + len(CATEGORICAL),
    }
    save_metrics(all_metrics)

    # (name, estimator, extra_prep_stages)
    specs = []
    if "lr" not in skip:
        specs.append(("logistic_regression", LogisticRegression(
            featuresCol="features", labelCol="is_fraud",
            weightCol="class_weight", maxIter=20, regParam=0.01,
            elasticNetParam=0.0), lr_prep))
    if "rf" not in skip:
        specs.append(("random_forest", RandomForestClassifier(
            featuresCol="features", labelCol="is_fraud",
            weightCol="class_weight", numTrees=args.rf_trees,
            maxDepth=args.max_depth, maxBins=args.max_bins, seed=42),
            [tree_assembler]))
    if "gbt" not in skip:
        specs.append(("gbt", GBTClassifier(
            featuresCol="features", labelCol="is_fraud",
            weightCol="class_weight", maxIter=args.gbt_iter,
            maxDepth=args.max_depth, maxBins=args.max_bins, seed=42),
            [tree_assembler]))

    for name, estimator, prep in specs:
        log("")
        log("-" * 66)
        log("TRAINING: %s" % name)
        log("-" * 66)
        t0 = time.time()
        pipeline = Pipeline(stages=indexers + prep + [estimator])
        try:
            model = pipeline.fit(train_w)
        except Exception as exc:                       # noqa: BLE001
            log("FAILED to train %s: %s" % (name, exc))
            all_metrics["models"][name] = {"error": str(exc)[:500]}
            save_metrics(all_metrics)
            continue

        train_secs = time.time() - t0
        log("trained in %.1f s" % train_secs)

        preds = model.transform(test_df)
        metrics = evaluate(preds)
        metrics["train_seconds"] = round(train_secs, 1)

        try:
            metrics["threshold_sweep"] = threshold_sweep(preds)
        except Exception as exc:                       # noqa: BLE001
            log("threshold sweep unavailable: %s" % exc)

        stage = model.stages[-1]
        if hasattr(stage, "featureImportances"):
            imps = list(stage.featureImportances.toArray())
            names = NUMERIC + [c + "_idx" for c in CATEGORICAL]
            if len(imps) != len(names):
                # LR path uses one-hot, so the vector is wider than the
                # human-readable name list; fall back to positional names.
                names = ["f%d" % i for i in range(len(imps))]
            top = sorted(zip(names, imps), key=lambda kv: -kv[1])[:15]
            metrics["top_features"] = [
                {"feature": f, "importance": round(float(v), 6)} for f, v in top
            ]
            log("top features:")
            for f, v in top[:10]:
                log("    %-32s %.5f" % (f, v))

        all_metrics["models"][name] = metrics
        save_metrics(all_metrics)

        log("PR-AUC %.5f | ROC-AUC %.5f | precision %.4f | recall %.4f | F1 %.4f"
            % (metrics["pr_auc"], metrics["roc_auc"],
               metrics["precision"], metrics["recall"], metrics["f1"]))

        path = MODEL_DIR + "/" + name
        model.write().overwrite().save(path)
        log("model saved to %s" % path)

    # ---------------- summary ---------------------------------------------
    trained = {k: v for k, v in all_metrics["models"].items() if "pr_auc" in v}
    if trained:
        best = max(trained.items(), key=lambda kv: kv[1]["pr_auc"])
        all_metrics["best_model"] = {"name": best[0], "pr_auc": best[1]["pr_auc"]}
        log("")
        log("=" * 66)
        log("RESULTS (ranked by PR-AUC -- the right metric for 0.165% fraud)")
        log("=" * 66)
        log("%-24s %10s %10s %10s %10s" %
            ("MODEL", "PR-AUC", "ROC-AUC", "RECALL", "F1"))
        for k, v in sorted(trained.items(), key=lambda kv: -kv[1]["pr_auc"]):
            log("%-24s %10.5f %10.5f %10.4f %10.4f" %
                (k, v["pr_auc"], v["roc_auc"], v["recall"], v["f1"]))
        log("")
        log("BEST: %s (PR-AUC %.5f)" % (best[0], best[1]["pr_auc"]))

    all_metrics["total_seconds"] = round(time.time() - t_start, 1)
    save_metrics(all_metrics)

    train_df.unpersist()
    test_df.unpersist()
    log("PHASE 5 COMPLETE in %.1f min" % ((time.time() - t_start) / 60.0))
    spark.stop()


if __name__ == "__main__":
    main()
