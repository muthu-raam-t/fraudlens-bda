#!/usr/bin/env python3
"""
PHASE 7b -- CatBoost serving model with SHAP explanations.

RUNS ON THE HOST, IN THE VENV -- NOT IN A CONTAINER.
The Spark containers ship Python 3.5, which CatBoost and SHAP do not support
(they need 3.7+). This model is single-machine by design anyway: it exists to
serve one transaction at a time in milliseconds, which is the opposite of a
distributed batch job.

Prerequisites:
    source .venv/bin/activate
    pip install catboost shap scikit-learn

Reads   : data/catboost_sample/{train,test}.csv   (pulled from HDFS)
Writes  : artifacts/catboost_model.cbm
          artifacts/catboost_metrics.json
          artifacts/catboost_feature_importance.json

Run:  bash scripts/run_catboost.sh
"""
import argparse
import glob
import json
import os
import time

import numpy as np
import pandas as pd

CATEGORICAL = ["use_chip", "merchant_state", "device_os", "error_type"]
LABEL = "is_fraud"
ART = "artifacts"


def log(m):
    print("[phase7b] " + str(m), flush=True)


# Downcast on load. float64 -> float32 and int64 -> int32 halves the frame,
# which matters: 10.6M rows x 22 columns at default dtypes is several GB
# BEFORE CatBoost copies it into a Pool.
DTYPES = {c: "float32" for c in [
    "amount_abs", "amount_log", "device_merchant_distance_km"]}
DTYPES.update({c: "int32" for c in [
    "hour", "day", "month", "minute", "mcc", "error_flag", "vpn_flag",
    "geo_missing", "hw_missing", "is_online", "is_foreign_or_unknown",
    "is_night", "is_refund", "is_fraud"]})
DTYPES.update({c: "category" for c in CATEGORICAL})


def load_csv_dir(path, max_rows=None, seed=42):
    """
    Spark writes a directory of part files, even after coalesce(1).

    max_rows caps memory by downsampling, but ALWAYS keeps every fraud row --
    the positives are the scarce resource at a 0.165 % base rate.
    """
    if os.path.isfile(path):
        parts = [path]
    else:
        parts = sorted(glob.glob(os.path.join(path, "part-*.csv")))
    if not parts:
        raise SystemExit("No part-*.csv found in %s -- run "
                         "scripts/run_export_sample.sh first." % path)

    frames = []
    for part in parts:
        frames.append(pd.read_csv(part, dtype=DTYPES, engine="c",
                                  low_memory=False))
    df = pd.concat(frames, ignore_index=True)
    del frames

    if max_rows and len(df) > max_rows:
        pos = df[df[LABEL] == 1]
        neg = df[df[LABEL] == 0]
        keep_neg = max(max_rows - len(pos), 1000)
        if keep_neg < len(neg):
            neg = neg.sample(n=keep_neg, random_state=seed)
        df = pd.concat([pos, neg], ignore_index=True)
        df = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
        log("capped %s to %s rows (all %s fraud rows kept)"
            % (os.path.basename(path), format(len(df), ","),
               format(int(pos.shape[0]), ",")))
        del pos, neg
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", default="data/catboost_sample/train")
    ap.add_argument("--test", default="data/catboost_sample/test")
    ap.add_argument("--iterations", type=int, default=600)
    ap.add_argument("--depth", type=int, default=6)
    ap.add_argument("--lr", type=float, default=0.08)
    ap.add_argument("--shap-rows", type=int, default=2000,
                    help="Rows used to compute global SHAP importance")
    ap.add_argument("--max-train-rows", type=int, default=3_000_000,
                    help="Cap on training rows. All fraud rows are always "
                         "kept; only legitimate rows are downsampled. "
                         "Guards against OOM on a 16 GB host.")
    ap.add_argument("--max-test-rows", type=int, default=1_000_000)
    ap.add_argument("--drop-features", default="",
                    help="Comma-separated features to exclude, e.g. vpn_flag")
    ap.add_argument("--thread-count", type=int, default=4,
                    help="CatBoost worker threads; fewer means less memory")
    args = ap.parse_args()

    try:
        from catboost import CatBoostClassifier, Pool
    except ImportError:
        raise SystemExit("catboost not installed. Run:\n"
                         "  source .venv/bin/activate\n"
                         "  pip install catboost shap scikit-learn")

    from sklearn.metrics import (average_precision_score, confusion_matrix,
                                 roc_auc_score)

    t0 = time.time()
    log("=" * 62)
    log("PHASE 7b: CatBoost serving model + SHAP")
    log("=" * 62)

    train = load_csv_dir(args.train, args.max_train_rows)
    test = load_csv_dir(args.test, args.max_test_rows)
    log("train: {:,} rows   test: {:,} rows".format(len(train), len(test)))

    drop = [c.strip() for c in args.drop_features.split(",") if c.strip()]
    if drop:
        train = train.drop(columns=[c for c in drop if c in train.columns])
        test = test.drop(columns=[c for c in drop if c in test.columns])
        log("dropped features: %s" % ", ".join(drop))

    for frame in (train, test):
        for c in CATEGORICAL:
            if c in frame.columns:
                frame[c] = frame[c].astype(str).fillna("UNKNOWN")
        frame.fillna(0, inplace=True)

    features = [c for c in train.columns if c != LABEL]
    cat_idx = [features.index(c) for c in CATEGORICAL if c in features]

    X_tr, y_tr = train[features], train[LABEL].astype(int)
    X_te, y_te = test[features], test[LABEL].astype(int)

    pos = int(y_tr.sum())
    neg = int(len(y_tr) - pos)
    spw = neg / max(pos, 1)
    log("train fraud: {:,} / {:,}  -> scale_pos_weight {:.2f}".format(
        pos, len(y_tr), spw))

    model = CatBoostClassifier(
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.lr,
        loss_function="Logloss",
        eval_metric="PRAUC",
        scale_pos_weight=spw,
        random_seed=42,
        early_stopping_rounds=60,
        thread_count=args.thread_count,
        verbose=100,
    )

    train_pool = Pool(X_tr, y_tr, cat_features=cat_idx)
    test_pool = Pool(X_te, y_te, cat_features=cat_idx)

    log("training...")
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    train_secs = time.time() - t0

    proba = model.predict_proba(test_pool)[:, 1]
    pred = (proba >= 0.5).astype(int)

    pr_auc = float(average_precision_score(y_te, proba))
    roc_auc = float(roc_auc_score(y_te, proba))
    tn, fp, fn, tp = confusion_matrix(y_te, pred).ravel()
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) else 0.0)
    brier = float(np.mean((proba - y_te.values) ** 2))

    sweep = []
    for th in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        p = (proba >= th).astype(int)
        tp_t = int(((p == 1) & (y_te == 1)).sum())
        fp_t = int(((p == 1) & (y_te == 0)).sum())
        sweep.append({
            "threshold": th,
            "precision": round(tp_t / (tp_t + fp_t), 6) if (tp_t + fp_t) else 0.0,
            "recall": round(tp_t / max(int(y_te.sum()), 1), 6),
            "flagged": tp_t + fp_t,
        })

    log("")
    log("PR-AUC   %.5f" % pr_auc)
    log("ROC-AUC  %.5f" % roc_auc)
    log("precision %.4f  recall %.4f  F1 %.4f" % (precision, recall, f1))
    log("Brier    %.6f  (lower = better calibrated)" % brier)

    os.makedirs(ART, exist_ok=True)
    model.save_model(os.path.join(ART, "catboost_model.cbm"))
    log("model saved to artifacts/catboost_model.cbm")

    # ---- SHAP: global importance, and proof per-row explanations work -----
    log("")
    log("computing SHAP values on %d rows" % args.shap_rows)
    shap_pool = Pool(X_te.head(args.shap_rows), y_te.head(args.shap_rows),
                     cat_features=cat_idx)
    # CatBoost computes exact TreeSHAP natively -- no shap package needed here.
    shap_vals = model.get_feature_importance(shap_pool, type="ShapValues")
    contribs = shap_vals[:, :-1]          # last column is the expected value
    mean_abs = np.abs(contribs).mean(axis=0)
    ranked = sorted(zip(features, mean_abs), key=lambda kv: -kv[1])

    log("top features by mean |SHAP|:")
    for f, v in ranked[:12]:
        log("    %-32s %.5f" % (f, v))

    top_names = [f for f, _ in ranked[:3]]
    if "vpn_flag" in top_names:
        log("")
        log("WARNING: vpn_flag is in the top 3 features. That column is")
        log("         synthetic, so this partly measures the data generator.")
        log("         Say so in the report, or drop the feature and retrain.")

    with open(os.path.join(ART, "catboost_feature_importance.json"), "w") as fh:
        json.dump({
            "method": "TreeSHAP (mean absolute contribution)",
            "rows_used": int(args.shap_rows),
            "features": [{"feature": f, "mean_abs_shap": round(float(v), 6)}
                         for f, v in ranked],
        }, fh, indent=2)

    # One worked example, to show the API the FastAPI layer will use.
    example_idx = int(np.argmax(proba[:args.shap_rows]))
    example = sorted(zip(features, contribs[example_idx]),
                     key=lambda kv: -abs(kv[1]))[:5]
    log("")
    log("example explanation (highest-risk row in the sample):")
    log("    P(fraud) = %.4f" % proba[example_idx])
    for f, v in example:
        log("    %-32s %+0.5f" % (f, v))

    metrics = {
        "model": "catboost",
        "role": "serving model (single-transaction latency + TreeSHAP)",
        "trained_on": "stratified sample, NOT all 132.5M rows",
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_fraud": pos,
        "test_fraud": int(y_te.sum()),
        "scale_pos_weight": round(spw, 3),
        "iterations_requested": args.iterations,
        "iterations_used": int(model.tree_count_),
        "depth": args.depth,
        "pr_auc": round(pr_auc, 6),
        "roc_auc": round(roc_auc, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "brier_score": round(brier, 6),
        "true_positives": int(tp), "false_positives": int(fp),
        "false_negatives": int(fn), "true_negatives": int(tn),
        "threshold_sweep": sweep,
        "top_features": [{"feature": f, "mean_abs_shap": round(float(v), 6)}
                         for f, v in ranked[:15]],
        "train_seconds": round(train_secs, 1),
    }
    with open(os.path.join(ART, "catboost_metrics.json"), "w") as fh:
        json.dump(metrics, fh, indent=2)

    log("")
    log("metrics -> artifacts/catboost_metrics.json")
    log("PHASE 7b COMPLETE in %.1f s" % (time.time() - t0))


if __name__ == "__main__":
    main()
