#!/usr/bin/env python3
"""
PHASE 9 -- FastAPI inference service.

WHY CATBOOST AND NOT THE SPARK GBT
    The Spark MLlib GBT is the DISTRIBUTED result (PR-AUC 0.2196 on the full
    132.5M-row holdout). It cannot serve this endpoint, for two reasons:
      1. SHAP has no Spark MLlib support, so per-transaction explanations are
         impossible.
      2. Spinning up a SparkSession to score ONE row costs seconds.
    CatBoost gives millisecond latency and exact TreeSHAP. It is the serving
    layer, trained on a stratified sample -- stated plainly, not implied away.

ENDPOINTS
    GET  /health                 service + model status
    POST /predict                verdict, P(fraud), top-N SHAP contributions
    GET  /aggregates             list of available precomputed aggregates
    GET  /aggregates/{name}      one aggregate (Phase 8 output)
    GET  /metrics                model comparison + benchmark numbers
    GET  /stream/recent          latest Structured Streaming verdicts

Run:  bash scripts/run_api.sh
"""
import glob
import json
import os
from typing import List, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ART = os.path.join(ROOT, "artifacts")
MODEL_PATH = os.path.join(ART, "catboost_model.cbm")
AGG_DIR = os.path.join(ART, "aggregates")
STREAM_DIR = os.path.join(ART, "stream_verdicts")

CATEGORICAL = ["use_chip", "merchant_state", "device_os", "error_type"]

app = FastAPI(
    title="FraudLens Inference API",
    description="Explainable credit card fraud scoring (CatBoost + TreeSHAP)",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

STATE = {"model": None, "features": None, "cat_idx": None, "error": None,
         "base_rate": 0.001650, "base_odds": None}


def _read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:                                          # noqa: BLE001
        return None


@app.on_event("startup")
def load_model():
    """Load ONCE at startup, not per request."""
    try:
        from catboost import CatBoostClassifier
        if not os.path.exists(MODEL_PATH):
            STATE["error"] = "model file not found: %s" % MODEL_PATH
            return
        m = CatBoostClassifier()
        m.load_model(MODEL_PATH)
        STATE["model"] = m
        STATE["features"] = list(m.feature_names_)
        STATE["cat_idx"] = list(m.get_cat_feature_indices())
        print("[api] model loaded: %d features, %d categorical"
              % (len(STATE["features"]), len(STATE["cat_idx"])))

        # ------------------------------------------------------------------
        # CALIBRATION
        #
        # The model was trained on a stratified sample (legitimate rows
        # downsampled) AND with scale_pos_weight set to neg/pos. Together
        # those make the training set effectively 50/50, so raw scores are
        # inflated by roughly 1/base_rate -- about 600x in odds terms. An
        # ordinary grocery transaction comes back at 30-60%, which is
        # meaningless as a probability.
        #
        # Standard correction: shift the prior back.
        #     odds_true = odds_model x base_odds
        # Ranking is unchanged; only the scale is corrected, so the number
        # can now be read as an actual probability of fraud.
        # ------------------------------------------------------------------
        summary = _read_json(os.path.join(AGG_DIR, "dataset_summary.json"))
        if summary and summary.get("fraud_rate_pct"):
            STATE["base_rate"] = float(summary["fraud_rate_pct"]) / 100.0
        br = STATE["base_rate"]
        STATE["base_odds"] = br / (1.0 - br)
        print("[api] calibrating to a base fraud rate of %.4f%% "
              "(odds correction 1:%.0f)" % (br * 100, 1.0 / STATE["base_odds"]))
    except Exception as exc:                                   # noqa: BLE001
        STATE["error"] = str(exc)
        print("[api] model load FAILED: %s" % exc)


class Transaction(BaseModel):
    """A transaction to score. Only `amount` is required."""
    amount: float = Field(..., description="Transaction amount")
    hour: int = Field(12, ge=0, le=23)
    day: int = Field(15, ge=1, le=31)
    month: int = Field(6, ge=1, le=12)
    minute: int = Field(0, ge=0, le=59)
    mcc: int = Field(5411, description="ISO 18245 merchant category code")
    merchant_state: str = "CA"
    use_chip: str = "Chip Transaction"
    device_os: str = "Android"
    error_type: str = "NONE"
    vpn_flag: int = Field(0, ge=0, le=1)
    is_online: int = Field(0, ge=0, le=1)
    device_merchant_distance_km: float = 5.0
    top_n: int = Field(5, ge=1, le=20, description="How many SHAP drivers")
    threshold_lift: float = Field(
        30.0, ge=1.0, le=500.0,
        description=("Flag when the calibrated probability exceeds this "
                     "multiple of the base fraud rate. 30x on a 0.165% base "
                     "rate means flagging at roughly 5% probability."))


class Contribution(BaseModel):
    feature: str
    value: str
    shap: float
    direction: str


class Prediction(BaseModel):
    verdict: str
    fraud_probability: float        # calibrated against the real base rate
    raw_model_score: float          # uncalibrated, for transparency
    lift_vs_base_rate: float        # how many times the average transaction
    base_rate: float
    threshold: float
    threshold_lift: float
    risk_band: str
    contributions: List[Contribution]
    base_value: float
    model: str
    note: str


def build_row(t: Transaction) -> pd.DataFrame:
    """Derive the exact feature set the model was trained on."""
    amount_abs = abs(t.amount)
    row = {
        "amount_abs": amount_abs,
        "amount_log": float(np.log1p(amount_abs)),
        "is_refund": 1 if t.amount < 0 else 0,
        "hour": t.hour,
        "day": t.day,
        "month": t.month,
        "minute": t.minute,
        "mcc": t.mcc,
        "error_flag": 0 if t.error_type.upper() == "NONE" else 1,
        "vpn_flag": t.vpn_flag,
        "geo_missing": 0,
        "hw_missing": 0,
        "device_merchant_distance_km": t.device_merchant_distance_km,
        "is_online": t.is_online,
        "is_foreign_or_unknown": 0 if len(t.merchant_state.strip()) == 2 else 1,
        "is_night": 1 if (t.hour < 6 or t.hour >= 22) else 0,
        "use_chip": t.use_chip,
        "merchant_state": t.merchant_state.upper(),
        "device_os": t.device_os,
        "error_type": t.error_type,
    }
    feats = STATE["features"]
    # Fill anything the model expects but the request did not supply.
    for f in feats:
        if f not in row:
            row[f] = "UNKNOWN" if f in CATEGORICAL else 0
    return pd.DataFrame([{f: row[f] for f in feats}])


def calibrate(p_model: float) -> float:
    """Raw weighted score -> probability against the real base rate."""
    p_model = min(max(p_model, 1e-9), 1 - 1e-9)
    odds_model = p_model / (1.0 - p_model)
    odds_true = odds_model * STATE["base_odds"]
    return odds_true / (1.0 + odds_true)


def risk_band(p: float, base: float) -> str:
    """Bands are multiples of the base rate, not absolute probabilities.

    At a 0.165% base rate an absolute 50% threshold would never fire, so
    severity is expressed as lift: how many times more likely than an
    average transaction.
    """
    lift = p / base if base else 0.0
    if lift >= 100:
        return "critical"
    if lift >= 30:
        return "high"
    if lift >= 10:
        return "elevated"
    if lift >= 3:
        return "low"
    return "minimal"


def _old_risk_band(p: float) -> str:
    if p >= 0.90:
        return "critical"
    if p >= 0.70:
        return "high"
    if p >= 0.40:
        return "elevated"
    if p >= 0.15:
        return "low"
    return "minimal"


@app.get("/health")
def health():
    return {
        "status": "ok" if STATE["model"] is not None else "degraded",
        "model_loaded": STATE["model"] is not None,
        "model_path": MODEL_PATH,
        "features": len(STATE["features"]) if STATE["features"] else 0,
        "aggregates_available": os.path.isdir(AGG_DIR),
        "base_rate": STATE["base_rate"],
        "calibrated": STATE["base_odds"] is not None,
        "error": STATE["error"],
    }


@app.post("/predict", response_model=Prediction)
def predict(t: Transaction):
    if STATE["model"] is None:
        raise HTTPException(503, "model not loaded: %s" % STATE["error"])
    from catboost import Pool

    df = build_row(t)
    for c in CATEGORICAL:
        if c in df.columns:
            df[c] = df[c].astype(str)

    pool = Pool(df, cat_features=STATE["cat_idx"])
    raw = float(STATE["model"].predict_proba(pool)[0, 1])
    prob = calibrate(raw)
    base = STATE["base_rate"]
    lift = prob / base if base else 0.0
    cutoff = t.threshold_lift * base

    # Exact TreeSHAP: the last column is the expected (base) value.
    shap = STATE["model"].get_feature_importance(pool, type="ShapValues")[0]
    shap_base = float(shap[-1])
    contribs = list(zip(STATE["features"], shap[:-1]))
    contribs.sort(key=lambda kv: -abs(kv[1]))

    top = []
    for name, val in contribs[: t.top_n]:
        top.append(Contribution(
            feature=name,
            value=str(df.iloc[0][name]),
            shap=round(float(val), 6),
            direction="increases risk" if val > 0 else "decreases risk",
        ))

    return Prediction(
        verdict="REVIEW" if prob >= cutoff else "CLEAR",
        fraud_probability=round(prob, 8),
        raw_model_score=round(raw, 6),
        lift_vs_base_rate=round(lift, 2),
        base_rate=round(base, 6),
        threshold=round(cutoff, 8),
        threshold_lift=t.threshold_lift,
        risk_band=risk_band(prob, base),
        contributions=top,
        base_value=round(shap_base, 6),
        model="catboost (serving model, trained on a stratified sample)",
        note=("Scores are calibrated back to the real %.4f%% base fraud rate. "
              "The model was trained on downsampled negatives with "
              "scale_pos_weight, so its raw output (%.3f here) is inflated by "
              "roughly 1:%.0f in odds and is not a probability. The "
              "distributed result is the Spark MLlib GBT trained on all "
              "132,500,000 rows; CatBoost serves this endpoint because SHAP "
              "does not support Spark MLlib."
              % (base * 100, raw, 1.0 / STATE["base_odds"])),
    )


@app.get("/aggregates")
def list_aggregates():
    if not os.path.isdir(AGG_DIR):
        raise HTTPException(404, "run scripts/run_aggregates.sh first")
    files = sorted(os.path.basename(p) for p in glob.glob(os.path.join(AGG_DIR, "*.json")))
    return {"available": [f[:-5] for f in files], "directory": AGG_DIR}


@app.get("/aggregates/{name}")
def get_aggregate(name: str):
    safe = os.path.basename(name).replace("..", "")
    path = os.path.join(AGG_DIR, safe + ".json")
    if not os.path.exists(path):
        raise HTTPException(404, "no aggregate named '%s'" % safe)
    with open(path) as fh:
        return json.load(fh)


@app.get("/metrics")
def metrics():
    """Everything the dashboard needs for the comparison views."""
    out = {}
    for key, fname in [
        ("spark_models", "metrics.json"),
        ("catboost", "catboost_metrics.json"),
        ("catboost_shap", "catboost_feature_importance.json"),
        ("tuning", "tuning_benchmark.json"),
        ("data_quality", "data_quality_profile.json"),
        ("sample_meta", "catboost_sample_meta.json"),
    ]:
        path = os.path.join(ART, fname)
        if os.path.exists(path):
            with open(path) as fh:
                out[key] = json.load(fh)
    out["engine_benchmark"] = {
        "job": "fraud rate by merchant state over 28.1 GB / 132,500,000 rows",
        "mapreduce_seconds": 613,
        "spark_seconds": 65.3,
        "speedup": 9.4,
        "mapreduce_disk_spill_bytes": 2087890445,
        "note": ("both engines read the SAME raw CSV: MapReduce cannot read "
                 "Parquet, so using it would have measured the file format "
                 "rather than the engine"),
    }
    if not out:
        raise HTTPException(404, "no metrics found -- run the phases first")
    return out


@app.get("/stream/recent")
def stream_recent(limit: int = 25):
    """Latest verdicts written by the Phase 11 streaming job."""
    if not os.path.isdir(STREAM_DIR):
        return {"available": False, "verdicts": [],
                "hint": "run scripts/run_streaming.sh"}
    files = sorted(glob.glob(os.path.join(STREAM_DIR, "*.json")),
                   key=os.path.getmtime, reverse=True)
    rows = []
    for f in files[:10]:
        try:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        except Exception:                                      # noqa: BLE001
            continue
        if len(rows) >= limit:
            break
    return {"available": True, "count": len(rows[:limit]),
            "verdicts": rows[:limit]}


@app.get("/")
def root():
    return {
        "service": "FraudLens Inference API",
        "docs": "/docs",
        "endpoints": ["/health", "/predict", "/aggregates", "/metrics",
                      "/stream/recent"],
    }
