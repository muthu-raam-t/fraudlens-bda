#!/usr/bin/env python3
"""
PHASE 12c -- Streamlit live operations dashboard.

Tails the verdict files the Spark streaming job writes after every
micro-batch and refreshes itself, so transactions appear on screen as they are
scored. This is the visible half of the streaming phase: the Spark job already
produced these numbers, but a terminal full of log lines is not a demonstration.

Reads   : artifacts/stream_verdicts/*.json   (written by foreachBatch)
Run     : bash scripts/run_streamlit.sh
"""
import glob
import json
import os
import time
from collections import Counter

import pandas as pd
import streamlit as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERDICT_DIR = os.path.join(ROOT, "artifacts", "stream_verdicts")
AGG_DIR = os.path.join(ROOT, "artifacts", "aggregates")
REFRESH_SECONDS = 3

st.set_page_config(
    page_title="FraudLens — live stream",
    page_icon="▣",
    layout="wide",
)

# --------------------------------------------------------------------------
# Styling. The risk ramp (green -> amber -> red) is reserved for risk only, so
# severity is readable without reading text. Figures use a mono face because
# comparing amounts and probabilities depends on aligned digits.
# --------------------------------------------------------------------------
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
  .masthead { background:#10141c; color:#fff; padding:16px 22px; border-radius:3px;
              margin-bottom:18px; display:flex; align-items:baseline; gap:16px; }
  .masthead h1 { font-size:19px; font-weight:600; margin:0; }
  .masthead .sub { color:#9aa6b8; font-size:13px; font-family:'IBM Plex Mono',monospace; }
  .masthead .live { margin-left:auto; color:#4ade80; font-size:12.5px;
                    font-family:'IBM Plex Mono',monospace; }
  div[data-testid="stMetricValue"] { font-family:'IBM Plex Mono',monospace;
                                     font-size:30px; letter-spacing:-0.02em; }
  div[data-testid="stMetricLabel"] { font-size:12.5px; color:#697586; }
  .stDataFrame { font-family:'IBM Plex Mono', monospace; }
  .note { background:#f2f4f8; border-left:3px solid #1f5fa8; padding:11px 15px;
          font-size:13px; color:#364152; border-radius:2px; margin-top:14px; }
  .warn { background:#fdf6e3; border-left:3px solid #b07d18; padding:11px 15px;
          font-size:13px; color:#6b5310; border-radius:2px; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=2)
def load_verdicts():
    """Read every batch file the streaming job has written so far."""
    if not os.path.isdir(VERDICT_DIR):
        return pd.DataFrame(), 0
    files = sorted(glob.glob(os.path.join(VERDICT_DIR, "*.json")))
    rows = []
    for f in files:
        try:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        except (OSError, json.JSONDecodeError):
            continue
    return (pd.DataFrame(rows) if rows else pd.DataFrame()), len(files)


@st.cache_data(ttl=60)
def load_summary():
    path = os.path.join(AGG_DIR, "dataset_summary.json")
    if os.path.exists(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            pass
    return {}


def risk_colour(p):
    if p >= 0.90:
        return "#b02525"
    if p >= 0.70:
        return "#c2601c"
    if p >= 0.40:
        return "#b07d18"
    if p >= 0.15:
        return "#4b8b3b"
    return "#1d7a4c"


# --------------------------------------------------------------------------
df, n_batches = load_verdicts()
summary = load_summary()
base_rate = summary.get("fraud_rate_pct", 0.165) / 100.0

st.markdown(
    '<div class="masthead"><h1>FraudLens</h1>'
    '<span class="sub">live transaction stream</span>'
    '<span class="live">● streaming · refresh %ds</span></div>' % REFRESH_SECONDS,
    unsafe_allow_html=True,
)

if df.empty:
    st.markdown(
        '<div class="warn"><b>No verdicts yet.</b> Start the pipeline:<br><br>'
        '<code>bash scripts/run_kafka_stream.sh</code> &nbsp;(terminal 1)<br>'
        '<code>bash scripts/run_producer.sh</code> &nbsp;(terminal 2)<br><br>'
        'This page refreshes every %d seconds and will fill in as batches '
        'are scored.</div>' % REFRESH_SECONDS,
        unsafe_allow_html=True,
    )
    time.sleep(REFRESH_SECONDS)
    st.rerun()

# --------------------------------------------------------------------------
total = len(df)
flagged = int(df["predicted_fraud"].sum()) if "predicted_fraud" in df else 0
actual = int(df["actual_fraud"].sum()) if "actual_fraud" in df else 0
flag_rate = 100.0 * flagged / total if total else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions scored", "{:,}".format(total))
c2.metric("Flagged for review", "{:,}".format(flagged), "{:.1f}%".format(flag_rate))
c3.metric("Labelled fraud", "{:,}".format(actual))
c4.metric("Micro-batches", "{:,}".format(n_batches))
if "fraud_probability" in df:
    c5.metric("Peak risk score", "{:.4f}".format(df["fraud_probability"].max()))

st.markdown("")

# --------------------------------------------------------------------------
left, right = st.columns([3, 2])

with left:
    st.markdown("##### Risk distribution across scored transactions")
    if "fraud_probability" in df:
        bins = pd.cut(
            df["fraud_probability"],
            bins=[-0.001, 0.15, 0.40, 0.70, 0.90, 1.001],
            labels=["minimal", "low", "elevated", "high", "critical"],
        )
        counts = bins.value_counts().reindex(
            ["minimal", "low", "elevated", "high", "critical"]).fillna(0)
        st.bar_chart(counts, height=240)

with right:
    st.markdown("##### Throughput by micro-batch")
    if "batch_id" in df:
        per_batch = df.groupby("batch_id").size().rename("events")
        st.line_chart(per_batch, height=240)
    else:
        st.info("Batch identifiers appear once the Kafka job runs.")

# --------------------------------------------------------------------------
st.markdown("##### Highest-risk transactions scored so far")

show = df.copy()
if "fraud_probability" in show:
    show = show.sort_values("fraud_probability", ascending=False)

cols = [c for c in ["event_id", "user", "amount", "merchant_state", "mcc",
                    "hour", "vpn_flag", "fraud_probability", "predicted_fraud",
                    "actual_fraud", "scored_at"] if c in show.columns]

st.dataframe(
    show[cols].head(25),
    use_container_width=True,
    hide_index=True,
    column_config={
        "fraud_probability": st.column_config.ProgressColumn(
            "risk", min_value=0.0, max_value=1.0, format="%.4f"),
        "amount": st.column_config.NumberColumn("amount", format="%.2f"),
    },
)

# --------------------------------------------------------------------------
if "merchant_state" in df:
    st.markdown("##### Flag rate by merchant location, this stream")
    by_state = (df.groupby("merchant_state")
                  .agg(events=("user", "size"),
                       flagged=("predicted_fraud", "sum"))
                  .sort_values("events", ascending=False)
                  .head(12))
    by_state["flag_rate_pct"] = (100.0 * by_state["flagged"] /
                                 by_state["events"]).round(2)
    st.dataframe(by_state, use_container_width=True)

st.markdown(
    '<div class="note"><b>What this page is showing.</b> Each row was scored by '
    'the Spark MLlib GBT trained on all 132,500,000 transactions, loaded from '
    'HDFS and applied to Kafka events as they arrive. The flag rate is higher '
    'than the %.3f%% base fraud rate because the model was trained with '
    'cost-sensitive class weighting, which deliberately favours recall over '
    'precision at the default threshold.</div>' % (base_rate * 100),
    unsafe_allow_html=True,
)

time.sleep(REFRESH_SECONDS)
st.rerun()
