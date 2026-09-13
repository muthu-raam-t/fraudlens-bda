#!/usr/bin/env python3
"""
Generate the FraudLens proposed-system architecture diagram.

Produces two files in the current directory:
    fraudlens_architecture.svg   vector, scales to any size, editable
    fraudlens_architecture.png   2600 px wide, flattened RGB (no alpha)

Requirements:
    pip install cairosvg pillow

Usage:
    python3 build_diagram.py

Why a generator rather than hand-written SVG: box heights, arrow positions and
the fan-out geometry are all computed from the content, so adding a line to a
stage cannot silently push text outside its border. Edit the `stage(...)` calls
below to change the diagram.
"""

W = 1600
PAD = 40
BOX_L = PAD
BOX_R = W - PAD
BOX_W = BOX_R - BOX_L

INK = "#1a1208"
RULE = "#1a1208"
DONE_BG = "#e8f3e6"
DONE_BD = "#3f7d3a"
DONE_TX = "#2c5c28"
PEND_BG = "#fdf2dc"
PEND_BD = "#b8891f"
PEND_TX = "#8a6612"
TODO_BG = "#f7e9e9"
TODO_BD = "#a54a4a"
TODO_TX = "#7d3535"
NEUTRAL_BG = "#faf7f0"
CALLOUT_BG = "#efe6d4"
PAGE_BG = "#fdfbf6"
PAGE_RGB = (253, 251, 246)

STATUS = {
    "DONE": (DONE_BG, DONE_BD, DONE_TX, "COMPLETED"),
    "PEND": (PEND_BG, PEND_BD, PEND_TX, "PENDING"),
    "TODO": (TODO_BG, TODO_BD, TODO_TX, "NOT STARTED"),
}

out = []
y = PAD


def esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def box(x, w, h, fill, stroke, sw=2.5, rx=6):
    out.append('<rect x="%d" y="%d" width="%d" height="%d" rx="%d" '
               'fill="%s" stroke="%s" stroke-width="%s"/>'
               % (x, y, w, h, rx, fill, stroke, sw))


def text(x, ty, s, size=15, weight="normal", anchor="start",
         fill=INK, family="Helvetica, Arial, sans-serif"):
    out.append('<text x="%s" y="%s" font-family="%s" font-size="%s" '
               'font-weight="%s" text-anchor="%s" fill="%s">%s</text>'
               % (x, ty, family, size, weight, anchor, fill, esc(s)))


def mono(x, ty, s, size=13.5, fill="#42342a", weight="normal", anchor="start"):
    # SVG collapses runs of whitespace, which destroys column alignment in the
    # monospaced result tables. Non-breaking spaces survive.
    s = s.replace("  ", "\u00a0\u00a0")
    text(x, ty, s, size=size, weight=weight, anchor=anchor, fill=fill,
         family="'DejaVu Sans Mono', Consolas, monospace")


def status_pill(bx, bw, py, key):
    bg, bd, tx, label = STATUS[key]
    pw = 148
    px = bx + bw - pw - 18
    out.append('<rect x="%d" y="%d" width="%d" height="24" rx="12" '
               'fill="%s" stroke="%s" stroke-width="1.6"/>'
               % (px, py, pw, bg, bd))
    text(px + pw / 2, py + 16.5, label, size=12.5, weight="bold",
         anchor="middle", fill=tx)


def arrow(cx, length=34, label=None):
    global y
    out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" '
               'stroke-width="3" marker-end="url(#ar)"/>'
               % (cx, y, cx, y + length, RULE))
    if label:
        mono(cx + 14, y + length / 2 + 4, label, size=12, fill="#6b5a48")
    y += length


def stage(num, title, lines, key, bx=BOX_L, bw=BOX_W, subtitle=None):
    """One numbered pipeline stage."""
    global y
    head = 40
    sub = 22 if subtitle else 0
    body = len(lines) * 20
    h = head + sub + body + 22
    bg, bd, _, _ = STATUS[key]
    box(bx, bw, h, NEUTRAL_BG, bd, sw=2.5)
    out.append('<rect x="%d" y="%d" width="%d" height="4" rx="2" fill="%s"/>'
               % (bx, y, bw, bd))
    text(bx + 20, y + 30, "%s  %s" % (num, title), size=17.5, weight="bold")
    status_pill(bx, bw, y + 12, key)
    ty = y + head + 16
    if subtitle:
        mono(bx + 20, ty, subtitle, size=13, fill="#7a6753", weight="bold")
        ty += sub
    for ln in lines:
        mono(bx + 20, ty, ln)
        ty += 20
    y += h


# ---------------------------------------------------------------- title
TITLE_H = 130
box(BOX_L, BOX_W, TITLE_H, "#ffffff", INK, sw=3.5)
text(W / 2, y + 42, "PROPOSED SYSTEM ARCHITECTURE", size=27,
     weight="bold", anchor="middle")
mono(W / 2, y + 68, "FraudLens - Distributed Credit Card Fraud Analytics Framework",
     size=14.5, anchor="middle", fill="#4a3c2e")
mono(W / 2, y + 90,
     "Containerized Hadoop HDFS  +  YARN  +  Apache Spark",
     size=13, anchor="middle", fill="#7a6753")
mono(W / 2, y + 112,
     "ALL TWELVE PHASES EXECUTED  -  every figure below is measured on this cluster",
     size=12.5, anchor="middle", fill="#2c5c28")
y += TITLE_H
arrow(W / 2)

# ---------------------------------------------------------------- stage 0
stage("0.", "SOURCE DATA ACQUISITION", [
    "Erik Altman et al., \"Credit Card Transactions\" (Kaggle)",
    "24,386,900 real transactions   |   2.2 GB CSV   |   15 columns",
    "User, Card, Year, Month, Day, Time, Amount, Use Chip, Merchant Name/City/State,",
    "Zip, MCC, Errors?, Is Fraud?",
], "DONE")
arrow(W / 2)

# ---------------------------------------------------------------- stage 1
stage("1.", "SYNTHETIC SCALING", [
    "Pool-sample 17.7% of EVERY chunk across the whole file  ->  full user coverage",
    "  (a head-of-file read yielded only 248 distinct users - fixed to 1,600)",
    "Jitter: amount +/-3%, day +/-2, time resampled",
    "APPEND device_metadata -> nested JSON {network{vpn,ip}, geo{lat,lon}, hardware{os}}",
    "  fabricated, supplies the SEMI-STRUCTURED half of the Variety requirement",
    "Inject ~2% nulls into merchant_city / merchant_state / zip / errors",
    "LEAKAGE CONTROL: user % 5 == 0 -> holdout;  chunks 1-4 train, chunk 5 holdout",
    "",
    "OUTPUT:  5 chunks x ~6.04 GB  =  28.1 GB  /  132,500,000 rows",
], "DONE", subtitle="scripts/generate_chunk.py   [Python, host machine]")
arrow(W / 2, 44, "generate -> put -> delete  (local disk never exceeds one chunk)")

# ---------------------------------------------------------------- stage 2
stage("2.", "DISTRIBUTED STORAGE - HADOOP HDFS", [
    "1 NameNode (metadata)  +  1 DataNode (blocks)",
    "Block size 128 MB   |   Replication factor 1   |   230 blocks",
    "fsck: HEALTHY, 0 under-replicated, average block 131,360,676 B",
    "",
    "/fraudlens/dataset/unprocessed/    28.1 GB raw CSV   <- MapReduce AND Spark",
    "/fraudlens/dataset/preprocessed/    6.6 GB Parquet   <- all ML phases",
    "/fraudlens/lookup/zip_coords.csv    800 KB           <- broadcast table",
    "/fraudlens/{mapreduce_output, models, aggregates, streaming}/",
], "DONE")

# -------------------------------------------------- split to two engines
SPLIT = 52
cx = W / 2
lx = BOX_L + BOX_W * 0.245
rx = BOX_L + BOX_W * 0.755
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (cx, y, cx, y + 18, RULE))
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (lx, y + 18, rx, y + 18, RULE))
mono(cx, y + 40, "same raw CSV to both engines  ->  this comparison measures the ENGINE, not the file format",
     size=12.5, anchor="middle", fill="#6b5a48")
for px in (lx, rx):
    out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" '
               'stroke-width="3" marker-end="url(#ar)"/>'
               % (px, y + 18, px, y + 18 + SPLIT, RULE))
y += 18 + SPLIT

# ------------------------------------------------ 3A / 3B side by side
GAP = 40
HALF = (BOX_W - GAP) / 2
h2 = 232
top = y
for i, (num, title, sub, lines) in enumerate([
    ("3A.", "HADOOP MAPREDUCE", "Hadoop Streaming on YARN", [
        "mapper.py   ->  state \\t fraud_flag",
        "reducer.py  ->  per-state totals + rate",
        "",
        "225 map tasks, 4 reducers",
        "2.09 GB SPILLED TO LOCAL DISK",
        "265,000,000 spilled records",
        "",
        "ELAPSED:  613 s   (10m 13s)",
    ]),
    ("3B.", "APACHE SPARK", "PySpark DataFrame API on YARN", [
        "groupBy(state).agg(count, sum)",
        "",
        "",
        "in-memory shuffle",
        "no intermediate materialisation",
        "",
        "",
        "ELAPSED:  65.3 s",
    ]),
]):
    bx = BOX_L if i == 0 else BOX_L + HALF + GAP
    y = top
    box(bx, HALF, h2, NEUTRAL_BG, DONE_BD, sw=2.5)
    out.append('<rect x="%d" y="%d" width="%d" height="4" rx="2" fill="%s"/>'
               % (bx, y, HALF, DONE_BD))
    text(bx + 20, y + 30, "%s  %s" % (num, title), size=17, weight="bold")
    status_pill(bx, HALF, y + 12, "DONE")
    mono(bx + 20, y + 54, sub, size=12.5, fill="#7a6753", weight="bold")
    ty = y + 78
    for ln in lines:
        bold = ln.startswith("ELAPSED") or "SPILLED" in ln
        mono(bx + 20, ty, ln, size=13,
             fill="#8a2f2f" if "SPILLED" in ln else ("#1a1208" if bold else "#42342a"),
             weight="bold" if bold else "normal")
        ty += 19
y = top + h2

# ------------------------------------------------------- merge + callout
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (lx, y, lx, y + 26, RULE))
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (rx, y, rx, y + 26, RULE))
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (lx, y + 26, rx, y + 26, RULE))
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" '
           'stroke-width="3" marker-end="url(#ar)"/>' % (cx, y + 26, cx, y + 56, RULE))
y += 56

CW, CH = 760, 104
cbx = (W - CW) / 2
box(cbx, CW, CH, CALLOUT_BG, INK, sw=3)
text(W / 2, y + 40, "MEASURED RESULT:   9.4x SPEEDUP", size=24,
     weight="bold", anchor="middle")
mono(W / 2, y + 66, "identical input, identical output - 222 states,", size=13,
     anchor="middle", fill="#4a3c2e")
mono(W / 2, y + 86, "132,500,000 reconciled, 0.1650% fraud rate", size=13,
     anchor="middle", fill="#4a3c2e")
y += CH
arrow(cx)

# ---------------------------------------------------------------- stage 4
stage("4.", "DISTRIBUTED PREPROCESSING - NATIVE SPARK-SCALA on YARN", [
    "a) Explicit StructType schema        - no inferSchema, saves a full 28 GB pass",
    "b) regexp_replace \"[$,]\"              - \"$1,204.55\"  ->  1204.55",
    "c) Label cast                        - \"Yes\"/\"No\"   ->  1 / 0",
    "d) lpad(split(zip,\".\"), 5, \"0\")       - \"4917.0\"     ->  \"04917\"",
    "e) from_json(device_metadata, declared schema)   - null-safe nested parse",
    "f) SENTINEL IMPUTATION + FLAG COLUMNS            - never .na.drop()",
    "g) BROADCAST JOIN: zip_coords (27,321 rows) x 132.5M  -> merchant lat/lon",
    "h) Haversine distance device <-> merchant",
    "i) approxQuantile p99  ->  is_outlier flag + amount_capped",
    "     original amount PRESERVED - large amounts are genuine fraud signal",
    "j) Assembled txn_timestamp  ->  day_of_week, is_weekend, day_of_year",
    "k) Derived: amount_log, is_refund, is_online, is_night, is_foreign_or_unknown",
    "l) repartition(120, year, month) then partitionBy(year, month)",
    "",
    "VERIFIED:   rows in 132,500,000  =  rows out 132,500,000   |   DROPPED: 0",
    "            null amounts / states / zips / timestamps = 0",
    "ELAPSED: 19m 08s      OUTPUT: 6.6 GB Snappy Parquet, 43 columns",
], "DONE", subtitle="spark-apps/preprocess.scala   [Scala, spark-shell --master yarn]")
arrow(cx)

# ---------------------------------------------------------------- stage 5
stage("5.", "DISTRIBUTED TRAINING - SPARK MLlib on YARN", [
    "FEATURE ENGINEERING (distributed over all 132.5M rows):",
    "   groupBy(user) -> amount mean/std/count  (~2k rows)  -> BROADCAST back",
    "   groupBy(mcc)  -> mcc_frequency                      -> BROADCAST back",
    "   derived -> amount_vs_user_mean, amount_zscore   (no feature uses is_fraud)",
    "",
    "SPLIT: user % 5 == 0 -> holdout. Same rule as generation, so no user appears",
    "       on both sides. A random split would leak (sampled WITH replacement).",
    "CACHE train_df + test_df    |    weightCol = negatives/positives  (~605x)",
    "",
    "TWO FEATURE PIPELINES, deliberately different:",
    "   TREES : StringIndexer -> VectorAssembler -> RF / GBT",
    "   LINEAR: StringIndexer -> OneHotEncoder -> VectorAssembler",
    "                         -> StandardScaler(withMean=false) -> LR",
    "     without one-hot, LR reads merchant_state as an ORDERED number;",
    "     without scaling, amount 0-30,000 swamps the binary flags",
    "",
    "MODELS (all three trained sequentially in ONE run):",
    "   1. LogisticRegression      - deliberately simple baseline",
    "   2. RandomForestClassifier  - ensemble baseline + feature importances",
    "   3. GBTClassifier           - PRIMARY distributed model",
    "",
    "RESULTS on the 26,500,000-row user-disjoint holdout (135m 50s total):",
    "                        PR-AUC    ROC-AUC    RECALL        F1",
    "   GBT  (best)          0.2196     0.9477    0.9121    0.0200",
    "   Random Forest        0.1832     0.9276    0.8938    0.0185",
    "   LogisticRegression   0.0223     0.9060    0.8727    0.0135",
    "",
    "ROC-AUC separates these by 4 points; PR-AUC separates them 10x.",
    "That gap IS the argument for PR-AUC on a 0.1650% positive class.",
    "Accuracy is not quoted: \"never fraud\" scores 99.835%.",
], "DONE", subtitle="spark-apps/train.py   [PySpark]")

# -------------------------------------------- fan out to 6 / 7 / 8
THIRD = (BOX_W - 2 * GAP) / 3
c1 = BOX_L + THIRD / 2
c2 = BOX_L + THIRD + GAP + THIRD / 2
c3 = BOX_L + 2 * (THIRD + GAP) + THIRD / 2
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (cx, y, cx, y + 24, RULE))
out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="3"/>'
           % (c1, y + 24, c3, y + 24, RULE))
for px in (c1, c2, c3):
    out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" '
               'stroke-width="3" marker-end="url(#ar)"/>'
               % (px, y + 24, px, y + 54, RULE))
y += 54

h3 = 366
top = y
for i, (num, title, lines) in enumerate([
    ("6.", "TUNING BENCHMARKS", [
        "MEASURED, adaptive exec off",
        "",
        "partition pruning   11.73x",
        "  year (partition col) vs",
        "  hour (full scan)",
        "",
        "broadcast join       3.79x",
        "  800 KB x 132.5M rows",
        "",
        "caching              1.22x",
        "  understated here: column",
        "  pruning already cut the",
        "  read to 2.6 s, leaving",
        "  little I/O to save.",
        "  Reported as measured.",
    ]),
    ("7.", "SERVING MODEL - CatBoost", [
        "3,000,000 train / 1,000,000",
        "test, all 218,622 fraud kept",
        "582 of 600 iterations used",
        "",
        "PR-AUC        0.6943",
        "ROC-AUC       0.9528",
        "Brier         0.0821",
        "recall @0.9    0.550",
        "precision@0.9  0.776",
        "",
        "NOT better than the GBT:",
        "tested on a 4.32% base rate",
        "vs the GBT's 0.163%. By lift",
        "over random, GBT 135x beats",
        "CatBoost 16x.",
    ]),
    ("8.", "DASHBOARD AGGREGATES", [
        "aggregates.py               ",
        "",
        "fraud rate by state / MCC /",
        "hour / device OS / VPN /",
        "amount bucket",
        "",
        "confusion matrix,",
        "PR curve points,",
        "model comparison",
        "",
        "6.6 GB Parquet  ->  18.7 KB",
        "of JSON, computed once so",
        "the dashboard never queries",
        "big data on page load",
    ]),
]):
    bx = BOX_L + i * (THIRD + GAP)
    y = top
    box(bx, THIRD, h3, NEUTRAL_BG, DONE_BD, sw=2.5)
    out.append('<rect x="%d" y="%d" width="%d" height="4" rx="2" fill="%s"/>'
               % (bx, y, THIRD, DONE_BD))
    text(bx + 18, y + 30, "%s %s" % (num, title), size=15.5, weight="bold")
    status_pill(bx, THIRD, y + 44, "DONE")
    ty = y + 88
    for ln in lines:
        mono(bx + 18, ty, ln, size=12.5)
        ty += 18
y = top + h3
arrow(cx)

# ---------------------------------------------------------------- stage 9
stage("9.", "FastAPI INFERENCE SERVICE", [
    "Loads the CatBoost model ONCE at startup - a SparkSession would cost seconds",
    "per call, CatBoost answers in milliseconds",
    "",
    "POST /predict         ->  verdict, P(fraud), top-5 SHAP contributions",
    "GET  /aggregates/*    ->  precomputed JSON from Phase 8",
    "GET  /stream/recent   ->  latest streaming verdicts",
    "",
    "CALIBRATION: weighted training inflates raw scores ~1:600 in odds, so an",
    "ordinary grocery transaction returned ~50%. The API applies a prior shift",
    "(odds_true = odds_model x base_odds) before responding. Ranking unchanged.",
], "DONE")
arrow(cx)

# --------------------------------------------------------------- stage 10
stage("10.", "REACT ANALYTICS DASHBOARD", [
    "TAB 1  Live scoring : transaction form -> verdict badge, probability gauge,",
    "                      SHAP waterfall",
    "TAB 2  Analytics    : fraud-rate map by state, MCC bars, hourly curve, amount",
    "                      distribution, model comparison, MapReduce vs Spark",
    "                      benchmark, live verdict feed",
    "",
    "Risk shown as a MULTIPLE of the 0.1650% base rate: an absolute 50%",
    "threshold would essentially never fire on this class balance.",
], "DONE", subtitle="Vite + React + Recharts")
arrow(cx)

# --------------------------------------------------------------- stage 11
stage("11.", "SPARK STRUCTURED STREAMING SCORER   (VELOCITY REQUIREMENT)", [
    "readStream on an HDFS file source with an explicit StructType",
    "   - streaming file sources CANNOT infer schema",
    "loads the saved GBT PipelineModel  ->  scores each micro-batch",
    "writes checkpointed verdicts to HDFS as Parquet",
    "feed.sh drips CSV files into the watched directory for a live demonstration",
    "",
    "WRITTEN IN SCALA: spark-master is Alpine 3.10 (Python 3.7 only) and the",
    "nodemanager is Debian 9 (Python 3.5 only). PySpark refuses to run across",
    "minor versions, and Scala launches no Python workers.",
    "",
    "MEASURED RUN:  12 micro-batches  |  6,000 rows scored  |  894 flagged",
], "DONE")
y += 30

# ------------------------------------------------------------ YARN footer
FH = 128
box(BOX_L, BOX_W, FH, "#f2ece0", INK, sw=3)
text(W / 2, y + 34, "RESOURCE MANAGEMENT", size=18, weight="bold", anchor="middle")
mono(W / 2, y + 62,
     "Every distributed job above - MapReduce, Scala ETL, MLlib training, tuning,",
     size=13.5, anchor="middle", fill="#42342a")
mono(W / 2, y + 82,
     "aggregates, streaming - is submitted to and scheduled by YARN. YARN performs no",
     size=13.5, anchor="middle", fill="#42342a")
mono(W / 2, y + 102,
     "computation; it allocates containers (6144 MB / 8 vCores) in which the engines run.",
     size=13.5, anchor="middle", fill="#42342a")
y += FH + PAD

H = int(y)
svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
       'viewBox="0 0 %d %d">' % (W, H, W, H),
       '<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" '
       'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
       '<path d="M 0 0 L 10 5 L 0 10 z" fill="%s"/></marker></defs>' % RULE,
       '<rect x="0" y="0" width="{}" height="{}" fill="{}"/>'.format(W, H, PAGE_BG)]
svg += out
svg.append("</svg>")

SVG_PATH = "fraudlens_architecture.svg"
PNG_PATH = "fraudlens_architecture.png"
PNG_WIDTH = 2600

with open(SVG_PATH, "w") as fh:
    fh.write("\n".join(svg))
print("SVG written: %s  (%d x %d)" % (SVG_PATH, W, H))

# ---------------------------------------------------------------------------
# Rasterise to an OPAQUE PNG.
#
# cairosvg emits RGBA. If anything leaves an alpha channel behind, image
# viewers show a grey/white checkerboard and dark text becomes hard to read on
# it. So the result is explicitly flattened onto a solid canvas and saved as
# RGB with no alpha channel at all.
# ---------------------------------------------------------------------------
try:
    import cairosvg
    from PIL import Image

    cairosvg.svg2png(url=SVG_PATH, write_to=PNG_PATH,
                     output_width=PNG_WIDTH, background_color=PAGE_BG)

    im = Image.open(PNG_PATH)
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGBA")
        canvas = Image.new("RGB", im.size, PAGE_RGB)
        canvas.paste(im, mask=im.split()[-1])
        im = canvas
    else:
        im = im.convert("RGB")
    im.save(PNG_PATH, "PNG", optimize=True)

    check = Image.open(PNG_PATH)
    assert "A" not in check.mode, "PNG still has an alpha channel"
    print("PNG written: %s  (%d x %d, mode %s, corner %s)"
          % (PNG_PATH, check.size[0], check.size[1], check.mode,
             check.getpixel((0, 0))))
except ImportError:
    print("PNG skipped -- install the renderers first:")
    print("    pip install cairosvg pillow")
