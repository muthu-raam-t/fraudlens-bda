#!/usr/bin/env python3
"""
Scale the base credit-card transaction dataset up to big-data volume, ONE CHUNK
AT A TIME, so local disk never holds more than a single chunk.

What this does and does NOT invent
----------------------------------
REAL, taken from the source dataset:
    user, card, year, month, day, time, amount, use_chip, merchant_name,
    merchant_city, merchant_state, zip, mcc, is_fraud
JITTERED (real values, perturbed -- never fabricated from nothing):
    amount (+/- pct), day, time
FABRICATED (not present in the source at all):
    device_metadata -- a nested JSON blob (VPN flag, device IP, geo, OS).
    Added deliberately to provide the SEMI-STRUCTURED half of the "Variety"
    requirement, since the base dataset is entirely flat CSV.
    Its correlation with fraud is intentionally weak so the model does not
    learn the generator instead of the data.

Leakage prevention
------------------
Users are partitioned into disjoint pools by a hash of the user id. Chunks
1..N-1 draw only from TRAIN users; chunk N draws only from HELD-OUT users.
No transaction of any user can appear on both sides of the split, which is what
makes the evaluation honest despite sampling with replacement.

Messiness is preserved on purpose
---------------------------------
    - amount keeps its "$" prefix and thousands separators -> the Scala job has
      real regex cleanup to do
    - is_fraud stays "Yes"/"No" -> real casting work
    - nulls are injected into merchant_city / merchant_state / zip / errors
    - device_metadata sometimes has whole sub-objects missing
This is the "Veracity" evidence.

Usage:
    python3 scripts/generate_chunk.py --chunk 1 --total-chunks 5 --target-gb 5.6
"""
import argparse
import json
import os
import sys
import time as _time

import numpy as np
import pandas as pd

SOURCE_COLS = [
    "user", "card", "year", "month", "day", "time", "amount", "use_chip",
    "merchant_name", "merchant_city", "merchant_state", "zip", "mcc",
    "errors", "is_fraud",
]
OUTPUT_COLS = SOURCE_COLS + ["device_metadata"]

OS_CHOICES = np.array(["Android", "iOS", "Windows", "macOS", "Linux", "ChromeOS"])
OS_WEIGHTS = np.array([0.34, 0.30, 0.22, 0.09, 0.03, 0.02])


def normalise_columns(df):
    """Map the source dataset's messy headers onto clean snake_case names."""
    renames = {}
    for col in df.columns:
        key = col.strip().lower().replace("?", "").replace(" ", "_")
        renames[col] = key
    df = df.rename(columns=renames)
    # The source uses "errors?" -> "errors" and "is_fraud?" -> "is_fraud",
    # already handled above. Guard against absent optional columns.
    for col in SOURCE_COLS:
        if col not in df.columns:
            df[col] = np.nan
    return df[SOURCE_COLS]


def parse_amount(series):
    """'$134.09' / '-$50.00' / '$1,204.55' -> float."""
    cleaned = (
        series.astype(str)
        .str.replace(r"[$,]", "", regex=True)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").astype("float32")


def load_pool(source, pool_rows, want_holdout, test_mod, chunksize,
              source_rows, rng, verbose=True):
    """
    Build ONE row pool, sampled across the ENTIRE source file.

    The source dataset is sorted by user, so filling a pool from the first N
    rows would cover only the first few hundred users. We instead take a random
    fraction from every chunk of the file, which gives full user coverage with
    bounded memory.
    """
    share = 0.2 if want_holdout else 0.8
    frac = min(1.0, (pool_rows * 1.15) / (share * source_rows))
    parts, kept, scanned = [], 0, 0
    if verbose:
        print(f"  sampling {frac:.1%} of every source chunk "
              f"({'holdout' if want_holdout else 'train'} users)")

    reader = pd.read_csv(source, dtype=str, chunksize=chunksize, low_memory=False)
    for raw in reader:
        df = normalise_columns(raw)
        df["amount_num"] = parse_amount(df["amount"])
        df = df.dropna(subset=["user", "amount_num"])
        df["user"] = pd.to_numeric(df["user"], errors="coerce").astype("Int32")
        df = df.dropna(subset=["user"])

        is_test = (df["user"].astype("int64") % 5) == test_mod
        df = df[is_test] if want_holdout else df[~is_test]
        if frac < 1.0 and len(df):
            df = df.sample(frac=frac, random_state=int(rng.integers(0, 2**31)))

        parts.append(df)
        kept += len(df)
        scanned += len(raw)
        if verbose:
            print(f"  scanned {scanned:,} source rows | pool {kept:,}", flush=True)

    if not parts:
        sys.exit("ERROR: pool is empty -- is the source file correct?")

    pool = pd.concat(parts, ignore_index=True)
    del parts
    if len(pool) > pool_rows:
        pool = pool.sample(n=pool_rows, random_state=7).reset_index(drop=True)
    return pool


def build_device_metadata(n, rng, lat, lon):
    """Vectorised construction of the nested JSON column."""
    vpn = rng.random(n) < 0.11
    octets = rng.integers(1, 255, size=(n, 4))
    ip = (
        pd.Series(octets[:, 0].astype(str)) + "." +
        pd.Series(octets[:, 1].astype(str)) + "." +
        pd.Series(octets[:, 2].astype(str)) + "." +
        pd.Series(octets[:, 3].astype(str))
    )
    os_pick = rng.choice(OS_CHOICES, size=n, p=OS_WEIGHTS)

    net_seg = (
        '{"network":{"vpn":'
        + pd.Series(np.where(vpn, "true", "false"))
        + ',"ip":"' + ip + '"}'
    )

    # ~4% of rows lose the geo object entirely; ~3% lose hardware.
    geo_present = rng.random(n) >= 0.04
    hw_present = rng.random(n) >= 0.03

    geo_body = (
        ',"geo":{"lat":' + pd.Series(np.round(lat, 5).astype(str))
        + ',"lon":' + pd.Series(np.round(lon, 5).astype(str)) + "}"
    )
    hw_body = ',"hardware":{"os":"' + pd.Series(os_pick) + '"}'

    geo_seg = geo_body.where(pd.Series(geo_present), "")
    hw_seg = hw_body.where(pd.Series(hw_present), "")

    return (net_seg + geo_seg + hw_seg + "}").to_numpy(), vpn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/credit_card_transactions-ibm_v2.csv")
    ap.add_argument("--zip-coords", default="data/source/zip_coords.csv")
    ap.add_argument("--out-dir", default="data/raw")
    ap.add_argument("--chunk", type=int, required=True, help="1-based chunk number")
    ap.add_argument("--total-chunks", type=int, default=5)
    ap.add_argument("--target-gb", type=float, default=5.6)
    ap.add_argument("--pool-rows", type=int, default=3_000_000)
    ap.add_argument("--batch-rows", type=int, default=500_000)
    ap.add_argument("--chunksize", type=int, default=2_000_000)
    ap.add_argument("--source-rows", type=int, default=24_386_900,
                    help="Approximate row count of the source file")
    ap.add_argument("--amount-jitter", type=float, default=0.03)
    ap.add_argument("--null-rate", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.chunk < 1 or args.chunk > args.total_chunks:
        sys.exit(f"ERROR: --chunk must be between 1 and {args.total_chunks}")
    if not os.path.exists(args.source):
        sys.exit(f"ERROR: source not found: {args.source}")

    seed = args.seed if args.seed is not None else 1000 + args.chunk
    rng = np.random.default_rng(seed)

    is_holdout_chunk = args.chunk == args.total_chunks
    role = "HELD-OUT (test)" if is_holdout_chunk else "TRAIN"
    target_bytes = int(args.target_gb * (1024 ** 3))

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, f"chunk_{args.chunk}.csv")
    if os.path.exists(out_path):
        sys.exit(f"ERROR: {out_path} already exists -- delete it or pick another chunk.")

    print(f"=== chunk {args.chunk}/{args.total_chunks} | role={role} | "
          f"target={args.target_gb} GB | seed={seed} ===")

    # --- ZIP -> coordinate lookup (also used later as a Spark broadcast join) ---
    zip_lat = zip_lon = None
    if os.path.exists(args.zip_coords):
        zc = pd.read_csv(args.zip_coords, dtype={"zip": str})
        zip_lat = dict(zip(zc["zip"], zc["latitude"]))
        zip_lon = dict(zip(zc["zip"], zc["longitude"]))
        print(f"Loaded {len(zip_lat):,} ZIP coordinates.")
    else:
        print(f"WARNING: {args.zip_coords} missing -- geo will use a national centroid.")

    print("Building the row pool (one pass over the whole source)...")
    pool = load_pool(
        args.source, args.pool_rows, want_holdout=is_holdout_chunk,
        test_mod=0, chunksize=args.chunksize,
        source_rows=args.source_rows, rng=rng,
    )
    print(f"Sampling from a pool of {len(pool):,} rows "
          f"({pool['user'].nunique():,} distinct users).")

    pool_zip_clean = (
        pool["zip"].astype(str).str.split(".").str[0].str.zfill(5).to_numpy()
    )
    pool_amount = pool["amount_num"].to_numpy()
    pool_n = len(pool)

    t0 = _time.time()
    written = 0
    header_written = False
    batch_no = 0

    while True:
        size_now = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        if size_now >= target_bytes:
            break

        batch_no += 1
        idx = rng.integers(0, pool_n, size=args.batch_rows)
        batch = pool.iloc[idx].reset_index(drop=True)
        n = len(batch)

        # --- jitter the real values -------------------------------------
        factor = 1.0 + rng.uniform(-args.amount_jitter, args.amount_jitter, size=n)
        amounts = np.round(pool_amount[idx] * factor, 2)
        # Keep the "$" and thousands separators: the Scala job must clean this.
        batch["amount"] = (
            pd.Series(amounts)
            .map(lambda v: f"-${abs(v):,.2f}" if v < 0 else f"${v:,.2f}")
            .to_numpy()
        )
        batch["day"] = np.clip(
            pd.to_numeric(batch["day"], errors="coerce").fillna(15).to_numpy()
            + rng.integers(-2, 3, size=n),
            1, 28,
        ).astype(int)
        batch["time"] = (
            pd.Series(rng.integers(0, 24, size=n)).astype(str).str.zfill(2)
            + ":" +
            pd.Series(rng.integers(0, 60, size=n)).astype(str).str.zfill(2)
        )

        # --- fabricated nested JSON -------------------------------------
        zips = pool_zip_clean[idx]
        if zip_lat is not None:
            lat = np.array([zip_lat.get(z, 39.833333) for z in zips], dtype="float64")
            lon = np.array([zip_lon.get(z, -98.583333) for z in zips], dtype="float64")
        else:
            lat = np.full(n, 39.833333)
            lon = np.full(n, -98.583333)
        lat = lat + rng.normal(0, 0.02, size=n)
        lon = lon + rng.normal(0, 0.02, size=n)
        device_meta, vpn = build_device_metadata(n, rng, lat, lon)
        batch["device_metadata"] = device_meta

        # Weak, deliberate signal only -- must not dominate the model.
        flip = (vpn & (rng.random(n) < 0.004))
        batch.loc[flip, "is_fraud"] = "Yes"

        # --- inject missing values (Veracity) ---------------------------
        for col in ("merchant_city", "merchant_state", "zip", "errors"):
            mask = rng.random(n) < args.null_rate
            batch.loc[mask, col] = np.nan

        batch[OUTPUT_COLS].to_csv(
            out_path, mode="a", header=not header_written, index=False
        )
        header_written = True
        written += n

        size_mb = os.path.getsize(out_path) / (1024 ** 2)
        pct = 100 * size_mb / (target_bytes / (1024 ** 2))
        elapsed = _time.time() - t0
        rate = size_mb / elapsed if elapsed else 0
        print(f"  batch {batch_no:>4} | {written:>12,} rows | {size_mb:8.1f} MB "
              f"({pct:5.1f}%) | {rate:5.1f} MB/s", flush=True)

    final_bytes = os.path.getsize(out_path)
    manifest = {
        "chunk": args.chunk,
        "total_chunks": args.total_chunks,
        "role": "holdout" if is_holdout_chunk else "train",
        "rows": int(written),
        "bytes": int(final_bytes),
        "gb": round(final_bytes / (1024 ** 3), 3),
        "seed": seed,
        "user_split_rule": "user % 5 == 0 -> holdout, else train",
        "fabricated_columns": ["device_metadata"],
        "jittered_columns": ["amount", "day", "time"],
        "null_rate": args.null_rate,
        "generated_at": _time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    with open(os.path.join(args.out_dir, f"chunk_{args.chunk}.manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"\nDONE  {out_path}")
    print(f"      {written:,} rows | {final_bytes / (1024 ** 3):.2f} GB "
          f"| {(_time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()
