#!/usr/bin/env python3
"""
Build a ZIP -> (latitude, longitude) lookup table from the source dataset.

Coordinates are derived from the US state centroid of each ZIP's state, with a
small deterministic offset seeded by the ZIP itself. They are therefore
geographically plausible (a Texas ZIP lands in Texas) without requiring an
external geocoding dataset.

The output is small (a few hundred KB) and is used in Spark as a BROADCAST JOIN
lookup against the full transaction table -- this is the performance-tuning
demonstration for the project.

Usage:
    python3 scripts/build_zip_lookup.py \
        --source data/source/credit_card_transactions-ibm_v2.csv \
        --out data/source/zip_coords.csv
"""
import argparse
import hashlib
import os
import sys

import pandas as pd

# Approximate geographic centroids of US states/territories.
STATE_CENTROIDS = {
    "AL": (32.806671, -86.791130), "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221), "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564), "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371), "DE": (39.318523, -75.507141),
    "DC": (38.897438, -77.026817), "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074), "HI": (21.094318, -157.498337),
    "ID": (44.240459, -114.478828), "IL": (40.349457, -88.986137),
    "IN": (39.849426, -86.258278), "IA": (42.011539, -93.210526),
    "KS": (38.526600, -96.726486), "KY": (37.668140, -84.670067),
    "LA": (31.169546, -91.867805), "ME": (44.693947, -69.381927),
    "MD": (39.063946, -76.802101), "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095), "MN": (45.694454, -93.900192),
    "MS": (32.741646, -89.678696), "MO": (38.456085, -92.288368),
    "MT": (46.921925, -110.454353), "NE": (41.125370, -98.268082),
    "NV": (38.313515, -117.055374), "NH": (43.452492, -71.563896),
    "NJ": (40.298904, -74.521011), "NM": (34.840515, -106.248482),
    "NY": (42.165726, -74.948051), "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012), "OH": (40.388783, -82.764915),
    "OK": (35.565342, -96.928917), "OR": (44.572021, -122.070938),
    "PA": (40.590752, -77.209755), "RI": (41.680893, -71.511780),
    "SC": (33.856892, -80.945007), "SD": (44.299782, -99.438828),
    "TN": (35.747845, -86.692345), "TX": (31.054487, -97.563461),
    "UT": (40.150032, -111.862434), "VT": (44.045876, -72.710686),
    "VA": (37.769337, -78.169968), "WA": (47.400902, -121.490494),
    "WV": (38.491226, -80.954453), "WI": (44.268543, -89.616508),
    "WY": (42.755966, -107.302490), "PR": (18.220833, -66.590149),
}
# Fallback: geographic centre of the contiguous US.
DEFAULT_CENTROID = (39.833333, -98.583333)


def deterministic_offset(key: str, spread: float = 1.2):
    """Stable pseudo-random offset in [-spread, +spread] derived from `key`."""
    digest = hashlib.md5(key.encode("utf-8")).digest()
    lat_frac = int.from_bytes(digest[0:4], "big") / 0xFFFFFFFF
    lon_frac = int.from_bytes(digest[4:8], "big") / 0xFFFFFFFF
    return ((lat_frac * 2 - 1) * spread, (lon_frac * 2 - 1) * spread)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="Source transactions CSV")
    ap.add_argument("--out", required=True, help="Output zip_coords.csv path")
    ap.add_argument("--chunksize", type=int, default=2_000_000)
    args = ap.parse_args()

    if not os.path.exists(args.source):
        sys.exit(f"ERROR: source file not found: {args.source}")

    pairs = set()
    reader = pd.read_csv(
        args.source,
        usecols=lambda c: c.strip().lower() in ("zip", "merchant state"),
        dtype=str,
        chunksize=args.chunksize,
        low_memory=False,
    )
    rows_seen = 0
    for chunk in reader:
        chunk.columns = [c.strip().lower() for c in chunk.columns]
        chunk = chunk.dropna(subset=["zip"])
        for zip_code, state in zip(chunk["zip"], chunk["merchant state"]):
            pairs.add((str(zip_code).strip(), str(state).strip() if pd.notna(state) else ""))
        rows_seen += len(chunk)
        print(f"  scanned {rows_seen:,} rows, {len(pairs):,} unique ZIPs so far", flush=True)

    records = []
    for zip_code, state in sorted(pairs):
        # ZIPs arrive as floats in the source ("91750.0") -- normalise to 5 digits.
        clean_zip = zip_code.split(".")[0].zfill(5)
        base_lat, base_lon = STATE_CENTROIDS.get(state.upper(), DEFAULT_CENTROID)
        d_lat, d_lon = deterministic_offset(clean_zip)
        records.append(
            {
                "zip": clean_zip,
                "state": state.upper(),
                "latitude": round(base_lat + d_lat, 6),
                "longitude": round(base_lon + d_lon, 6),
            }
        )

    out_df = pd.DataFrame(records).drop_duplicates(subset=["zip"])
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    out_df.to_csv(args.out, index=False)
    size_kb = os.path.getsize(args.out) / 1024
    print(f"\nWrote {len(out_df):,} ZIP entries to {args.out} ({size_kb:.1f} KB)")
    print("This file is the broadcast-join lookup table for Spark.")


if __name__ == "__main__":
    main()
