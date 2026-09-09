#!/usr/bin/env python3
"""
MapReduce MAPPER -- fraud rate by merchant state.

Reads raw CSV lines from stdin (Hadoop Streaming feeds one input split per
mapper) and emits one tab-separated record per transaction:

    <merchant_state>\t<is_fraud as 0 or 1>

Why the csv module and not line.split(","):
    the Amount field is written with thousands separators ("$1,204.55"), so it
    is quoted in the CSV, and the trailing device_metadata field is a quoted
    JSON blob full of commas. A naive split would shift every column.

Rows with a missing merchant_state are bucketed as UNKNOWN rather than dropped,
so the totals still reconcile against the raw row count.
"""
import csv
import sys

# Column positions in the generated dataset.
COL_MERCHANT_STATE = 10
COL_IS_FRAUD = 14
EXPECTED_FIELDS = 16


def main():
    reader = csv.reader(sys.stdin)
    out = sys.stdout

    for row in reader:
        # Each of the 5 chunk files carries its own header line.
        if not row or row[0] == "user" or row[0] == "User":
            continue
        if len(row) < EXPECTED_FIELDS:
            # Malformed line -- count it as a counter, never crash the task.
            sys.stderr.write("reporter:counter:FraudByState,MalformedRows,1\n")
            continue

        state = row[COL_MERCHANT_STATE].strip().upper()
        if not state:
            state = "UNKNOWN"

        fraud = row[COL_IS_FRAUD].strip().lower()
        flag = "1" if fraud in ("yes", "1", "true") else "0"

        out.write("{}\t{}\n".format(state, flag))


if __name__ == "__main__":
    main()
