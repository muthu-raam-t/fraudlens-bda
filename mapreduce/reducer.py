#!/usr/bin/env python3
"""
MapReduce REDUCER -- fraud rate by merchant state.

Hadoop Streaming guarantees that all records for a given key arrive at the same
reducer, sorted by key. So we accumulate while the key stays the same and emit
one summary line when it changes.

Input  (per line):  <state>\t<0 or 1>
Output (per line):  <state>\t<total_txns>\t<fraud_txns>\t<fraud_rate_pct>
"""
import sys


def emit(state, total, fraud):
    if state is None or total == 0:
        return
    rate = 100.0 * fraud / total
    sys.stdout.write("{}\t{}\t{}\t{:.4f}\n".format(state, total, fraud, rate))


def main():
    current = None
    total = 0
    fraud = 0

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        state, flag = parts

        if state != current:
            emit(current, total, fraud)
            current, total, fraud = state, 0, 0

        total += 1
        if flag == "1":
            fraud += 1

    emit(current, total, fraud)


if __name__ == "__main__":
    main()
