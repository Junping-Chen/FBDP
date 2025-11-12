#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from decimal import Decimal, ROUND_HALF_UP


def fmt2(x):
    return format(Decimal(str(x)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP), "f")


def emit(bucket, rec, use, sum_days):
    if rec <= 0:
        return
    usage_rate = (float(use) / float(rec)) if rec > 0 else 0.0
    avg_days = (float(sum_days) / float(use)) if use > 0 else 0.0
    sys.stdout.write(f"{bucket}\t{rec}\t{use}\t{fmt2(usage_rate)}\t{fmt2(avg_days)}\n")


def main():
    current_bucket = None
    rec = 0
    use = 0
    sum_days = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            bucket, tag, val = line.split("\t", 2)
        except ValueError:
            continue

        if current_bucket is None:
            current_bucket = bucket

        if bucket != current_bucket:
            emit(current_bucket, rec, use, sum_days)
            current_bucket = bucket
            rec = 0
            use = 0
            sum_days = 0

        if tag == "REC":
            try:
                rec += int(val)
            except Exception:
                pass
        elif tag == "USE":
            try:
                use += int(val)
            except Exception:
                pass
        elif tag == "DAYS":
            try:
                sum_days += int(val)
            except Exception:
                pass

    if current_bucket is not None:
        emit(current_bucket, rec, use, sum_days)


if __name__ == "__main__":
    main()


