#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def emit(coupon_id, cnt, sum_days):
    # 输出：coupon_id \t usedCount \t sumDays
    sys.stdout.write(f"{coupon_id}\t{cnt}\t{sum_days}\n")


def main():
    current_coupon = None
    cnt = 0
    sum_days = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            coupon_id, days_str = line.split("\t", 1)
        except ValueError:
            continue

        if current_coupon is None:
            current_coupon = coupon_id

        if coupon_id != current_coupon:
            emit(current_coupon, cnt, sum_days)
            current_coupon = coupon_id
            cnt = 0
            sum_days = 0

        try:
            d = int(days_str)
        except Exception:
            d = 0
        cnt += 1
        sum_days += d

    if current_coupon is not None:
        emit(current_coupon, cnt, sum_days)


if __name__ == "__main__":
    main()


