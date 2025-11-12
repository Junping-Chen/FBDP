#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import csv
from datetime import datetime


def is_null(s):
    if s is None:
        return True
    t = s.strip()
    return t == "" or t.lower() == "null"


def diff_days(rcv, use):
    try:
        d1 = datetime.strptime(rcv, "%Y%m%d")
        d2 = datetime.strptime(use, "%Y%m%d")
        delta = (d2 - d1).days
        return delta
    except Exception:
        return None


def main():
    # 线下数据
    # 输出：coupon_id \t days
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row:
            continue
        if row[0] == "User_id":
            continue
        try:
            # User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            coupon_id = row[2]
            date_rcv = row[5]
            date_use = row[6]
        except Exception:
            continue

        if is_null(coupon_id) or is_null(date_rcv) or is_null(date_use):
            continue

        d = diff_days(date_rcv, date_use)
        if d is None or d < 0:
            continue
        sys.stdout.write(f"{coupon_id}\t{d}\n")


if __name__ == "__main__":
    main()


