#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import csv
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime


def is_null(s):
    if s is None:
        return True
    t = s.strip()
    return t == "" or t.lower() == "null"


def eff_discount(s):
    # 返回 [0,1]，越小折扣越大
    if s is None:
        return 1.0
    t = s.strip().lower()
    if t == "" or t == "null":
        return 1.0
    if ":" in t:
        parts = t.split(":")
        if len(parts) >= 2:
            try:
                full = float(parts[0])
                minus = float(parts[1])
                if full > 0:
                    eff = 1.0 - (minus / full)
                    if eff < 0:
                        eff = 0.0
                    if eff > 1:
                        eff = 1.0
                    return eff
            except Exception:
                return 1.0
        return 1.0
    try:
        x = float(t)
        if x <= 0:
            return 1.0
        if x > 1:
            return 1.0
        return x
    except Exception:
        return 1.0


def bucketize(x):
    bd = Decimal(str(x)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
    return format(bd, "f")


def diff_days(rcv, use):
    try:
        d1 = datetime.strptime(rcv, "%Y%m%d")
        d2 = datetime.strptime(use, "%Y%m%d")
        return (d2 - d1).days
    except Exception:
        return None


def main():
    # 线下数据
    # Emit：
    #   bucket\tREC\t1
    #   bucket\tUSE\t1
    #   bucket\tDAYS\t<days>
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row:
            continue
        if row[0] == "User_id":
            continue
        try:
            # User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            coupon_id = row[2]
            discount = row[3]
            date_rcv = row[5]
            date_use = row[6]
        except Exception:
            continue

        if is_null(coupon_id):
            continue
        b = bucketize(eff_discount(discount))
        sys.stdout.write(f"{b}\tREC\t1\n")
        if not is_null(date_rcv) and not is_null(date_use):
            d = diff_days(date_rcv, date_use)
            if d is not None and d >= 0:
                sys.stdout.write(f"{b}\tUSE\t1\n")
                sys.stdout.write(f"{b}\tDAYS\t{d}\n")


if __name__ == "__main__":
    main()


