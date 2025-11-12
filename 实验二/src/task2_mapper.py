#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import csv


def is_null(s):
    if s is None:
        return True
    t = s.strip()
    return t == "" or t.lower() == "null"


def norm_distance(s):
    if s is None:
        return "0"
    t = s.strip()
    if t == "" or t.lower() == "null":
        return "0"
    return t


def main():
    # 仅线下数据
    # 输出 key=merchant|distance, value=user_id
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row:
            continue
        if row[0] == "User_id":
            continue
        try:
            # User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
            user_id = row[0]
            merchant_id = row[1]
            coupon_id = row[2]
            distance = row[4]
            date = row[6]
        except Exception:
            continue

        if is_null(coupon_id) or is_null(date):
            continue

        d = norm_distance(distance)
        key = f"{merchant_id}|{d}"
        sys.stdout.write(f"{key}\t{user_id}\n")


if __name__ == "__main__":
    main()


