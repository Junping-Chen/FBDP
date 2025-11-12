#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import csv


def is_null(s):
    if s is None:
        return True
    t = s.strip()
    return t == "" or t.lower() == "null"


def main():
    # 用法：cat offline.csv | python task1_mapper.py
    #      cat online.csv  | python task1_mapper.py --online
    is_online = ("--online" in sys.argv)
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row:
            continue
        # 跳过表头
        if row[0] == "User_id":
            continue
        try:
            if is_online:
                # User_id,Merchant_id,Action,Coupon_id,Discount_rate,Date_received,Date
                merchant_id = row[1]
                coupon_id = row[3]
                date = row[6]
            else:
                # User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
                merchant_id = row[1]
                coupon_id = row[2]
                date = row[6]
        except Exception:
            continue

        coupon_is_null = is_null(coupon_id)
        date_is_null = is_null(date)

        typ = None
        if (date_is_null and (not coupon_is_null)):
            typ = "NEG"
        elif ((not date_is_null) and coupon_is_null):
            typ = "NOR"
        elif ((not date_is_null) and (not coupon_is_null)):
            typ = "POS"
        else:
            # 其他情况忽略
            continue

        # 输出：<Merchant_id>\t<NEG|NOR|POS>
        sys.stdout.write(f"{merchant_id}\t{typ}\n")


if __name__ == "__main__":
    main()


