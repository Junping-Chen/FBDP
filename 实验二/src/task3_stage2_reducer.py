#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def main():
    # 接收 key=ALL 的所有记录，内存聚合后计算 totalUsed 与阈值，再过滤并排序输出
    items = []  # (coupon_id, usedCount, sumDays)
    total_used = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            _all, payload = line.split("\t", 1)
        except ValueError:
            continue
        parts = payload.split("\t")
        if len(parts) < 3:
            continue
        coupon_id = parts[0]
        try:
            used = int(parts[1])
            sum_days = int(parts[2])
        except Exception:
            continue
        total_used += used
        items.append((coupon_id, used, sum_days))

    threshold = total_used * 0.01
    result = []
    for coupon_id, used, sum_days in items:
        if used <= threshold or used <= 0:
            continue
        avg = float(sum_days) / float(used)
        result.append((avg, coupon_id))

    # 按平均天数升序
    result.sort(key=lambda x: x[0])

    for avg, coupon_id in result:
        # 输出：<Coupon_id>\t<平均消费间隔>
        sys.stdout.write(f"{coupon_id}\t{avg:.2f}\n")


if __name__ == "__main__":
    main()


