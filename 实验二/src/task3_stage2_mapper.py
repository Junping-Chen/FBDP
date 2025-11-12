#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def main():
    # 读取 stage1 的输出：coupon_id \t usedCount \t sumDays
    # 统一发往同一个key，保证单个reducer全局可见
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        # 保持原行作为 value
        sys.stdout.write(f"ALL\t{line}\n")


if __name__ == "__main__":
    main()


