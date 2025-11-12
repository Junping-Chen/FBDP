#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def emit(merchant_id, neg, nor, pos):
    sys.stdout.write(f"{merchant_id}\t{neg}\t{nor}\t{pos}\n")


def main():
    current_merchant = None
    neg = nor = pos = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            merchant_id, typ = line.split("\t", 1)
        except ValueError:
            continue

        if current_merchant is None:
            current_merchant = merchant_id

        if merchant_id != current_merchant:
            emit(current_merchant, neg, nor, pos)
            current_merchant = merchant_id
            neg = nor = pos = 0

        if typ == "NEG":
            neg += 1
        elif typ == "NOR":
            nor += 1
        elif typ == "POS":
            pos += 1

    if current_merchant is not None:
        emit(current_merchant, neg, nor, pos)


if __name__ == "__main__":
    main()


