#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys


def emit(key, userset):
    # key 是 merchant|distance
    try:
        merchant, distance = key.split("|", 1)
    except ValueError:
        return
    sys.stdout.write(f"{merchant}\t{distance}\t{len(userset)}\n")


def main():
    current_key = None
    userset = set()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, user_id = line.split("\t", 1)
        except ValueError:
            continue

        if current_key is None:
            current_key = key

        if key != current_key:
            emit(current_key, userset)
            current_key = key
            userset = set()

        userset.add(user_id)

    if current_key is not None:
        emit(current_key, userset)


if __name__ == "__main__":
    main()


