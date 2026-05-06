#!/usr/bin/env python3
"""
Entry point:
  spark-submit run.py -q tpch_q1 -c 10000000
  spark-submit run.py --list
"""
from framework.runner import run

if __name__ == "__main__":
    run()
