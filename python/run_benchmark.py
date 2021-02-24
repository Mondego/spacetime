import argparse

import numpy as np
import time
from progressbar import ProgressBar

from benchmarks.push_conflicting_updates import run_bench as push_conf_updates_bench
from benchmarks.push_creates import run_bench as push_creates_bench
from benchmarks.push_reg_updates import run_bench as push_reg_updates_bench
from benchmarks.read_creates import run_bench as read_create_bench
from benchmarks.redis_read_creates import run_bench as redis_read_create_bench

BENCHMARKS = [read_create_bench]

def main(args):
    for bench in BENCHMARKS:
        avgs = list()
        pbar = ProgressBar()
        for rn in pbar(range(args.runs)):
            avgs.extend(bench(args.oc, args.cc, rn))
        print (f"{bench.__module__}: {np.mean(avgs)*1000:.4f},{np.std(avgs)*1000:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--oc", type=int, default=100)
    parser.add_argument("--cc", type=int, default=5)
    parser.add_argument("--runs", type=int, default=1)
    args = parser.parse_args()
    main(args)
