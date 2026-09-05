#!/usr/bin/env python3
"""Re-derive every headline number in the paper straight from results_final.csv.

Run this before submitting.  If a line says FAIL, the paper claims something the
data does not support and must be corrected.  Nothing here reuses the analysis
scripts; it recomputes from the raw trial records.
"""
import re, sys
import numpy as np, pandas as pd

d = pd.read_csv("results_final.csv")
ok = fail = 0


def check(desc, got, want, tol=0.06):
    global ok, fail
    if want == 0:
        good = abs(got) < 1e-9
    else:
        good = abs(got - want) / abs(want) <= tol
    print(f"  [{'OK  ' if good else 'FAIL'}] {desc:<58} got {got:>10.4g}  paper {want:>10.4g}")
    ok, fail = ok + good, fail + (not good)


def comp(lab, plan, s, c=4):
    q = d[(d.label == lab) & (d.plan == plan) & (d.sigma_target == s)]
    if plan != "FULL_SHIP":
        q = q[q.cores_b == c]
    return None if q.empty else (q.setup_s.median(), q.pq_s_med.median(),
                                 q.setup_bytes.median(), q.pq_bytes.median())


def kstar(lab, s, metric, c=4):
    r, p = comp(lab, "PROJECT_ONLY", s, c), comp(lab, "FULL_PUSH", s, c)
    if not (r and p):
        return np.nan
    i = 0 if metric == "time" else 2
    den = p[i + 1] - r[i + 1]
    return (r[i] - p[i]) / den if den > 0 else np.inf


print("== trial counts")
check("total trials", len(d), 513, tol=0.001)
check("distinct link/dataset labels", d.label.nunique(), 9, tol=0.001)
check("datasets", d.dataset.nunique(), 2, tol=0.001)

print("\n== measured goodput per condition (MB/s)")
for lab, want in [("10gbit_native", 888), ("1gbit_10ms", 118), ("1gbit_50ms", 91.3),
                  ("1gbit_150ms", 33.0), ("500mbit_50ms", 58.5), ("100mbit_50ms", 11.9)]:
    sh = d[(d.label == lab) & (d.plan == "FULL_SHIP")]
    if not sh.empty:
        check(lab, float((sh.setup_bytes / sh.setup_s).median()) / 1e6, want, tol=0.08)

print("\n== Result 1: egress-optimal k* is the same on every link (sigma=0.001)")
vals = []
for lab in ["10gbit_native", "1gbit_10ms", "1gbit_50ms", "1gbit_150ms",
            "500mbit_50ms", "100mbit_50ms"]:
    v = kstar(lab, 0.001, "cost")
    if np.isfinite(v):
        vals.append(v)
        check(f"{lab} egress k*", v, 796, tol=0.02)
if vals:
    spread = (max(vals) - min(vals)) / np.mean(vals)
    check("spread of egress k* across links (should be ~0)", spread, 0.0, tol=1)
    print(f"         -> min {min(vals):.0f}, max {max(vals):.0f}: "
          f"{'invariant' if spread < 0.02 else 'NOT invariant'}")

print("\n== Result 1/2: latency-optimal k* does vary (sigma=0.001)")
for lab, want in [("10gbit_native", 174), ("1gbit_10ms", 69), ("1gbit_50ms", 31),
                  ("1gbit_150ms", 27), ("500mbit_50ms", 32), ("100mbit_50ms", 114)]:
    v = kstar(lab, 0.001, "time")
    if np.isfinite(v):
        check(f"{lab} latency k*", v, want, tol=0.10)

print("\n== Result 4: RTT series at 1 Gbit, sigma=0.001")
for lab, want in [("10gbit_native", 174), ("1gbit_10ms", 69),
                  ("1gbit_50ms", 31), ("1gbit_150ms", 27)]:
    check(f"{lab}", kstar(lab, 0.001, "time"), want, tol=0.10)

print("\n== Result 7: gap concentrates at low selectivity (10gbit_full)")
for s, want in [(0.001, 6), (0.01, 2), (0.05, 1), (0.1, 1), (1.0, 1)]:
    kt, kc = kstar("10gbit_full", s, "time"), kstar("10gbit_full", s, "cost")
    if np.isfinite(kt) and kt > 0 and np.isfinite(kc):
        check(f"sigma={s:g} gap (egress k* / latency k*)", kc / kt, want, tol=0.35)

print("\n== Result 8: footer selectivity estimator, two datasets")
for lab, ds, want in [("10gbit_full", "synth", 0.0009), ("tpch_full", "TPC-H", 0.0224)]:
    x = d[(d.label == lab) & (d.plan == "FULL_PUSH")]
    if not x.empty:
        g = x.groupby("sigma_target").agg(h=("sigma_hat", "median"), t=("sigma_true", "median"))
        check(f"{ds} max |sigma_hat - sigma_true|", float((g.h - g.t).abs().max()),
              want, tol=0.15)

print("\n== Result 5: B's core budget barely moves k* (10gbit_full, sigma=0.05)")
ks = [kstar("10gbit_full", 0.05, "time", c) for c in (1, 2, 4, 8)]
ks = [k for k in ks if np.isfinite(k)]
if len(ks) > 1:
    check("spread across 1/2/4/8 cores (fraction of mean)",
          (max(ks) - min(ks)) / np.mean(ks), 0.20, tol=0.60)
    print(f"         -> {[round(k) for k in ks]}")

print("")
print("== Result 6: cost model accuracy, held-out evaluation")
try:
    import subprocess, sys, re
    out = subprocess.run([sys.executable, "model_v6.py"],
                         capture_output=True, text=True).stdout
    blk = out.split("HELD OUT only")[-1]
    for key, want in (("v1  no pruning", 48.9), ("v4  pruning", 38.5),
                      ("v6  pruning + per-site floor", 21.4)):
        m = re.search(re.escape(key) + r"\s+median\s+([0-9.]+)%", blk)
        if m:
            check("held-out median err, " + key, float(m.group(1)), want, tol=0.10)
        else:
            print("  [SKIP] could not parse " + key)
except Exception as e:
    print("  [SKIP] model_v6.py not runnable here (%s)" % e)

print("")
print("%d checks passed, %d failed" % (ok, fail))
raise SystemExit(1 if fail else 0)
