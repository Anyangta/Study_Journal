#!/usr/bin/env python3
"""Cost model v6: pruned scans with a per-site floor, expressed only in
quantities the trials actually measured (no Parquet footer needed).

v4 modelled zone-map pruning on both sites and halved the median error, but it
overshoots badly at low selectivity because pruning cannot drive a scan to zero.
v6 gives each site a floor.

Calibration is read off ONE condition (10gbit_full, cores_b=4) by regressing
measured per-query seconds on sigma: intercept = floor, slope = full-scan cost.
Nothing is fitted against k*.  Every other condition is held out.
"""
import numpy as np, pandas as pd

d = pd.read_csv("results_final.csv")
FIT_ON = "10gbit_full"
FIT_CORES = 4
DELAY = {"10gbit_native": 0.0, "10gbit_full": 0.0, "1gbit_10ms": .010,
         "1gbit_50ms": .050, "1gbit_150ms": .150, "500mbit_50ms": .050,
         "100mbit_50ms": .050}
SIG7 = [0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]
SIG3 = [0.001, 0.1, 1.0]


def meas(x, plan, s, c):
    q = x[(x.plan == plan) & (x.sigma_target == s)]
    if plan != "FULL_SHIP":
        q = q[q.cores_b == c]
    return None if q.empty else (q.setup_s.median(), q.pq_s_med.median(),
                                 q.setup_bytes.median(), q.pq_bytes.median())


def goodput(x):
    sh = x[x.plan == "FULL_SHIP"]
    return float((sh.setup_bytes / sh.setup_s).median())


# ---- object sizes, straight from the trials -------------------------------
S_WIRE = float(d[d.plan == "FULL_SHIP"].setup_bytes.median())          # whole object
PROJ_WIRE = float(d[d.plan == "PROJECT_ONLY"].setup_bytes.median())    # projected cols
print(f"object {S_WIRE/2**20:.1f} MiB, projected {PROJ_WIRE/2**20:.1f} MiB "
      f"(pi = {PROJ_WIRE/S_WIRE:.3f})")

# ---- read the floors and scan costs off the held-in condition --------------
fit = d[d.label == FIT_ON]
B_fit = goodput(fit)
ls, lt, rs, rt = [], [], [], []
for s in SIG7:
    r, p = meas(fit, "PROJECT_ONLY", s, FIT_CORES), meas(fit, "FULL_PUSH", s, FIT_CORES)
    if r:
        ls.append(s); lt.append(r[1])
    if p:
        rs.append(s); rt.append(p[1])
slope_a, floor_a = np.polyfit(ls, lt, 1)
slope_b_tot, floor_b = np.polyfit(rs, rt, 1)
scan_b_fit = slope_b_tot - PROJ_WIRE / B_fit      # strip the transfer part out
setup_fit = float(fit[fit.plan == "PROJECT_ONLY"].setup_s.median())
setup_scan_fit = setup_fit - PROJ_WIRE / B_fit    # B's unpruned read for the projection

print(f"calibrated on {FIT_ON} (cores_b={FIT_CORES}), all else held out:")
print(f"  site A  floor {floor_a*1000:6.1f} ms   full local scan {slope_a*1000:8.1f} ms")
print(f"  site B  floor {floor_b*1000:6.1f} ms   full remote scan {scan_b_fit*1000:8.1f} ms")
print(f"  site B  unpruned projection read {setup_scan_fit*1000:8.1f} ms")


def kstar(rep, push):
    den = push[1] - rep[1]
    return (rep[0] - push[0]) / den if den > 0 else np.inf


rows = []
for lab in DELAY:
    x = d[d.label == lab]
    if x.empty:
        continue
    B = goodput(x)
    sigs = SIG7 if lab.endswith("_full") else SIG3
    for c in (1, 2, 4, 8):
        k_scale = FIT_CORES / c                       # scan time scales with cores
        for s in sigs:
            p, r = meas(x, "FULL_PUSH", s, c), meas(x, "PROJECT_ONLY", s, c)
            if not (p and r):
                continue
            # v1: no pruning anywhere
            v1_r = (setup_scan_fit * k_scale + PROJ_WIRE / B, slope_a)
            v1_p = (0.0, scan_b_fit * k_scale + s * PROJ_WIRE / B)
            # v4: both predicate-bearing scans prune, no floor
            v4_r = (setup_scan_fit * k_scale + PROJ_WIRE / B, s * slope_a)
            v4_p = (0.0, s * scan_b_fit * k_scale + s * PROJ_WIRE / B)
            # v6: same, plus a floor per site; the remote floor carries the delay
            v6_r = (setup_scan_fit * k_scale + PROJ_WIRE / B, floor_a + s * slope_a)
            v6_p = (0.0, floor_b + DELAY[lab] + s * scan_b_fit * k_scale
                    + s * PROJ_WIRE / B)
            rows.append(dict(label=lab, held_out=(lab != FIT_ON), cores_b=c, sigma=s,
                             k_meas=kstar(r, p), k_v1=kstar(v1_r, v1_p),
                             k_v4=kstar(v4_r, v4_p), k_v6=kstar(v6_r, v6_p)))

t = pd.DataFrame(rows)
t.to_csv("kstar_v6.csv", index=False)


def report(sub, title):
    fin = sub[np.isfinite(sub.k_meas) & (sub.k_meas > 0)]
    if fin.empty:
        return
    print(f"\n{title}  (n={len(fin)})")
    for col, name in (("k_v1", "v1  no pruning"), ("k_v4", "v4  pruning"),
                      ("k_v6", "v6  pruning + per-site floor")):
        e = ((fin[col] - fin.k_meas).abs() / fin.k_meas).replace(
            [np.inf, -np.inf], np.nan).dropna()
        if len(e):
            print(f"  {name:<32} median {e.median():7.1%}  mean {e.mean():9.1%}  n={len(e)}")


report(t, "all matched configurations")
report(t[t.held_out], "HELD OUT only (calibration never saw these)")
