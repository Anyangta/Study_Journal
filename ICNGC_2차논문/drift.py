#!/usr/bin/env python3
"""Attach the box's checkpoint write throughput to every run, and refit the
steady term on a chosen slice of runs.

Why this exists: on this shared machine the checkpoint write throughput swings
about 4x over the day (measured over the JobManager log: ~250 MB/s at 03:00,
~66 MB/s at 17:00) while the checkpoint *size* stays put.  A sweep that walks
tau upward in wall-clock order therefore tilts the very latency-vs-1/tau curve
it is fitting.  This tool makes that visible per run, and lets the steady fit
be redone on a window where the box was not drifting.

  drift.py table                 -- every run: time, tau, steady, MB/s
  drift.py fit <sel> [sel...]    -- refit delta on runs whose id contains sel
"""
import json, math, os, re, sys, glob, datetime
import numpy as np

ROOT = "/home/wontak/icngc2_stream"
MIN_CKPT = 5   # fewest checkpoints a run may have and still give a steady level
JMLOG = os.path.join(ROOT, "flink-1.20.5/log/flink-wontak-standalonesession-0-cluster04.log")

CKPT = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ INFO.*"
                  r"Completed checkpoint \d+ for job \w+ "
                  r"\((\d+) bytes, checkpointDuration=(\d+) ms")


def load_checkpoints():
    """(epoch_seconds, bytes, duration_ms) for every completed checkpoint"""
    out = []
    for path in sorted(glob.glob(JMLOG + "*")):
        try:
            fh = open(path, errors="ignore")
        except OSError:
            continue
        for line in fh:
            m = CKPT.match(line)
            if not m:
                continue
            t = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
            d = int(m.group(3))
            if d > 0:
                out.append((t, int(m.group(2)), d))
    out.sort()
    return out


def load_runs():
    runs = {}
    for f in glob.glob(os.path.join(ROOT, "results/*/meta.json")):
        try:
            d = json.load(open(f))
        except Exception:
            continue
        if d.get("t_start"):
            runs[d["run_id"]] = d
    summ = json.load(open(os.path.join(ROOT, "results/summary.json")))
    summ = summ if isinstance(summ, list) else summ.get("runs", summ)
    for r in summ:
        if r["run_id"] in runs:
            runs[r["run_id"]]["_s"] = r
    return runs


def throughput(cks, t0, t1):
    """median MB/s over the checkpoints that completed inside one run"""
    v = [b / 1e6 / (d / 1000.0) for t, b, d in cks if t0 <= t <= t1]
    return float(np.median(v)) if v else float("nan")


def table(runs, cks, sels):
    rows = []
    for rid, m in runs.items():
        if sels and not all_sel(rid, sels):
            continue
        s = m.get("_s", {})
        rows.append((m["t_start"], rid, s.get("ckpt_gap_s"), s.get("steady_lat_ms"),
                     s.get("ckpt_dur_ms"), throughput(cks, m["t_start"], m.get("t_end", 0))))
    rows.sort()
    print("%-42s %-12s %8s %10s %9s %8s" %
          ("run", "time", "tau_s", "steady_ms", "ckdur_ms", "MB/s"))
    for t, rid, tau, st, cd, thr in rows:
        print("%-42s %-12s %8s %10s %9s %8.0f" % (
            rid[:42], datetime.datetime.fromtimestamp(t).strftime("%m-%d %H:%M"),
            f"{tau:.1f}" if tau == tau and tau else "-",
            f"{st:.0f}" if st == st and st else "-",
            f"{cd:.0f}" if cd and cd == cd else "-", thr))
    return rows


def all_sel(rid, sels):
    return all(s in rid for s in sels)


def fit(runs, cks, sels):
    """the steady regression of model.py, on a chosen slice, with the same
    exclusions: a run whose interval is under twice its own checkpoint duration
    is saturated and cannot show 1/tau."""
    pts, dropped = [], []
    for rid, m in runs.items():
        if not all_sel(rid, sels):
            continue
        s = m.get("_s")
        if not s:
            continue
        tau, st, cd = s.get("ckpt_gap_s"), s.get("steady_lat_ms"), s.get("ckpt_dur_ms") or 0
        if not tau or tau != tau or not st or st != st:
            continue
        thr = throughput(cks, m["t_start"], m.get("t_end", 0))
        if st > 5000 or s.get("steady_bins", 99) < 12 or s.get("overloaded"):
            dropped.append((rid, tau, st, "unstable", thr)); continue
        if cd and tau < 2.0 * cd / 1000.0:
            dropped.append((rid, tau, st, "saturated", thr)); continue
        # A steady level averaged over two or three checkpoint cycles is not a
        # level, it is whichever spike happened to land in the window: the
        # tau=128 s no-failure run has n_ckpt=3, sd 2.6x its own mean and a
        # 22 s maximum, and on its own it flips the sign of the fit.
        if (s.get("n_ckpt") or 0) < MIN_CKPT:
            dropped.append((rid, tau, st, "few_ckpt", thr)); continue
        pts.append((tau, st / 1000.0, thr, rid))
    print(f"selector {sels}: {len(pts)} usable, {len(dropped)} dropped")
    for rid, tau, st, why, thr in sorted(dropped, key=lambda x: x[1]):
        print("   drop %-40s tau=%6.1f steady=%9.1f  %-9s %5.0f MB/s" % (rid[:40], tau, st, why, thr))
    if len(pts) < 3:
        print("   not enough points to fit")
        return
    pts.sort()
    x = np.array([1.0 / p[0] for p in pts])
    y = np.array([p[1] for p in pts])
    A = np.column_stack([x, np.ones_like(x)])
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = A @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")
    slope, icept = float(coef[0]), float(coef[1])
    rho = 0.5
    delta = math.sqrt(2 * max(slope, 0.0) * (1 - rho))
    print("   used points (tau, steady_ms, MB/s):")
    for tau, st, thr, rid in pts:
        print("      %7.1f %10.1f %7.0f   %s" % (tau, st * 1000, thr, rid[:46]))
    thrs = [p[2] for p in pts if p[2] == p[2]]
    print("   l0 = %.1f ms   slope = %.4f   R2 = %.4f   delta = %.3f s" %
          (icept * 1000, slope, r2, delta))
    print("   tau range %.1f - %.1f s  (1/tau spread %.0fx)" %
          (pts[0][0], pts[-1][0], pts[-1][0] / pts[0][0]))
    if thrs:
        print("   throughput across these runs: %.0f - %.0f MB/s (spread %.1fx)" %
              (min(thrs), max(thrs), max(thrs) / max(min(thrs), 1e-9)))


if __name__ == "__main__":
    cks = load_checkpoints()
    runs = load_runs()
    print(f"{len(cks)} checkpoints, {len(runs)} runs with meta\n")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "table"
    sels = sys.argv[2:]
    if cmd == "table":
        table(runs, cks, sels)
    elif cmd == "fit":
        fit(runs, cks, sels)
