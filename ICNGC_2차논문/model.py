#!/usr/bin/env python3
"""
Fit the checkpoint-interval model from measured runs.

Latency objective, as a record actually experiences it.  After a restart the
oldest surviving record is (a + D) seconds old, where a is the age of the last
completed checkpoint and D the effective outage; the backlog lam(a+D) drains at
(mu_d - lam), and the records that come out during that drain are served at
mu_d, not lam.  So the excess latency summed over records is
    A = mu_d lam E[(a+D)^2] / (2 (mu_d - lam))
and, with a ~ U(0, tau) so E[(a+D)^2] = tau^2/3 + tau D + D^2,

    Lbar(tau) = l0 + delta^2 / (2 (1-rho) tau)
                   + mu_d E[(a+D)^2] / (2 (mu_d - lam) M)

    d/dtau = 0  ->  delta^2 M (1-rho_d) / (1-rho) = tau^2 (2 tau/3 + D)
                    with rho_d = lam / mu_d
    D << tau  ->   tau* ~ (1.5 delta^2 M)^(1/3)           [cube root]
    D >> tau  ->   tau* ~ delta sqrt(M / D)               [square root]

Wasted-capacity objective (the classical one):
    W(tau) = delta/tau + (D + rho tau/2)/M
    d/dtau = 0   ->   tau* = sqrt(2 delta M / rho)        [Young/Daly form]
"""
import json, math, os, re, sys
import numpy as np

ROOT = "/home/wontak/icngc2_stream"
RES = os.path.join(ROOT, "results")
# Fewest checkpoints a run may contain and still be said to have a steady
# level.  A window holding two or three checkpoint cycles does not average to
# a level, it reports whichever spike happened to land in it.
MIN_CKPT = 5


def load_mu():
    mu = {}
    for f in sorted(os.listdir(os.path.join(ROOT, "results")) if os.path.isdir(os.path.join(ROOT, "results")) else []):
        if not (f.startswith("calib") and f.endswith(".json")):
            continue
        p = os.path.join(RES, f)
        for r in json.load(open(p)):
            if "mu_median" in r:
                key = (r["payload"], r["keys"], r["spin"], r.get("backend", "rocksdb"))
                mu[key] = float(r["mu_median"])
    return mu


def lstsq(x, y):
    x = np.asarray(x, float); y = np.asarray(y, float)
    A = np.column_stack([x, np.ones_like(x)])
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    pred = A @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")
    return float(coef[0]), float(coef[1]), r2


def tau_star_lag(delta, D, rho, M, rho_d=None):
    """solve delta^2 M (1-rho_d)/(1-rho) = tau^2 (2 tau/3 + D) for tau > 0"""
    if delta <= 0 or M <= 0:
        return float("nan")
    rd = rho if rho_d is None else rho_d
    k = (1 - rd) / (1 - rho) if rho < 1 else 1.0
    f = lambda t: t * t * (2 * t / 3 + D) - k * delta * delta * M
    lo, hi = 1e-4, 1.0
    while f(hi) < 0 and hi < 1e6:
        hi *= 2
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if f(mid) < 0:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def tau_stable_max(D, rho, mu_eff, lam, M):
    """Largest interval whose expected catch-up still finishes inside one MTBF.
       backlog lam*(tau/2 + D) drained at (mu_eff - lam), plus the downtime."""
    if mu_eff <= lam:
        return 0.0
    r = lam / (mu_eff - lam)
    # r*(tau/2 + D) + D < M
    return max(0.0, 2.0 * ((M - D) / r - D))


def tau_star_waste(delta, rho, M):
    return math.sqrt(2 * delta * M / rho) if delta > 0 and M > 0 else float("nan")


def lbar_fitted(tau, f, M):
    """Mean latency composed from the two measured terms.  The checkpoint term
    uses the fitted l0 + s/tau; the failure term uses the fitted episode
    regression, so nothing here assumes the closed form is right."""
    lam = f["lambda"]
    steady = f["l0_s"] + f["ckpt_slope"] / tau
    D = f["D_s"]
    Ea2 = tau * tau / 3 + tau * D + D * D
    area = f["area_icept"] + f["area_slope"] * Ea2
    return steady + area / (lam * M)


def tau_star_empirical(f, M, lo=0.5, hi=600.0, n=4000):
    if any(f.get(k) != f.get(k) for k in ("l0_s", "ckpt_slope", "D_s",
                                          "area_icept", "area_slope")):
        return float("nan")
    tau = np.exp(np.linspace(math.log(lo), math.log(hi), n))
    y = np.array([lbar_fitted(t, f, M) for t in tau])
    return float(tau[int(np.argmin(y))])


def measured_argmin(runs, mtbf):
    """argmin of the mean latency actually measured, at the MTBF actually run"""
    pts = [(r["ckpt_gap_s"], r["mean_lat_ms"]) for r in runs
           if r.get("_mtbf_run") == mtbf and r["ckpt_gap_s"] == r["ckpt_gap_s"]
           and r["mean_lat_ms"] == r["mean_lat_ms"]]
    if len(pts) < 3:
        return None
    pts.sort()
    best = min(pts, key=lambda p: p[1])
    return {"tau": best[0], "lat_ms": best[1], "n_points": len(pts),
            "curve": pts}


def group_key(r):
    return (r["payload"], r["keys"], r["spin"], r["backend"],
            bool(r["incremental"]), bool(r.get("unaligned", False)),
            int(round(r["lambda_meas"] / 1000.0)))


def fit_group(runs, mu):
    """runs: list of per-run dicts from analyze.py for one workload+load point"""
    lam = float(np.median([r["lambda_meas"] for r in runs]))
    rho = lam / mu
    out = {"lambda": lam, "mu": mu, "rho": rho, "n_runs": len(runs)}

    # ---- steady term: mean latency between failures vs 1/tau
    # Two kinds of run are excluded from this regression and reported separately:
    #  * saturated: the interval is shorter than about twice the checkpoint's own
    #    duration, so checkpoints run back to back and the cost stops scaling as 1/tau
    #  * unstable: the recovery backlog outlives the gap to the next failure, so
    #    the run never returns to a steady level at all
    used, saturated, unstable, overloaded = [], [], [], []
    for r in runs:
        g, st = r.get("ckpt_gap_s"), r.get("steady_lat_ms")
        if g != g or st != st or g <= 0:
            continue
        d = (r.get("ckpt_dur_ms") or 0) / 1000.0
        if r.get("overloaded"):
            overloaded.append((g, st / 1000.0, r["run_id"]))
            unstable.append((g, st / 1000.0, r["run_id"]))
        elif r.get("steady_bins", 99) < 12 or st > 5000:
            unstable.append((g, st / 1000.0, r["run_id"]))
        elif d > 0 and g < 2.0 * d:
            saturated.append((g, st / 1000.0, r["run_id"]))
        else:
            used.append((g, st / 1000.0, r["run_id"],
                         r.get("n_ckpt") or 0, r.get("n_failures") or 0, d))
    out["n_saturated"] = len(saturated)
    out["n_unstable"] = len(unstable)
    out["n_overloaded"] = len(overloaded)
    out["overloaded_runs"] = [x[2] for x in overloaded]
    out["keep_up_curve"] = sorted(
        (round(r.get("ckpt_gap_s") or 0, 2), round(r.get("keep_up") or 0, 3))
        for r in runs if r.get("keep_up") == r.get("keep_up"))
    out["saturated_runs"] = [x[2] for x in saturated]
    out["unstable_runs"] = [x[2] for x in unstable]
    out["steady_curve"] = sorted([(round(x[0], 2), round(x[1] * 1000, 1)) for x in used + saturated + unstable])
    # The tau = 128 s no-failure run holds three checkpoints, an sd 2.6x its own
    # mean and a 22 s maximum; on its own it flipped the sign of the fit.
    thin = [x for x in used if x[3] < MIN_CKPT]
    used = [x for x in used if x[3] >= MIN_CKPT]
    out["n_thin"] = len(thin)
    out["thin_runs"] = [x[2] for x in thin]

    # The steady term wants runs in which a steady level exists at all.  At
    # M = 55 s the job fails every 55 s, recovers for ~8 s and checkpoints for
    # another 5-9 s, so almost nothing is left in between and the level being
    # regressed is mostly recovery.  The no-failure runs measure the same term
    # with none of that in the way, so they are used alone whenever there are
    # enough of them.
    clean = [x for x in used if x[4] == 0]
    if len(clean) >= 3:
        used, out["steady_from"] = clean, "m0"
    else:
        out["steady_from"] = "all"

    # A checkpoint that takes 9 s does not cost what one taking 2.5 s costs, so
    # runs measured while the store was delivering very different throughput are
    # not measuring the same delta and pooling them fits a line through two
    # different systems.  This is not hypothetical: the reference arm holds
    # failure-free runs from a night at ~200 MB/s and from one at ~65 MB/s, and
    # together they gave delta = 0.  Keep the largest set of runs whose own
    # checkpoint durations sit inside one 1.5x band, and say which band it was.
    # Which band to keep is not decided by size, and not by how little the runs
    # were perturbed either: both of those pick the 9 s-checkpoint night here,
    # whose own control runs disagree with each other by 2.2x.  Take the band in
    # which a 1/tau relation is actually resolvable -- the one whose regression
    # is best determined -- and report how many bands there were, so a reader
    # knows a choice was made.  n_bands > 1 is a signal to slice the runs by
    # hand with drift.py rather than trust this row.
    band = [x for x in used if x[5] and x[5] == x[5]]
    if len(band) >= 3:
        band.sort(key=lambda x: x[5])
        cands = []
        for i in range(len(band)):
            j = i
            while j < len(band) and band[j][5] <= 1.5 * band[i][5]:
                j += 1
            if j - i >= 3:
                grp = band[i:j]
                _, _, r2g = lstsq([1.0 / x[0] for x in grp], [x[1] for x in grp])
                cands.append((r2g if r2g == r2g else -1.0, len(grp), grp))
        out["n_bands"] = len(cands)
        best = max(cands, key=lambda c: (c[0], c[1]))[2] if cands else []
        if len(best) >= 3 and len(best) < len(used):
            out["ckdur_band_s"] = [round(best[0][5], 2), round(best[-1][5], 2)]
            out["n_offband"] = len(used) - len(best)
            used = best
        else:
            out["ckdur_band_s"] = [round(band[0][5], 2), round(band[-1][5], 2)]
            out["n_offband"] = 0

    pts = [(a, b) for a, b, *_ in used]
    if len(pts) >= 3:
        inv = [1.0 / p[0] for p in pts]
        lat = [p[1] for p in pts]
        slope, icept, r2 = lstsq(inv, lat)
        slope = max(slope, 0.0)
        delta = math.sqrt(2 * slope * (1 - rho)) if slope > 0 else 0.0
        out.update({"l0_s": icept, "ckpt_slope": slope, "steady_r2": r2,
                    "delta_s": delta, "n_steady_pts": len(pts)})
        # the same checkpoint, costed the other way: capacity actually lost.
        # A checkpoint delays far more records than it drops, so these differ.
        dd = [r["delta_direct_s"] for r in runs
              if r.get("delta_direct_s") == r.get("delta_direct_s")
              and r.get("ckpt_gap_s", 0) >= 2.0 * (r.get("ckpt_dur_ms") or 0) / 1000.0]
        out["delta_capacity_s"] = float(np.median(dd)) if dd else float("nan")
        out["delta_ratio"] = (delta / out["delta_capacity_s"]
                              if out["delta_capacity_s"] == out["delta_capacity_s"]
                              and out["delta_capacity_s"] > 0 else float("nan"))
    else:
        out.update({"l0_s": float("nan"), "delta_s": float("nan"),
                    "steady_r2": float("nan"), "n_steady_pts": len(pts)})

    # ---- failure term: pooled episodes, area vs (age + D_eff)^2
    # Episodes are only measurable against a steady level that exists.  In an
    # unstable run the job never returns to one; in a saturated run (interval
    # below the checkpoint's own duration) the level is itself inflated by
    # back-to-back checkpointing, so the "excess" understates the real cost.
    bad = set(out.get("unstable_runs", [])) | set(out.get("saturated_runs", []))
    eps = []
    for r in runs:
        if r["run_id"] in bad:
            continue
        for e in r.get("episodes", []):
            if e.get("overlapped") or e.get("truncated"):
                continue
            if (e.get("ckpt_age_s") is not None and e.get("D_eff_s") is not None
                    and e.get("lag_area_rec_s") is not None
                    and e["lag_area_rec_s"] == e["lag_area_rec_s"]
                    and e["lag_area_rec_s"] > 0):
                eps.append((e["ckpt_age_s"], e["D_eff_s"], e["lag_area_rec_s"],
                            e.get("mu_drain"), e.get("peak_lat_ms"),
                            (e.get("detect_restore_ms") or 0) / 1000.0))
    out["n_episodes"] = len(eps)
    if eps:
        D = float(np.median([e[1] for e in eps]))
        out["D_s"] = D
        out["D_eff_sd"] = float(np.std([e[1] for e in eps]))
        out["D_restore_s"] = float(np.median([e[5] for e in eps]))
        x = np.array([(e[0] + e[1]) ** 2 for e in eps])
        y = np.array([e[2] for e in eps])
        slope, icept, r2 = lstsq(x, y)
        out.update({"area_slope": slope, "area_icept": icept, "area_r2": r2})
        # the drain rate right after a restore is not the steady-state mu: the
        # restored state backend is cold, so catch-up runs slower than nominal
        dr = [e[3] for e in eps if e[3]]
        mu_d = float(np.median(dr)) if dr else float("nan")
        out["mu_drain"] = mu_d
        out["mu_drain_ratio"] = mu_d / mu if mu_d == mu_d else float("nan")
        out["rho_d"] = lam / mu_d if mu_d == mu_d and mu_d > 0 else float("nan")
        # record-weighted: mu_d lam / (2 (mu_d - lam)), not lam^2 / (...)
        pred_nom = mu * lam / (2 * (mu - lam)) if mu > lam else float("nan")
        pred_drain = (mu_d * lam / (2 * (mu_d - lam))
                      if mu_d == mu_d and mu_d > lam else float("nan"))
        out["area_slope_pred_nominal"] = pred_nom
        out["area_slope_pred_drain"] = pred_drain
        out["area_slope_pred"] = pred_drain if pred_drain == pred_drain else pred_nom
        ps = out["area_slope_pred"]
        out["area_slope_ratio"] = slope / ps if ps == ps and ps > 0 else float("nan")
        # parameter-free episode check: the backlog decays from lam*peak at
        # (mu_drain - lam), so its area is lam^2 peak^2 / (2 (mu_drain - lam)),
        # with peak and mu_drain both measured on that same episode
        pk_pred, pk_meas = [], []
        for a_, d_, ar_, md_, pkms_, _dr in eps:
            if not md_ or not pkms_ or md_ <= lam:
                continue
            pk = pkms_ / 1000.0
            pk_pred.append(md_ * lam * pk * pk / (2 * (md_ - lam)))
            pk_meas.append(ar_)
        if len(pk_pred) >= 5:
            pp, pm = np.array(pk_pred), np.array(pk_meas)
            out["peak_pred_n"] = len(pp)
            out["peak_pred_median_ratio"] = float(np.median(pm / pp))
            out["peak_pred_median_relerr"] = float(np.median(np.abs(pp - pm) / pm))
            sl, ic, r2p = lstsq(pp, pm)
            out["peak_pred_r2"] = r2p
            out["peak_pred_slope"] = sl
        for tag, psl in (("nominal", pred_nom), ("drain", pred_drain)):
            if psl != psl:
                continue
            pred = psl * x
            rel = np.abs(pred - y) / np.maximum(y, 1e-9)
            out[f"area_relerr_{tag}"] = float(np.median(rel))
        out["area_pred_median_relerr"] = out.get("area_relerr_drain",
                                                 out.get("area_relerr_nominal", float("nan")))
    else:
        out.update({"D_s": float("nan"), "area_slope": float("nan"),
                    "area_slope_pred": float("nan"), "area_r2": float("nan")})
    return out


def main():
    rows = json.load(open(os.path.join(RES, "summary.json")))
    mu_tab = load_mu()
    groups = {}
    for r in rows:
        if r.get("lambda_meas") != r.get("lambda_meas"):
            continue
        m = re.search(r"_m(\d+)", r["run_id"])
        r["_mtbf_run"] = int(m.group(1)) if m else None
        k = (r["payload"], r["keys"], r["spin"], r["backend"],
             bool(r["incremental"]), bool(r.get("unaligned", False)))
        mu = mu_tab.get((r["payload"], r["keys"], r["spin"], r["backend"]))
        if not mu:
            continue
        rho_b = round(r["lambda_meas"] / mu, 1)
        groups.setdefault(k + (rho_b,), []).append(r)

    fits = {}
    print(f"{'workload':>30} {'rho':>5} {'n':>3} {'l0_ms':>7} {'delta_s':>8} {'D_s':>6} "
          f"{'R2_st':>6} {'ep':>4} {'areaR2':>7} {'s/pred':>7} {'relerr':>7} "
          f"{'mu_dr':>7} {'dr/mu':>6}")
    print("  delta_s: equivalent stall inferred from the latency curve; "
          "delta_cap: capacity actually lost per checkpoint")
    for k, runs in sorted(groups.items()):
        mu = mu_tab[(k[0], k[1], k[2], k[3])]
        f = fit_group(runs, mu)
        f["workload"] = k
        f["_runs"] = runs
        fits[str(k)] = f
        name = f"p{k[0]}_k{k[1]//1000}k_s{k[2]}_{k[3]}{'_inc' if k[4] else ''}{'_una' if k[5] else ''}"
        print(f"{name:>30} {f['rho']:>5.2f} {f['n_runs']:>3} "
              f"{f.get('l0_s', float('nan'))*1000:>7.1f} {f.get('delta_s', float('nan')):>8.3f} "
              f"{f.get('D_s', float('nan')):>6.2f} {f.get('steady_r2', float('nan')):>6.3f} "
              f"{f.get('n_episodes', 0):>4} {f.get('area_r2', float('nan')):>7.3f} "
              f"{f.get('area_slope_ratio', float('nan')):>7.3f} "
              f"{f.get('area_pred_median_relerr', float('nan')):>7.3f} "
              f"{f.get('mu_drain', float('nan'))/1000:>7.1f} {f.get('mu_drain_ratio', float('nan')):>6.2f}"
              f" | Dres={f.get('D_restore_s', float('nan')):.2f} "
              f"pkR2={f.get('peak_pred_r2', float('nan')):.3f} "
              f"pkratio={f.get('peak_pred_median_ratio', float('nan')):.2f} "
              f"| cap={f.get('delta_capacity_s', float('nan')):.3f} "
              f"x{f.get('delta_ratio', float('nan')):.1f} "
              f"sat={f.get('n_saturated', 0)} uns={f.get('n_unstable', 0)} "
              f"ovl={f.get('n_overloaded', 0)} thin={f.get('n_thin', 0)} "
              f"off={f.get('n_offband', 0)} bands={f.get('n_bands', 1)} src={f.get('steady_from', '-')} "
              f"ckdur={f.get('ckdur_band_s', '-')}")

    # ---- optimal interval versus MTBF, both objectives
    print("\n  optimal checkpoint interval (s) versus MTBF")
    Ms = [30, 60, 120, 300, 600, 1800, 3600, 21600, 86400]
    for k, f in sorted(fits.items(), key=lambda kv: kv[1]["workload"]):
        if f.get("delta_s") != f.get("delta_s") or f.get("D_s") != f.get("D_s"):
            continue
        w = f["workload"]
        name = f"p{w[0]}_k{w[1]//1000}k rho={f['rho']:.2f}"
        print(f"\n  {name}   delta={f['delta_s']:.3f}s  D={f['D_s']:.2f}s")
        print(f"    {'M(s)':>8} {'tau*_lag':>9} {'tau*_waste':>11} {'Daly':>8} {'ratio':>7}")
        for M in Ms:
            tl = tau_star_lag(f["delta_s"], f["D_s"], f["rho"], M, f.get("rho_d"))
            dcap = f.get("delta_capacity_s")
            dcap = dcap if dcap == dcap else f["delta_s"]
            tw = tau_star_waste(dcap, f["rho"], M)
            td = math.sqrt(2 * dcap * M)
            print(f"    {M:>8} {tl:>9.2f} {tw:>11.2f} {td:>8.2f} {tw/tl:>7.2f}")

    # ---- measurement-driven optimum vs the two closed forms
    print("\n  measurement-driven optimum (fitted terms, no closed form assumed)")
    for k, f in sorted(fits.items(), key=lambda kv: str(kv[1]["workload"])):
        if f.get("delta_s") != f.get("delta_s"):
            continue
        w = f["workload"]
        print(f"\n  p{w[0]}_k{w[1]//1000}k rho={f['rho']:.2f}"
              f"  delta={f['delta_s']:.3f}s D={f['D_s']:.2f}s")
        print(f"    {'M(s)':>8} {'tau*_emp':>9} {'tau*_model':>11} {'tau*_waste':>11} "
              f"{'Daly':>8} {'waste/emp':>10} {'tau_stable':>11}")
        for M in [30, 55, 150, 300, 600, 1800, 3600, 86400]:
            te = tau_star_empirical(f, M)
            tm_ = tau_star_lag(f["delta_s"], f["D_s"], f["rho"], M, f.get("rho_d"))
            dcap = f.get("delta_capacity_s")
            dcap = dcap if dcap == dcap else f["delta_s"]
            tw = tau_star_waste(dcap, f["rho"], M)
            td = math.sqrt(2 * dcap * M)
            mu_eff = f.get("mu_drain") or f["mu"]
            ts = tau_stable_max(f["D_s"], f["rho"], mu_eff, f["lambda"], M)
            print(f"    {M:>8} {te:>9.2f} {tm_:>11.2f} {tw:>11.2f} {td:>8.2f} "
                  f"{tw/te if te == te and te > 0 else float('nan'):>10.2f} {ts:>11.1f}")
        for mtbf in sorted({r.get("_mtbf_run") for r in f["_runs"]} - {None, 0}):
            mm = measured_argmin(f["_runs"], mtbf)
            if mm:
                te = tau_star_empirical(f, mtbf)
                print(f"    measured at M={mtbf}s: argmin tau={mm['tau']:.1f}s "
                      f"({mm['n_points']} points, best {mm['lat_ms']:.0f} ms)"
                      f"  vs fitted {te:.1f}s")

    for f in fits.values():
        f.pop("_runs", None)
    json.dump(fits, open(os.path.join(RES, "fits.json"), "w"), indent=1, default=str)
    print(f"\nwrote {RES}/fits.json")


if __name__ == "__main__":
    main()
