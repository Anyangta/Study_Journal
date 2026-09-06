#!/usr/bin/env python3
"""
Re-derive every number the paper quotes, from the raw run data.

QUOTED holds the value as it appears in paper_draft_en.md.  Each check recomputes
the same quantity from results/ and compares.  Change the text without re-running
the analysis and this fails; re-run the analysis and get a different number and
this fails too.  That is the point.

    python3 verify.py             # all checks
    python3 verify.py --update    # print a QUOTED block matching current data
"""
import json, math, os, sys
import numpy as np

ROOT = "/home/wontak/icngc2_stream"
RES = os.path.join(ROOT, "results")
sys.path.insert(0, ROOT)
from model import load_mu, tau_star_lag, tau_star_waste, tau_stable_max  # noqa: E402

REF = "(1024, 200000, 32000, 'rocksdb', False, False, 0.5)"

# ------------------------------------------------------------------ quoted
QUOTED = {
    "lambda":                 54811,
    "mu":                     108925,
    "rho":                    0.503,
    "mu_spread_pct":          17.0,
    "l0_ms":                  57.2,
    "delta_latency_s":        1.277,
    "delta_capacity_s":       0.221,
    "delta_ratio":            5.8,
    "D_eff_s":                4.11,
    "D_restore_s":            1.25,
    "mu_drain":               98000,
    "mu_drain_ratio":         0.90,
    "steady_fit_r2":          0.950,
    "area_fit_r2":            0.999,
    "area_slope_over_pred":   0.937,
    "peak_pred_r2":           0.998,
    "peak_pred_ratio":        1.14,
    "peak_pred_relerr":       0.146,
    "n_episodes":             36,
    "tau_star_55":            3.5,
    "tau_star_3600":          18.0,
    "tau_star_86400":         55.3,
    "daly_55":                4.9,
    "daly_3600":              39.9,
    "daly_86400":             195.3,
    "tau_stable_55":          72.0,
    "measured_argmin_55_s":   4.4,
    "steady_tau8_ms":         265,
    "steady_tau64_ms":        98,
    "unstable_tau128_lat_ms": 126281,
}

CHECKS = []


def check(name, tol=0.03):
    def deco(fn):
        CHECKS.append((name, fn, tol))
        return fn
    return deco


def fits():
    return json.load(open(os.path.join(RES, "fits.json")))


def ref():
    f = fits()
    if REF in f:
        return f[REF]
    for k, v in f.items():
        if k.startswith("(1024, 200000, 32000") and abs(v.get("rho", 9) - 0.5) < 0.06:
            return v
    raise SystemExit("reference workload not in fits.json")


def summary():
    return json.load(open(os.path.join(RES, "summary.json")))


def ref_runs():
    out = []
    for r in summary():
        if (r["payload"] == 1024 and r["keys"] == 200000 and r["spin"] == 32000
                and r["backend"] == "rocksdb" and not r["incremental"]
                and abs(r["lambda_meas"] / QUOTED["mu"] - 0.5) < 0.06):
            out.append(r)
    return out


# ------------------------------------------------------------------ checks
@check("offered load")
def c_lambda():
    return float(np.median([r["lambda_meas"] for r in ref_runs()])), QUOTED["lambda"]


@check("calibrated service rate")
def c_mu():
    return load_mu()[(1024, 200000, 32000)], QUOTED["mu"]


@check("utilisation")
def c_rho():
    return ref()["rho"], QUOTED["rho"]


@check("service rate spread across an 8x state range")
def c_mu_spread():
    mu = load_mu()
    v = [mu[(1024, k, 32000)] for k in (100000, 200000, 400000, 800000)]
    return 100.0 * (max(v) - min(v)) / max(v), QUOTED["mu_spread_pct"]


@check("latency floor l0")
def c_l0():
    return ref()["l0_s"] * 1000, QUOTED["l0_ms"]


@check("equivalent stall from the latency curve")
def c_delta_lat():
    return ref()["delta_s"], QUOTED["delta_latency_s"]


@check("capacity lost per checkpoint")
def c_delta_cap():
    return ref()["delta_capacity_s"], QUOTED["delta_capacity_s"]


@check("ratio between the two checkpoint costs", tol=0.05)
def c_delta_ratio():
    return ref()["delta_ratio"], QUOTED["delta_ratio"]


@check("effective outage a record sees")
def c_deff():
    return ref()["D_s"], QUOTED["D_eff_s"]


@check("outage the engine reports")
def c_drestore():
    return ref()["D_restore_s"], QUOTED["D_restore_s"]


@check("catch-up service rate", tol=0.05)
def c_mudrain():
    return ref()["mu_drain"], QUOTED["mu_drain"]


@check("catch-up rate as a fraction of nominal", tol=0.05)
def c_mudrain_ratio():
    return ref()["mu_drain_ratio"], QUOTED["mu_drain_ratio"]


@check("steady-state fit quality")
def c_steady_r2():
    return ref()["steady_r2"], QUOTED["steady_fit_r2"]


@check("episode area fit quality", tol=0.005)
def c_area_r2():
    return ref()["area_r2"], QUOTED["area_fit_r2"]


@check("fitted area slope against the parameter-free one")
def c_area_slope():
    return ref()["area_slope_ratio"], QUOTED["area_slope_over_pred"]


@check("parameter-free episode prediction, fit quality", tol=0.005)
def c_peak_r2():
    return ref()["peak_pred_r2"], QUOTED["peak_pred_r2"]


@check("parameter-free episode prediction, median ratio")
def c_peak_ratio():
    return ref()["peak_pred_median_ratio"], QUOTED["peak_pred_ratio"]


@check("parameter-free episode prediction, median relative error", tol=0.06)
def c_peak_relerr():
    return ref()["peak_pred_median_relerr"], QUOTED["peak_pred_relerr"]


@check("episodes in the reference fit", tol=0.0)
def c_neps():
    return ref()["n_episodes"], QUOTED["n_episodes"]


def _taus(M):
    f = ref()
    dcap = f.get("delta_capacity_s") or f["delta_s"]
    return (tau_star_lag(f["delta_s"], f["D_s"], f["rho"], M, f.get("rho_d")),
            math.sqrt(2 * dcap * M))


@check("latency optimum at M = 55 s", tol=0.05)
def c_ts55():
    return _taus(55)[0], QUOTED["tau_star_55"]


@check("latency optimum at M = 1 h", tol=0.05)
def c_ts3600():
    return _taus(3600)[0], QUOTED["tau_star_3600"]


@check("latency optimum at M = 1 day", tol=0.05)
def c_ts86400():
    return _taus(86400)[0], QUOTED["tau_star_86400"]


@check("classical rule at M = 55 s", tol=0.05)
def c_d55():
    return _taus(55)[1], QUOTED["daly_55"]


@check("classical rule at M = 1 h", tol=0.05)
def c_d3600():
    return _taus(3600)[1], QUOTED["daly_3600"]


@check("classical rule at M = 1 day", tol=0.05)
def c_d86400():
    return _taus(86400)[1], QUOTED["daly_86400"]


@check("interval above which catch-up never finishes", tol=0.05)
def c_stable():
    f = ref()
    mu_eff = f.get("mu_drain") or f["mu"]
    return tau_stable_max(f["D_s"], f["rho"], mu_eff, f["lambda"], 55), QUOTED["tau_stable_55"]


@check("interval of the best measured run at M = 55 s", tol=0.05)
def c_argmin():
    rs = [r for r in ref_runs() if r.get("_mtbf_run") == 55
          and r["mean_lat_ms"] == r["mean_lat_ms"]]
    if not rs:
        return float("nan"), QUOTED["measured_argmin_55_s"]
    best = min(rs, key=lambda r: r["mean_lat_ms"])
    return best["ckpt_gap_s"], QUOTED["measured_argmin_55_s"]


def _steady_at(ck_ms):
    rs = [r for r in ref_runs() if r["ckpt_ms"] == ck_ms and r.get("_mtbf_run") == 55]
    return rs[0]["steady_lat_ms"] if rs else float("nan")


@check("steady latency at tau = 8 s", tol=0.05)
def c_s8():
    return _steady_at(8000), QUOTED["steady_tau8_ms"]


@check("steady latency at tau = 64 s", tol=0.05)
def c_s64():
    return _steady_at(64000), QUOTED["steady_tau64_ms"]


@check("mean latency of the unstable tau = 128 s run", tol=0.05)
def c_unstable():
    rs = [r for r in ref_runs() if r["ckpt_ms"] == 128000 and r.get("_mtbf_run") == 55]
    return (rs[0]["mean_lat_ms"] if rs else float("nan")), QUOTED["unstable_tau128_lat_ms"]


# ------------------------------------------------------------------ main
def main():
    update = "--update" in sys.argv
    vals, npass, nfail = {}, 0, 0
    for name, fn, tol in CHECKS:
        try:
            measured, quoted = fn()
        except Exception as e:
            print(f"[ERROR] {name}: {type(e).__name__}: {e}")
            nfail += 1
            continue
        vals[name] = measured
        if measured != measured:
            print(f"[SKIP ] {name}: not measurable from current data")
            continue
        ok = abs(measured - quoted) <= max(tol * abs(quoted), 1e-9)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: measured {measured:.6g}, "
              f"paper says {quoted:.6g}")
        npass += ok
        nfail += (not ok)
    print(f"\n{npass}/{npass + nfail} checks pass")
    if update:
        print("\n# current values, for pasting into QUOTED:")
        for name, fn, _ in CHECKS:
            if name in vals and vals[name] == vals[name]:
                print(f"#   {name}: {vals[name]:.6g}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
