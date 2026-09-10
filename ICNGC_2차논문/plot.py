#!/usr/bin/env python3
"""Figures for the ICNGC-2 paper.  Reads results/summary.json + results/fits.json."""
import json, math, os, sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, "/home/wontak/icngc2_stream")
from model import load_mu, tau_star_lag, tau_star_waste

ROOT = "/home/wontak/icngc2_stream"
RES = os.path.join(ROOT, "results")
FIG = os.path.join(ROOT, "figs")

# categorical slots, validated for CVD separation; every series also carries a
# distinct dash pattern and marker so the figures survive greyscale printing
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#4a3aa7"]
DASH = [(None, None), (5, 2), (1.5, 1.5), (7, 2, 1.5, 2), (3, 1.5, 1, 1.5)]
MARK = ["o", "s", "^", "D", "v"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d8d7d2"

plt.rcParams.update({
    "font.size": 8, "axes.labelsize": 8, "axes.titlesize": 8,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
    "axes.edgecolor": INK2, "axes.linewidth": 0.6,
    "xtick.color": INK2, "ytick.color": INK2,
    "axes.labelcolor": INK, "text.color": INK,
    "grid.color": GRID, "grid.linewidth": 0.5,
    "figure.dpi": 150, "savefig.dpi": 300, "savefig.bbox": "tight",
    "legend.frameon": False, "axes.spines.top": False, "axes.spines.right": False,
    "font.family": "DejaVu Sans",
})


def style(ax):
    ax.grid(True, which="major", axis="both", alpha=0.7)
    ax.set_axisbelow(True)


def save(fig, name):
    os.makedirs(FIG, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(FIG, f"{name}.{ext}"))
    plt.close(fig)
    print("wrote", name)


def Lbar(tau, f, M):
    """Mean latency composed from the two measured terms: the fitted steady
    curve, and the fitted per-episode area spread over one MTBF.  Same
    composition the model script reports, so the curve and the per-run points
    in fig 1 are the same quantity."""
    lam = f["lambda"]
    steady = f["l0_s"] + f["ckpt_slope"] / tau
    D = f["D_s"]
    Ea2 = tau * tau / 3 + tau * D + D * D
    area = max(0.0, f["area_icept"] + f["area_slope"] * Ea2)
    return steady + area / (lam * M)


def run_point(r, M, unstable):
    """A run's own measurement composed to a common MTBF: its steady level plus
    its own mean per-failure area spread over M.  Nothing borrowed from a fit."""
    if r["run_id"] in unstable:
        return None
    eps = [e for e in r.get("episodes", [])
           if not e.get("overlapped") and not e.get("truncated") and e.get("lag_area_rec_s")
           and e["lag_area_rec_s"] == e["lag_area_rec_s"] and e["lag_area_rec_s"] > 0]
    if not eps or r["ckpt_gap_s"] != r["ckpt_gap_s"]:
        return None
    area = float(np.mean([e["lag_area_rec_s"] for e in eps]))
    return r["ckpt_gap_s"], r["steady_lat_ms"] + 1000.0 * area / (r["lambda_meas"] * M)


def fig1_latency_vs_tau(fits, rows):
    """the optimum exists, and it moves with the failure rate"""
    key = pick_ref(fits)
    if key is None:
        return
    f = fits[key]
    unstable = set(f.get("unstable_runs", [])) | set(f.get("saturated_runs", []))
    w = f["workload"]
    if isinstance(w, str):
        w = eval(w)
    mine = [r for r in rows if r["payload"] == w[0] and r["keys"] == w[1]
            and r["spin"] == w[2] and abs(r["lambda_meas"] / f["mu"] - f["rho"]) < 0.06
            and r.get("n_failures", 0) > 0]

    tau = np.logspace(np.log10(1), np.log10(300), 500)
    fig, ax = plt.subplots(figsize=(3.4, 2.6))
    Ms = [55, 600, 3600]
    for i, M in enumerate(Ms):
        y = np.array([Lbar(t, f, M) for t in tau]) * 1000
        ax.plot(tau, y, color=C[i], lw=1.5, dashes=DASH[i], zorder=2,
                label=f"$M$ = {M} s" if M < 3600 else "$M$ = 1 h")
        j = int(np.argmin(y))
        ax.plot(tau[j], y[j], marker=MARK[i], ms=6, color=C[i],
                mec="#fcfcfb", mew=1.0, ls="none", zorder=4)
    pts = [run_point(r, Ms[0], unstable) for r in mine]
    pts = sorted(p for p in pts if p)
    if pts:
        ax.plot([p[0] for p in pts], [p[1] for p in pts], "o", ms=4.5,
                color=INK, mfc="none", mew=1.0, ls="none", zorder=5,
                label=f"measured, $M$ = {Ms[0]} s")
    # what the classical rule would pick, costed with the capacity delta
    dcap = f.get("delta_capacity_s") or f["delta_s"]
    td = math.sqrt(2 * dcap * Ms[0])
    ax.axvline(td, color=INK2, lw=0.9, dashes=(1.5, 1.5), zorder=1)
    ax.annotate("Young/Daly", xy=(td, ax.get_ylim()[0]), xytext=(3, 4),
                textcoords="offset points", fontsize=6.5, color=INK2, rotation=90,
                va="bottom")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("checkpoint interval $\\tau$ (s)")
    ax.set_ylabel("mean end-to-end latency (ms)")
    style(ax)
    ax.legend(loc="upper left", ncol=1, handlelength=2.2, borderpad=0.2,
              labelspacing=0.25)
    save(fig, "fig1_latency_vs_tau")


def fig2_scaling(fits, rows=None):
    """the two objectives scale differently in MTBF, and there is a ceiling"""
    key = pick_ref(fits)
    if key is None:
        return
    f = fits[key]
    from model import tau_stable_max
    Ms = np.logspace(np.log10(20), np.log10(200000), 300)
    dcap = f.get("delta_capacity_s") or f["delta_s"]
    tl = np.array([tau_star_lag(f["delta_s"], f["D_s"], f["rho"], M, f.get("rho_d"))
                   for M in Ms])
    tw = np.array([tau_star_waste(dcap, f["rho"], M) for M in Ms])
    mu_eff = f.get("mu_drain") or f["mu"]
    ts = np.array([tau_stable_max(f["D_s"], f["rho"], mu_eff, f["lambda"], M)
                   for M in Ms])

    fig, ax = plt.subplots(figsize=(3.4, 2.6))
    ax.fill_between(Ms, ts, 1e5, color=C[4], alpha=0.10, lw=0, zorder=0)
    ax.plot(Ms, ts, color=C[4], lw=1.2, dashes=DASH[4], zorder=2,
            label="never catches up")
    ax.plot(Ms, tw, color=C[1], lw=1.8, dashes=DASH[1], zorder=3,
            label="wasted work  ($M^{1/2}$)")
    ax.plot(Ms, tl, color=C[0], lw=1.8, zorder=4,
            label="latency  (0.43 $\\rightarrow$ 1/3)")

    # what the runs themselves said, at the MTBFs actually injected
    if rows:
        seen = {}
        for r in rows:
            m = r.get("_mtbf_run")
            if not m or r.get("n_failures", 0) < 3:
                continue
            if r["ckpt_gap_s"] != r["ckpt_gap_s"] or r["mean_lat_ms"] != r["mean_lat_ms"]:
                continue
            cur = seen.get(m)
            if cur is None or r["mean_lat_ms"] < cur[1]:
                seen[m] = (r["ckpt_gap_s"], r["mean_lat_ms"])
        if seen:
            ax.plot(list(seen), [v[0] for v in seen.values()], "o", ms=6,
                    color=C[0], mec="#fcfcfb", mew=1.0, ls="none", zorder=6,
                    label="measured argmin")

    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_ylim(1.5, 3e3)
    ax.set_xlabel("mean time between failures $M$ (s)")
    ax.set_ylabel("checkpoint interval $\\tau$ (s)")
    style(ax)
    ax.legend(loc="upper left", handlelength=2.2, borderpad=0.2, labelspacing=0.25)
    ax2 = ax.twiny()
    ax2.set_xscale("log"); ax2.set_xlim(ax.get_xlim())
    ax2.set_xticks([3600, 86400])
    ax2.set_xticklabels(["1 h", "1 d"])
    ax2.minorticks_off()
    ax2.tick_params(axis="x", length=2)
    for sp in ("top", "right", "left", "bottom"):
        ax2.spines[sp].set_visible(False)
    save(fig, "fig2_scaling")


def fig3_area_validation(rows, mu_tab, fits):
    """episode cost predicted from measured peak and measured drain rate alone"""
    unstable = set()
    for f in fits.values():
        unstable |= set(f.get("unstable_runs", []))
        unstable |= set(f.get("saturated_runs", []))
    groups = {}
    for r in rows:
        if r["run_id"] in unstable or not r.get("episodes"):
            continue
        lam = r["lambda_meas"]
        for e in r["episodes"]:
            if (e.get("overlapped") or e.get("truncated")
                    or not e.get("peak_lat_ms") or not e.get("mu_drain")):
                continue
            md, ar = e["mu_drain"], e.get("lag_area_rec_s")
            if md <= lam or not ar or ar != ar or ar <= 0:
                continue
            pk = e["peak_lat_ms"] / 1000.0
            pred = md * lam * pk * pk / (2 * (md - lam))
            groups.setdefault(r["keys"], []).append((pred, ar))
    if not groups:
        return
    fig, ax = plt.subplots(figsize=(3.4, 2.6))
    allx, ally = [], []
    for i, (k, pts) in enumerate(sorted(groups.items())):
        x = np.array([p[0] for p in pts]) / 1e6
        y = np.array([p[1] for p in pts]) / 1e6
        allx += list(x); ally += list(y)
        ax.plot(x, y, MARK[i % 5], ms=4, color=C[i % 5], alpha=0.8, mew=0,
                ls="none", label=f"{k // 1000}k keys")
    ax.set_xscale("log"); ax.set_yscale("log")
    lo = min(min(allx), min(ally)) * 0.6
    hi = max(max(allx), max(ally)) * 1.6
    g = np.array([lo, hi])
    ax.plot(g, g, color=INK2, lw=0.8, dashes=(3, 2), zorder=0)
    ax.annotate("$y=x$", xy=(hi, hi), xytext=(-3, -11), textcoords="offset points",
                fontsize=6.5, color=INK2, ha="right", va="top")
    ratio = float(np.median(np.array(ally) / np.array(allx)))
    ax.plot(g, ratio * g, color=INK, lw=0.9, zorder=1)
    ax.annotate(f"predicted $\\times${ratio:.2f}", xy=(hi, ratio * hi),
                xytext=(-3, 3), textcoords="offset points", fontsize=6.5,
                color=INK, ha="right", va="bottom")
    ax.set_xlim(lo, hi); ax.set_ylim(lo, hi * ratio)
    ax.set_xlabel("predicted backlog area (M record$\\cdot$s)")
    ax.set_ylabel("measured excess (M record$\\cdot$s)")
    style(ax)
    if len(groups) > 1:
        ax.legend(loc="upper left")
    save(fig, "fig3_area_validation")


def fig4_components(fits):
    """the two checkpoint costs scale differently, and so do the two outages"""
    # One state size can span more than one fitted group (a failure sweep and a
    # failure-free sweep can land at slightly different rho), so merge by state
    # size and take each component from whichever group actually measured it.
    # Which utilisation to draw this at is not a free choice.  At rho = 0.5 the
    # 400k and 800k arms cannot keep up on checkpointing alone, so they have no
    # steady level to fit and the scaling would rest on two points; the sweep
    # that reaches all four state sizes runs at rho = 0.3.  Rather than hard-code
    # either, take the utilisation that actually covers the most state sizes and
    # say which one it was.
    cand = {}
    for k, f in fits.items():
        w = f["workload"]
        if isinstance(w, str):
            w = eval(w)
        if w[3] != "rocksdb" or w[4] or w[5]:
            continue
        if f.get("delta_s") != f.get("delta_s") and f.get("D_s") != f.get("D_s"):
            continue
        cand.setdefault(round(f.get("rho", 9), 1), set()).add(w[1])
    if not cand:
        return
    rho_pick = max(cand, key=lambda r: (len(cand[r]), -abs(r - 0.5)))
    lo, hi = rho_pick - 0.05, rho_pick + 0.05

    by_keys = {}
    for k, f in fits.items():
        w = f["workload"]
        if isinstance(w, str):
            w = eval(w)
        rho = f.get("rho", 9)
        if not (lo <= rho <= hi) or w[3] != "rocksdb" or w[4] or w[5]:
            continue
        e = by_keys.setdefault(w[1], {"keys": w[1], "d_lat": None, "d_cap": None,
                                      "D_eff": None, "D_res": None, "rhos": []})
        e["rhos"].append(round(rho, 2))
        for fld, src, want in (("d_lat", "delta_s", "n_steady_pts"),
                               ("d_cap", "delta_capacity_s", "n_steady_pts"),
                               ("D_eff", "D_s", "n_episodes"),
                               ("D_res", "D_restore_s", "n_episodes")):
            v = f.get(src)
            if v is None or v != v:
                continue
            score = f.get(want, 0) or 0
            key = fld + "_score"
            if score > e.get(key, 0):
                e[fld] = v
                e[key] = score
    rows = list(by_keys.values())

    rows = [r for r in rows if r["d_lat"] or r["D_eff"]]
    if len(rows) < 2:
        return
    rows.sort(key=lambda r: r["keys"])
    print(f"  fig4: rho={rho_pick:.1f}, state sizes "
          + ", ".join(f"{r['keys']//1000}k" for r in rows))

    def series(field):
        xs = [r["keys"] / 1000.0 for r in rows if r.get(field)]
        ys = [r[field] for r in rows if r.get(field)]
        return xs, ys

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(5.2, 2.2))
    for ax, fields, labels, ylab in (
            (a1, ("d_lat", "d_cap"),
             ("$\\delta$ from latency", "capacity lost"), "per checkpoint (s)"),
            (a2, ("D_eff", "D_res"),
             ("outage a record sees", "restore, as reported"), "per failure (s)")):
        for i, (fld, lab) in enumerate(zip(fields, labels)):
            xs, ys = series(fld)
            if len(xs) < 2:
                continue
            ax.plot(xs, ys, marker=MARK[i], ms=5, color=C[i], lw=1.6,
                    dashes=DASH[i], mec="#fcfcfb", mew=0.8, label=lab)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("keys (thousands)")
        ax.set_ylabel(ylab)
        ax.set_xticks([r["keys"] / 1000.0 for r in rows])
        ax.set_xticklabels([f"{int(r['keys']/1000)}" for r in rows])
        ax.minorticks_off()
        style(ax)
        ax.legend(loc="best", handlelength=2.0, borderpad=0.2, labelspacing=0.2)
    fig.tight_layout(w_pad=1.2)
    save(fig, "fig4_components")


def fig0_trace(run_id, episode_idx=0):
    """one failure episode, so the reader sees the mechanism the model abstracts"""
    import analyze as A
    d = os.path.join(RES, run_id)
    meta = json.load(open(os.path.join(d, "meta.json")))
    lat = A.read_lat(os.path.join(d, "lat.csv"))
    r = A.analyze_run(run_id)
    eps = [e for e in r["episodes"] if not e.get("overlapped")]
    if not eps:
        return
    e = eps[min(episode_idx, len(eps) - 1)]
    k = e["t_kill"]
    ser = A.bin_series(lat, k - 25000, k + 55000, 1000)
    ser = ser[np.isfinite(ser[:, 2])]
    if len(ser) < 5:
        return
    t = (ser[:, 0] - k) / 1000.0

    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(3.4, 3.2), sharex=True,
                                  gridspec_kw={"height_ratios": [2, 1]})
    ax.plot(t, ser[:, 2] / 1000.0, color=C[0], lw=1.6)
    st = r["steady_lat_ms"] / 1000.0
    ax.axhline(st, color=INK2, lw=0.8, dashes=(3, 2))
    ax.annotate("steady", xy=(t[-1], st), xytext=(-2, 3),
                textcoords="offset points", ha="right", color=INK2, fontsize=6.5)
    ax.axvline(0, color=C[1], lw=1.0)
    ax.annotate("kill", xy=(0, ax.get_ylim()[1]), xytext=(-3, -9),
                textcoords="offset points", color=C[1], fontsize=6.5, ha="right")
    if e.get("detect_restore_ms"):
        ax.axvline(e["detect_restore_ms"] / 1000.0, color=C[2], lw=1.0, dashes=(4, 2))
        ax.annotate("restored", xy=(e["detect_restore_ms"] / 1000.0, ax.get_ylim()[1]),
                    xytext=(2, -18), textcoords="offset points", color=C[2], fontsize=6.5)
    ax.set_ylabel("latency (s)")
    style(ax)

    ax2.plot(t, ser[:, 1] / 1000.0, color=C[3], lw=1.4)
    ax2.axhline(r["lambda_meas"] / 1000.0, color=INK2, lw=0.8, dashes=(3, 2))
    ax2.annotate("$\\lambda$", xy=(t[-1], r["lambda_meas"] / 1000.0), xytext=(-2, 3),
                 textcoords="offset points", ha="right", color=INK2, fontsize=6.5)
    ax2.axvline(0, color=C[1], lw=1.0)
    ax2.set_ylabel("throughput\n(k rec/s)")
    ax2.set_xlabel("time relative to failure (s)")
    style(ax2)
    fig.tight_layout(h_pad=0.4)
    save(fig, "fig0_trace")


def pick_ref(fits):
    best, bestn = None, -1
    for k, f in fits.items():
        w = f["workload"]
        if isinstance(w, str):
            w = eval(w)
        if abs(f["rho"] - 0.5) > 0.06:
            continue
        if f.get("delta_s") != f.get("delta_s") or f.get("D_s") != f.get("D_s"):
            continue
        n = f.get("n_episodes", 0)
        if w[1] == 200000 and n > bestn:
            best, bestn = k, n
    return best


def main():
    rows = json.load(open(os.path.join(RES, "summary.json")))
    fits = json.load(open(os.path.join(RES, "fits.json")))
    mu_tab = load_mu()
    for r in rows:
        if r.get("n_failures", 0) >= 3 and r["ckpt_ms"] in (16000, 32000):
            fig0_trace(r["run_id"])
            break
    fig1_latency_vs_tau(fits, rows)
    fig2_scaling(fits, rows)
    fig3_area_validation(rows, mu_tab, fits)
    fig4_components(fits)


if __name__ == "__main__":
    main()
