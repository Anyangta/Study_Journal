#!/usr/bin/env python3
"""
Per-run analysis for the ICNGC-2 checkpoint-interval study.

Primary instrument is the job's own per-interval latency summary (lat.csv),
not Flink's smoothed meters: every number below is a record-weighted statistic
over records the job actually emitted.
"""
import json, math, os, re, sys
import numpy as np

ROOT = "/home/wontak/icngc2_stream"
RES = os.path.join(ROOT, "results")


def read_lat(path):
    """-> array of (ts_ms, subtask, n, mean_ms, p50, p95, p99, max)"""
    out = []
    if not os.path.exists(path):
        return np.zeros((0, 8))
    for line in open(path):
        p = line.rstrip("\n").split(",")
        if len(p) < 9:
            continue
        try:
            out.append([float(p[0]), float(p[1]), float(p[2]), float(p[3]),
                        float(p[4]), float(p[5]), float(p[6]), float(p[7])])
        except ValueError:
            continue
    return np.array(out) if out else np.zeros((0, 8))


def read_gen(path):
    out = []
    if not os.path.exists(path):
        return np.zeros((0, 2))
    for line in open(path):
        p = line.strip().split(",")
        if len(p) < 3 or p[0] == "wall_ms":
            continue
        try:
            out.append([float(p[0]), float(p[1])])
        except ValueError:
            continue
    return np.array(out) if out else np.zeros((0, 2))


def wmean(n, v):
    n = np.asarray(n, dtype=float)
    v = np.asarray(v, dtype=float)
    s = n.sum()
    return float((n * v).sum() / s) if s > 0 else float("nan")


def bin_series(lat, t0_ms, t1_ms, binsize_ms=1000):
    """-> (bin_start_ms, records, weighted mean latency ms) inside [t0,t1)"""
    m = (lat[:, 0] >= t0_ms) & (lat[:, 0] < t1_ms)
    L = lat[m]
    if len(L) == 0:
        return np.zeros((0, 3))
    b = ((L[:, 0] - t0_ms) // binsize_ms).astype(int)
    nb = b.max() + 1
    cnt = np.zeros(nb)
    s = np.zeros(nb)
    np.add.at(cnt, b, L[:, 2])
    np.add.at(s, b, L[:, 2] * L[:, 3])
    ts = t0_ms + np.arange(nb) * binsize_ms
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(cnt > 0, s / np.maximum(cnt, 1e-9), np.nan)
    return np.column_stack([ts, cnt, mean])


def analyze_run(rid):
    d = os.path.join(RES, rid)
    meta = json.load(open(os.path.join(d, "meta.json")))
    lat = read_lat(os.path.join(d, "lat.csv"))
    gen = read_gen(os.path.join(d, "gen.csv"))
    cks = json.load(open(os.path.join(d, "ckpts.json"))) if os.path.exists(os.path.join(d, "ckpts.json")) else []

    t0 = meta["t_meas0"] * 1000.0
    t1 = meta["t_end"] * 1000.0
    dur = (t1 - t0) / 1000.0

    m = (lat[:, 0] >= t0) & (lat[:, 0] < t1)
    L = lat[m]
    n_tot = float(L[:, 2].sum()) if len(L) else 0.0

    # offered load from the generator's own progress log
    lam = float("nan")
    produced = float("nan")
    if len(gen) > 2:
        g = gen[(gen[:, 0] >= t0) & (gen[:, 0] < t1)]
        if len(g) > 5:
            lam = float((g[-1, 1] - g[0, 1]) / ((g[-1, 0] - g[0, 0]) / 1000.0))
            produced = float(g[-1, 1] - g[0, 1])

    mtbf = meta.get("_mtbf")
    if mtbf is None:
        m = re.search(r"_m(\d+)", rid)
        mtbf = int(m.group(1)) if m else None

    res = {
        "run_id": rid,
        "_mtbf_run": mtbf,
        "_rho_cfg": meta.get("_rho"),
        "_mu_cfg": meta.get("_mu"),
        "rate_cfg": meta["rate"], "keys": meta["keys"], "payload": meta["payload"],
        "spin": meta["spin"], "par": meta["par"], "ckpt_ms": meta["ckpt_ms"],
        "backend": meta["backend"], "incremental": meta["incremental"],
        "unaligned": meta.get("unaligned", False),
        "measure_s": round(dur, 1),
        "lambda_meas": lam, "produced": produced,
        "processed": n_tot,
        "throughput": n_tot / dur if dur > 0 else float("nan"),
        "replay_overhead": (n_tot / produced - 1.0) if produced and produced == produced and produced > 0 else float("nan"),
    }
    # A run that emits fewer records than were produced never kept up at all:
    # that is a capacity ceiling, a different failure from "recovery outlived
    # the gap to the next failure", and worth separating.
    res["keep_up"] = (res["throughput"] / lam) if lam == lam and lam > 0 else float("nan")
    res["overloaded"] = bool(res["keep_up"] == res["keep_up"] and res["keep_up"] < 0.99)

    if len(L):
        res["mean_lat_ms"] = wmean(L[:, 2], L[:, 3])
        res["p95_lat_ms"] = wmean(L[:, 2], L[:, 5])
        res["p99_lat_ms"] = wmean(L[:, 2], L[:, 6])
        res["max_lat_ms"] = float(L[:, 7].max())
    else:
        res.update({"mean_lat_ms": float("nan"), "p95_lat_ms": float("nan"),
                    "p99_lat_ms": float("nan"), "max_lat_ms": float("nan")})

    # ---- checkpoints inside the measurement window
    ck = [c for c in cks if c.get("status") == "COMPLETED"
          and t0 <= c.get("latest_ack_timestamp", c.get("trigger_timestamp", 0)) <= t1]
    res["n_ckpt"] = len(ck)
    if ck:
        dur = [c.get("end_to_end_duration", 0) for c in ck]
        res["ckpt_dur_ms"] = float(np.mean(dur))
        res["ckpt_dur_p95_ms"] = float(np.percentile(dur, 95))
        res["ckpt_size_mb"] = float(np.mean([c.get("state_size", 0) for c in ck])) / 1e6
        res["ckpt_proc_mb"] = float(np.mean([c.get("processed_data", 0) for c in ck])) / 1e6
        syncs = []
        for c in ck:
            t = c.get("tasks") or {}
            for v in t.values():
                cd = (v or {}).get("checkpoint_duration") or {}
                if "sync" in cd:
                    syncs.append(cd["sync"])
        res["ckpt_sync_ms"] = float(np.mean(syncs)) if syncs else float("nan")
    else:
        res.update({"ckpt_dur_ms": float("nan"), "ckpt_dur_p95_ms": float("nan"),
                    "ckpt_size_mb": float("nan"), "ckpt_sync_ms": float("nan"),
                    "ckpt_proc_mb": float("nan")})
    res["n_ckpt_failed"] = meta.get("ckpt_counts", {}).get("failed", 0)
    # the configured interval is only a lower bound: a checkpoint longer than it
    # pushes the achieved interval out, so measure what actually happened
    acks = sorted(c.get("latest_ack_timestamp") or 0 for c in ck)
    if len(acks) > 2:
        gaps = np.diff(np.array(acks, dtype=float)) / 1000.0
        res["ckpt_gap_s"] = float(np.median(gaps))
    else:
        res["ckpt_gap_s"] = float("nan")

    # ---- failure episodes
    kills = sorted(e["ts"] for e in meta.get("events", []) if e["type"] == "kill")
    restores = sorted(e["ts"] for e in meta.get("events", []) if e["type"] == "restore")
    ser = bin_series(lat, t0, t1, 1000)
    valid = ser[np.isfinite(ser[:, 2])]

    ck_all = sorted(
        [(c.get("latest_ack_timestamp") or c.get("trigger_timestamp") or 0, c)
         for c in cks if c.get("status") == "COMPLETED"])

    def ckpt_age_at(t):
        prev = [a for a, _ in ck_all if a and a <= t]
        return (t - prev[-1]) / 1000.0 if prev else None

    def build_episodes(baseline_s):
        """baseline_s: steady latency in seconds used for the recovery threshold"""
        base_ms = baseline_s * 1000.0 if baseline_s == baseline_s else 0.0
        thr = base_ms * 2.0 if base_ms > 0 else 1e9
        out = []
        for k in kills:
            rr = [r for r in restores if k <= r <= k + 90000]
            t_restore = rr[0] if rr else float("nan")
            after = valid[valid[:, 0] > k]
            t_first = float(after[0, 0]) if len(after) else float("nan")
            t_rec, run = float("nan"), 0
            for r in after:
                if r[2] <= thr:
                    run += 1
                    if run >= 3:
                        t_rec = float(r[0]) - 2000
                        break
                else:
                    run = 0
            end = t_rec if t_rec == t_rec else k + 90000
            seg = after[after[:, 0] <= end]
            # Service rate while genuinely still behind.  Bins near the end of
            # the episode are already caught up and run at lambda, so including
            # them would bias the drain rate down toward the arrival rate.
            drain = seg[seg[:, 0] >= (t_first + 1500 if t_first == t_first else k)]
            busy = drain[drain[:, 2] > 2.0 * base_ms] if base_ms > 0 else drain
            src = busy if len(busy) >= 3 else drain
            mu_drain = float(np.median(src[:, 1])) if len(src) >= 3 else float("nan")
            mu_drain_top = float(np.percentile(drain[:, 1], 90)) if len(drain) >= 3 else float("nan")
            area = float(np.nansum((seg[:, 2] - base_ms) * seg[:, 1] / 1000.0)) if len(seg) else float("nan")
            age = ckpt_age_at(k)
            pk = float(seg[:, 2].max()) / 1000.0 if len(seg) else None
            # What the interval actually has to cover.  The oldest record the job
            # must re-read is the one written just after the last checkpoint, so
            # its latency when it finally comes out is age + effective outage;
            # that outage is longer than the restore Flink reports, because the
            # job still has to reopen its state and refill its consumers.
            d_eff = (pk - age) if (pk is not None and age is not None) else None
            # An episode whose recovery ran past the end of the measurement
            # window is censored: its area is a lower bound, not a measurement.
            last_bin = float(valid[-1, 0]) if len(valid) else k
            truncated = (t_rec != t_rec) or (t_rec > last_bin - 1500)
            out.append({
                "t_kill": k,
                "ckpt_age_s": age,
                "D_eff_s": d_eff,
                "truncated": bool(truncated),
                "detect_restore_ms": (t_restore - k) if t_restore == t_restore else None,
                "first_output_ms": (t_first - k) if t_first == t_first else None,
                "recover_ms": (t_rec - k) if t_rec == t_rec else None,
                "peak_lat_ms": float(seg[:, 2].max()) if len(seg) else None,
                "mu_drain": mu_drain if mu_drain == mu_drain else None,
                "mu_drain_top": mu_drain_top if mu_drain_top == mu_drain_top else None,
                "n_busy_bins": int(len(busy)) if base_ms > 0 else None,
                "lag_area_rec_s": area,
            })
        return out

    def steady_from(episodes):
        """bins outside every failure episode, using each episode's own measured
        recovery time rather than one conservative window that would swallow the
        whole run when failures are frequent"""
        excl = []
        for e in episodes:
            rec = e.get("recover_ms") or 20000.0
            excl.append((e["t_kill"] - 3000, e["t_kill"] + rec + 5000))
        keep = np.array([r for r in valid
                         if not any(a <= r[0] <= b for a, b in excl)]) if len(valid) else np.zeros((0, 3))
        return keep

    if len(valid):
        # pass 1: a robust baseline that ignores the failure spikes outright
        base0 = float(np.percentile(valid[:, 2], 20)) / 1000.0
        eps = build_episodes(base0)
        keep = steady_from(eps)
        if len(keep) > 3:
            steady_s = wmean(keep[:, 1], keep[:, 2]) / 1000.0
        else:
            steady_s = base0
        # pass 2: episodes measured against the refined steady level
        eps = build_episodes(steady_s)
        keep = steady_from(eps)
        if len(keep) > 3:
            res["steady_lat_ms"] = wmean(keep[:, 1], keep[:, 2])
            res["steady_rate"] = float(np.median(keep[:, 1]))
            res["steady_lat_sd"] = float(np.std(keep[:, 2]))
            res["steady_bins"] = int(len(keep))
        else:
            res["steady_lat_ms"] = steady_s * 1000.0
            res["steady_rate"] = float(np.median(valid[:, 1]))
            res["steady_lat_sd"] = float("nan")
            res["steady_bins"] = int(len(keep))
    else:
        eps = []
        res.update({"steady_lat_ms": float("nan"), "steady_rate": float("nan"),
                    "steady_lat_sd": float("nan"), "steady_bins": 0})

    # ---- delta measured directly, per checkpoint
    # Over a checkpoint's own window the pipeline should have served lam*(ack-trigger)
    # records; whatever it fell short by is the equivalent stall the model calls delta.
    if lam == lam and lam > 0 and len(lat):
        fine = bin_series(lat, t0, t1, 500)
        fine = fine[np.isfinite(fine[:, 1])]
        fail_windows = [(e["t_kill"] - 3000, e["t_kill"] + (e.get("recover_ms") or 20000) + 5000)
                        for e in eps]
        deltas, dwin = [], []
        for c in ck:
            tt = c.get("trigger_timestamp")
            ta = c.get("latest_ack_timestamp")
            if not tt or not ta or ta <= tt:
                continue
            if any(a <= tt <= b or a <= ta <= b for a, b in fail_windows):
                continue
            w = fine[(fine[:, 0] >= tt - 500) & (fine[:, 0] < ta + 500)]
            if len(w) < 2:
                continue
            span = (w[-1, 0] + 500 - w[0, 0]) / 1000.0
            processed = float(w[:, 1].sum())
            deficit = lam * span - processed
            deltas.append(deficit / lam)
            dwin.append(span)
        if len(deltas) >= 3:
            arr = np.array(deltas)
            res["delta_direct_s"] = float(np.median(arr))
            res["delta_direct_mean_s"] = float(np.mean(arr))
            res["delta_direct_sd_s"] = float(np.std(arr))
            res["delta_direct_n"] = int(len(arr))
            res["ckpt_window_s"] = float(np.median(dwin))
        else:
            res.update({"delta_direct_s": float("nan"), "delta_direct_n": len(deltas),
                        "delta_direct_mean_s": float("nan"), "delta_direct_sd_s": float("nan"),
                        "ckpt_window_s": float("nan")})
    else:
        res.update({"delta_direct_s": float("nan"), "delta_direct_n": 0,
                    "delta_direct_mean_s": float("nan"), "delta_direct_sd_s": float("nan"),
                    "ckpt_window_s": float("nan")})

    res["n_failures"] = len(kills)
    res["episodes"] = eps
    if eps:
        gv = lambda k: [e[k] for e in eps if e.get(k) is not None]
        res["ckpt_age_mean_s"] = float(np.mean(gv("ckpt_age_s"))) if gv("ckpt_age_s") else float("nan")
        res["D_restore_ms"] = float(np.mean(gv("detect_restore_ms"))) if gv("detect_restore_ms") else float("nan")
        res["D_first_out_ms"] = float(np.mean(gv("first_output_ms"))) if gv("first_output_ms") else float("nan")
        res["recover_ms"] = float(np.mean(gv("recover_ms"))) if gv("recover_ms") else float("nan")
        res["peak_lat_ms"] = float(np.mean(gv("peak_lat_ms"))) if gv("peak_lat_ms") else float("nan")
        res["mu_drain"] = float(np.median(gv("mu_drain"))) if gv("mu_drain") else float("nan")
        res["mu_drain_top"] = float(np.median(gv("mu_drain_top"))) if gv("mu_drain_top") else float("nan")
        res["D_eff_s"] = float(np.median(gv("D_eff_s"))) if gv("D_eff_s") else float("nan")
        # episodes that started before the previous one finished are not
        # independent samples; flag them rather than double count
        res["n_truncated"] = sum(1 for e in eps if e.get("truncated"))
        for i in range(1, len(eps)):
            prev = eps[i - 1]
            if prev.get("recover_ms") and eps[i]["t_kill"] < prev["t_kill"] + prev["recover_ms"]:
                eps[i]["overlapped"] = True
                prev["overlapped"] = True
    return res


def main():
    rids = sys.argv[1:] if len(sys.argv) > 1 else sorted(os.listdir(RES))
    rows = []
    for rid in rids:
        p = os.path.join(RES, rid, "meta.json")
        if not os.path.exists(p):
            continue
        try:
            rows.append(analyze_run(rid))
        except Exception as e:
            print(f"# {rid}: {e}", file=sys.stderr)
    json.dump(rows, open(os.path.join(RES, "summary.json"), "w"), indent=1, default=str)
    keys = ["run_id", "ckpt_ms", "ckpt_gap_s", "payload", "keys", "spin", "backend",
            "incremental", "unaligned", "lambda_meas", "throughput",
            "mean_lat_ms", "steady_lat_ms", "p95_lat_ms", "p99_lat_ms", "max_lat_ms",
            "n_ckpt", "ckpt_dur_ms", "ckpt_dur_p95_ms", "ckpt_size_mb", "steady_bins",
            "delta_direct_s", "delta_direct_n", "keep_up", "overloaded",
            "n_failures", "D_restore_ms", "D_eff_s", "recover_ms", "peak_lat_ms", "mu_drain",
            "ckpt_age_mean_s", "replay_overhead", "steady_rate", "measure_s"]
    with open(os.path.join(RES, "summary.csv"), "w") as f:
        f.write(",".join(keys) + "\n")
        for r in rows:
            f.write(",".join(str(r.get(k, "")) for k in keys) + "\n")
    print(f"analyzed {len(rows)} runs -> results/summary.csv")
    for r in rows:
        print(f"{r['run_id']:>28} ckpt={r['ckpt_ms']:>6} lam={r['lambda_meas']:>9.0f} "
              f"thr={r['throughput']:>9.0f} lat={r['mean_lat_ms']:>8.1f} "
              f"steady={r.get('steady_lat_ms', float('nan')):>7.1f} "
              f"nck={r['n_ckpt']:>3} ckdur={r['ckpt_dur_ms']:>7.0f} "
              f"sz={r['ckpt_size_mb']:>6.0f}MB d={r.get('delta_direct_s', float('nan')):>6.3f} "
              f"f={r['n_failures']}")


if __name__ == "__main__":
    main()
