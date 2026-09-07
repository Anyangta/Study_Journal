#!/usr/bin/env python3
"""Generate drift-robust sweep configs.

Two changes over mkcfg.py, both aimed at one problem: on this shared box the
checkpoint write throughput swings by ~4x over the day (250 MB/s at 03:00 down
to 66 MB/s at 17:00, measured over 6127 checkpoints in the JobManager log).
A sweep that walks tau in increasing order therefore bakes that drift into the
very curve it is fitting.

  1. the tau order inside a block is shuffled, so residual drift becomes noise
     in the regression instead of a tilt;
  2. a control run at a fixed tau is repeated every `control_every` runs, so
     the drift can be measured after the fact and, if needed, divided out.

Usage: mkcfg_drift.py <spec.json> <out.json>
"""
import json, random, sys

ROOT = "/home/wontak/icngc2_stream"


def poisson_offsets(mtbf, window, rng, first=30.0, min_gap=40.0):
    """same as mkcfg.py: exponential gaps, stretched so two kills never land
    inside one recovery"""
    out, t = [], first
    while t < window - 45:
        out.append(round(t, 1))
        t += max(min_gap, rng.expovariate(1.0 / mtbf))
    return out


def make(block, ck, rep, rng, spec, tag, suffix=""):
    w = block
    mu = w["mu"]
    rho = w["rho"]
    meas = w.get("measure_s", spec.get("measure_s", 480))
    mtbf = w.get("mtbf", spec.get("mtbf", 55))
    fails = poisson_offsets(mtbf, meas, rng) if mtbf else []
    sfx = ""
    if w.get("backend", "rocksdb") != "rocksdb":
        sfx += "_" + w["backend"]
    if w.get("incremental"):
        sfx += "_inc"
    if w.get("unaligned"):
        sfx += "_una"
    if w.get("par", 4) != 4:
        sfx += f"_par{w['par']}"
    sfx += suffix
    rid = (f"{tag}_p{w['payload']}_k{w['keys']//1000}k_s{w['spin']}"
           f"_r{int(rho*100)}_c{ck//1000 if ck>=1000 else str(ck)+'ms'}"
           f"_m{mtbf}{sfx}_{rep}")
    return {
        "run_id": rid, "rate": int(mu * rho), "keys": w["keys"],
        "payload": w["payload"], "spin": w["spin"],
        "ckpt_ms": ck, "backend": w.get("backend", "rocksdb"),
        "incremental": w.get("incremental", False),
        "unaligned": w.get("unaligned", False),
        "par": w.get("par", 4),
        "warmup_s": w.get("warmup_s", spec.get("warmup_s", 75)),
        "measure_s": meas, "failures": fails,
        "_mu": mu, "_rho": rho, "_mtbf": mtbf,
    }


def main():
    spec = json.load(open(sys.argv[1]))
    out_path = sys.argv[2]
    rng = random.Random(spec.get("seed", 20260907))
    cfgs = []
    for block in spec["blocks"]:
        ticks = list(block["ckpts"])
        if block.get("shuffle", True):
            rng.shuffle(ticks)
        ctrl_tau = block.get("control_ckpt")
        every = block.get("control_every", 0)
        n = 0
        for ck in ticks:
            cfgs.append(make(block, ck, 0, rng, spec, spec["tag"]))
            n += 1
            if ctrl_tau and every and n % every == 0:
                cfgs.append(make(block, ctrl_tau, len(
                    [c for c in cfgs if "_ctl_" in c["run_id"]]), rng, spec,
                    spec["tag"], suffix="_ctl"))
    json.dump(cfgs, open(out_path, "w"), indent=1)
    tot = sum(c["warmup_s"] + c["measure_s"] + 55 for c in cfgs)
    print(f"{len(cfgs)} runs -> {out_path}   est {tot/3600:.1f} h")
    for c in cfgs:
        print("  ", c["run_id"], "rate", c["rate"], "fails", len(c["failures"]))


if __name__ == "__main__":
    main()
