# Checkpointing for Latency, Not for Throughput: Interval Selection in Distributed Stream Processing

> **Draft status.** Sections 1–4 are written; Section 5 numbers are filled from
> `results/summary.csv` + `results/fits.json` as runs land. Every `[REF: …]` marker is a
> citation the author must supply — none of them are filled in by the tooling, on purpose.
> See `ICNGC_2차논문/체크포인트_주기.md` for the working record.

---

## Abstract

Fault-tolerant stream processors checkpoint periodically, and the interval is
almost always chosen with a rule inherited from batch and HPC computing: the
Young/Daly optimum, which balances checkpoint cost against the work lost to a
failure and grows as the square root of the mean time between failures. We show
that this rule answers the wrong question for a latency-sensitive streaming job.
In a stream processor with a durable source, a failure does not destroy work — it
*rewinds* the source, and the rewound records must be re-read through whatever
service capacity is left over after the live input is served. The cost of a
failure is therefore an area of accumulated backlog, not a quantity of lost work,
and the two objectives are minimised at different intervals with different
scaling exponents: the wasted-work optimum grows as `M^{1/2}`, the latency
optimum as `M^{1/3}`. We derive both, and measure them on a Flink 1.20 cluster
with a Kafka source under injected TaskManager failures. [RESULT-SUMMARY]
We also find that the headroom available during recovery is not the nominal
headroom: immediately after a restore the state backend is cold, and the measured
catch-up rate is [DRAIN-GAP] below the steady-state service rate, which makes a
failure substantially more expensive than the nominal figures predict.

---

## 1. Introduction

A stream processor that guarantees exactly-once state has to write checkpoints,
and someone has to pick how often. The question looks like the classical
checkpoint-interval problem: checkpoints cost something while you take them, and
a failure costs something that grows with how long ago the last checkpoint was,
so there is an interior optimum. The classical answer — Young's first-order
formula and Daly's higher-order refinement — is `τ* = sqrt(2 δ M)` for checkpoint
cost `δ` and mean time between failures `M`. [REF: Young 1974 first-order
checkpoint interval] [REF: Daly 2006 higher-order estimate] It is the rule of
thumb behind most of the interval advice in circulation, and stream-processing
practice has largely inherited it.

The inheritance does not survive inspection. The classical derivation assumes
that a failure destroys the work done since the last checkpoint, and that this
work must be redone at the same rate it was originally done. Neither holds in a
stream processor fed by a durable log. The work is not destroyed: the source is
rewound to the offset recorded in the checkpoint and the records are read again.
And the re-reading does not happen at the original rate — it happens at whatever
service capacity is left after the *live* arrival stream is served, because the
input keeps arriving during the failure and during the catch-up.

That single change of mechanism changes the shape of the cost. In the classical
setting the failure penalty is proportional to the checkpoint age; in streaming
it is proportional to the *square* of the checkpoint age, because the backlog
built up over that age has to be drained, and the area under the backlog curve —
which is what an end-to-end latency SLA actually sees — is quadratic in the peak.
Differentiating a cost with a quadratic term instead of a linear one moves the
optimum from a square root to a cube root of `M`.

This is not a small correction. The two rules agree at one point and diverge
everywhere else, and they diverge in the direction that matters: as the cluster
becomes more reliable — as `M` grows — the classical rule tells you to checkpoint
ever more rarely, at a rate that a latency-sensitive job cannot afford.

This paper makes three contributions.

1. A latency (backlog-area) model of checkpoint interval selection for stream
   processing with a durable source, and the corresponding optimum, contrasted
   against the wasted-work optimum on the same system (§3).
2. A measurement of every term in that model on a real Flink/Kafka deployment
   under injected process failures, with the service rate held fixed while the
   state size is varied over 8× so that the checkpoint cost and the restore cost
   move independently of the utilisation (§4, §5).
3. The observation that recovery does not run at the nominal spare capacity —
   the restored state backend is cold, and the measured catch-up rate is well
   below the steady-state service rate, which is the dominant correction to the
   naive prediction of failure cost (§5).

## 2. Why the batch rule does not transfer

Consider a job reading from a partitioned durable log at rate `λ`, with maximum
sustainable service rate `μ`, utilisation `ρ = λ/μ`. It checkpoints every `τ`
seconds; a checkpoint costs the pipeline `δ` seconds of service capacity (we
treat the disturbance as an equivalent stall, and measure `δ` rather than
assuming it). A failure takes the job down for `D` seconds — detection, restart,
and state restore — after which the source resumes from the last completed
checkpoint.

In the classical batch setting the accounting is in *capacity*: a fraction `δ/τ`
of capacity is spent on checkpoints, and each failure wastes `D` plus the work
since the last checkpoint. That is a linear function of the checkpoint age, and
minimising it gives the familiar square root.

In streaming the accounting an operator cares about is in *lag*. When the job
stops, records keep arriving; when it restarts, it must first re-read everything
between the last checkpoint and the failure, and it must do so while still
serving the live stream. The backlog at the moment of restart is
`λ (a + D)`, where `a` is the age of the last completed checkpoint, and it drains
at `μ − λ` — the spare capacity, not the full capacity. The time to drain is
`λ(a + D)/(μ − λ)`, and the area under the backlog curve, which by Little's law
is exactly the sum of the extra latency paid by every record involved, is
quadratic in `(a + D)`.

The distinction is not academic. At `ρ = 0.8` the spare capacity is a fifth of
the total, so a backlog drains five times slower than it accumulated; the same
failure that costs a batch job a bounded amount of redone work costs a streaming
job an amount that grows quadratically with how stale its last checkpoint was.

## 3. Model

Let `a` be the age of the last completed checkpoint when the failure occurs.
Failures are independent of the checkpoint schedule, so `a ~ U(0, τ)` and
`E[(a+D)^2] = τ^2/3 + τD + D^2`.

**Checkpoint term.** Treating a checkpoint as an equivalent stall of `δ` seconds,
the backlog rises to `λδ` and drains at `μ − λ`; the area is
`λδ^2 / (2(1−ρ))` per checkpoint, so per unit time it is `λδ^2 / (2(1−ρ)τ)`.

**Failure term.** Per failure, the area is
`λD^2/2 + λ^2 E[(a+D)^2] / (2(μ−λ))`, incurred once every `M` seconds.

Dividing by `λ` (Little's law) gives mean end-to-end latency:

```
Lbar(τ) = l0 + δ² / (2(1−ρ)τ)  +  [ D²/2 + λ(τ²/3 + τD + D²) / (2(μ−λ)) ] / M
```

Setting `dLbar/dτ = 0`, the factor `(1−ρ)` cancels from both terms and the
condition reduces to

```
δ² M = ρ τ² ( 2τ/3 + D )                                    (1)
```

Two regimes follow. When the fixed downtime is small relative to the checkpoint
age (`D ≪ τ`), `τ* = (1.5 δ² M / ρ)^{1/3}` — a **cube root** of the mean time
between failures. When the fixed downtime dominates (`D ≫ τ`),
`τ* = δ sqrt(M / (ρD))` — a square root again, but with a different constant than
the classical rule. Utilisation does not change the *shape* of the optimum, only
its position through the explicit `ρ` in (1); it does, however, scale the whole
cost, since both terms carry `1/(1−ρ)`.

**The other objective.** If instead we account in capacity, as the classical
derivation does, then per unit time the job spends `δ/τ` on checkpoints and
`(D + ρτ/2)/M` on failures (the re-read of `λτ/2` records occupies `ρτ/2`
seconds of capacity). Minimising gives

```
τ*_waste = sqrt( 2 δ M / ρ )                                (2)
```

which is Young/Daly with a utilisation correction. Equations (1) and (2) are the
two answers the same system gives to two different questions, and their ratio
grows without bound in `M`.

## 4. Measurement setup

**Hardware.** One bare-metal node, Intel i9-9900K (8 physical cores, 16 threads),
32 GB RAM, NVMe-backed root filesystem, Ubuntu 22.04, Linux 5.15. The machine is
shared with unrelated lab services; a MySQL instance holds roughly one core for
the duration and the Kubernetes control plane is resident. We pin our components
to disjoint core sets — Kafka on cores 0–3, the JobManager on core 4,
TaskManagers on 5–12, the load generator on 13–15 — and record the one-minute
load average alongside every measurement. [SECOND-NODE-NOTE]

**Software.** Apache Flink 1.20.5 in standalone mode: one JobManager and three
TaskManagers of two slots each, job parallelism 4, so a failure always leaves
enough free slots for an immediate restart. Apache Kafka 3.9.2 in KRaft mode,
single broker, one topic of eight partitions. State is kept in RocksDB with full
(non-incremental) checkpoints written to the local filesystem; exactly-once mode,
aligned checkpoints, restart strategy fixed-delay with no delay, heartbeat
timeout 5 s. Everything runs unprivileged out of a home directory; no kernel or
system configuration is modified.

**Workload.** A generator produces fixed-rate records carrying an emission
timestamp and a key; the job keys by that field and maintains one `ValueState`
entry of configurable size per key. State entries are filled with a
key-derived pseudo-random pattern — a zero-filled payload is squashed by the
backend's block compression and would not produce the checkpoint size we
configured, which is a trap worth naming. A configurable per-record spin loop
sets the service rate: without it the pipeline sustains over 10^6 records/s and
the harness, not the system under test, becomes the bottleneck.

**Instrumentation.** Flink's own `numRecordsOutPerSecond` and
`records-lag-max` are 60-second moving averages and cannot resolve the
sub-second dynamics of a checkpoint or a restart, so we do not use them for
anything quantitative. Instead the operator emits, every 500 ms per subtask, a
summary of the records it processed and the distribution of their
`now − emission_timestamp` latency; those summaries are the primary instrument.
The driver polls the REST API at 4 Hz for job state and checkpoint events only.
Record-weighted means over these summaries are, by Little's law, exactly the
backlog areas the model is written in.

**Service-rate calibration.** For each workload point we pre-fill a Kafka backlog
with no consumer attached, then start the job from the earliest offset and read
off the drain rate. That is the `μ` the model's catch-up term refers to, measured
rather than inferred. Holding the spin loop fixed at 32 000 iterations, `μ` stays
within [MU-RANGE] across an 8× change in state size, so state size moves `δ` and
`D` while leaving `μ` and `ρ` alone.

**Failure injection.** At Poisson-distributed times (minimum gap 40 s, so no two
episodes overlap) the driver `kill -9`s a TaskManager that is currently hosting
tasks, then immediately starts a replacement so that capacity is restored for the
next episode. For each episode we record the exact age of the last completed
checkpoint, the detection-to-restore interval, the peak latency, the catch-up
service rate, and the excess-backlog area until latency returns to within twice
its steady value.

## 5. Results

[RESULTS-SECTION]

## 6. Limitations

[LIMITATIONS]

## 7. Conclusion

[CONCLUSION]

---

## References

Every entry below is a placeholder describing what belongs there. They are
deliberately unfilled: fabricated citations are worse than missing ones.

- [REF: Young 1974] — first-order approximation to the optimum checkpoint interval.
- [REF: Daly 2006] — higher-order refinement of the same, the form most often quoted.
- [REF: Flink checkpointing] — the asynchronous barrier snapshotting mechanism this work perturbs.
- [REF: Kafka] — the durable partitioned log whose replay semantics the argument depends on.
- [REF: unaligned checkpoints] — the Flink mechanism evaluated in §5 as a robustness arm.
- [REF: streaming latency SLOs] — at least one paper or industrial report motivating end-to-end latency as the objective.
- [REF: stream benchmark] — Nexmark or an equivalent, to position the workload.
