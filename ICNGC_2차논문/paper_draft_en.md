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
with a Kafka source under injected TaskManager failures. On a Flink 1.20 / Kafka 3.9 deployment the measured optimum sits at 4.4 s where the classical rule says 4.9 s at a 55 s MTBF and 39.9 s at a one-hour MTBF, against a latency optimum of 18.4 s; the failure cost of an episode is predicted from measured quantities alone with R² = 0.998.
We also find that the headroom available during recovery is not the nominal
headroom: immediately after a restore the state backend is cold, and the measured
catch-up rate is 10 %, and 28 % when checkpointing is continuous, below the steady-state service rate, which makes a
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
load average alongside every measurement. The second node of the intended two-node testbed is unreachable: it answers ICMP and completes TCP handshakes, but no user-space service on it responds, and we have no out-of-band power control. §6 states what that costs us.

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
within 93 384–112 469 records/s (a 17 % spread) across an 8× change in state size, so state size moves `δ` and
`D` while leaving `μ` and `ρ` alone.

**Failure injection.** At Poisson-distributed times (minimum gap 40 s, so no two
episodes overlap) the driver `kill -9`s a TaskManager that is currently hosting
tasks, then immediately starts a replacement so that capacity is restored for the
next episode. For each episode we record the exact age of the last completed
checkpoint, the detection-to-restore interval, the peak latency, the catch-up
service rate, and the excess-backlog area until latency returns to within twice
its steady value.

## 5. Results

All numbers below are for the reference workload: 200 000 keys of 1 KiB state,
`lambda` = 54 811 records/s against a calibrated service rate of 108 925
records/s (`rho` = 0.50), checkpoints of 534–701 MB taking 2.1–2.8 s. The
checkpoint interval is swept over {2, 4, 8, 16, 32, 64, 128} s with failures
injected at a mean of one per 55 s. Every run lasts 480 s of measurement after a
75 s warm-up; the seven runs in this sweep contain 40 failure episodes.

### 5.1 The interval has a floor, and it is the checkpoint's own duration

| `tau` (s) | achieved gap (s) | steady latency (ms) | checkpoint (MB) | checkpoint (ms) |
|---|---|---|---|---|
| 2   | 1.9  | 622    | 534 | 2122 |
| 4   | 3.8  | 696    | 701 | 2764 |
| 8   | 7.6  | 265    | 633 | 2460 |
| 16  | 15.6 | 137    | 573 | 2316 |
| 32  | 31.5 | 106    | 580 | 2183 |
| 64  | 63.4 | 98     | 669 | 2687 |
| 128 | —    | (unstable) | 519 | 2842 |

Between 8 s and 64 s the steady-state latency falls as `1/tau` exactly as the
model says, converging on a floor of `l0` = 57 ms (fit R² = 0.950). Below about
8 s it turns around: a checkpoint takes 2.1–2.8 s, so at `tau` = 2 s the job is
checkpointing essentially all the time, one checkpoint's disturbance never drains
before the next begins, and the latency more than doubles. **The useful interval
range is bounded below by roughly twice the checkpoint duration**, which is not a
free parameter — it is set by the state size and the store's write bandwidth.
Extrapolating the `1/tau` term below that bound, as the classical rule implicitly
does, predicts a benefit that the system cannot deliver.

### 5.2 A checkpoint delays five times more than it costs

Two different measurements of "what a checkpoint costs" disagree by 5.8×.

- **Capacity actually lost.** Over each checkpoint's own window the job served
  `delta_cap` = 0.221 s worth of arrivals less than it should have — 2–3 % of
  capacity, stable across `tau` from 8 s to 64 s.
- **Equivalent stall implied by latency.** Fitting `l0 + delta²/(2(1-rho)tau)` to
  the same runs gives `delta` = 1.277 s.

A checkpoint therefore *delays* about 5.8 times more record-seconds than the
throughput it removes: barrier alignment and buffered records age without being
dropped from the count. This matters directly for interval selection, because the
classical rule is written in the capacity currency. Feeding the capacity figure
into a latency objective understates the checkpoint term by a factor of 34
(`delta` enters squared).

### 5.3 The outage is three times what the engine reports

Flink reports 1.25 s median from the `kill -9` to the restore of the checkpoint.
The oldest record that comes out after the restart, however, is 4.11 s old once
its own checkpoint age is subtracted (median over 36 episodes, s.d. 0.5 s). The
missing 2.9 s is the ramp back to service — task scheduling, reopening ~600 MB of
RocksDB state, and re-establishing the Kafka fetches. The interval has to cover
the outage a record sees, not the one the engine logs, and using the reported
figure underestimates the failure term by a factor of about 10 at small `tau`.

The catch-up rate is likewise not the nominal one: the median service rate while
still behind is 98 000 records/s, 90 % of the calibrated `mu`, and it falls to
78 000 records/s (72 %) at `tau` = 2 s where checkpointing is continuous.

### 5.4 Failure cost is predicted with no free parameter

For each episode we predict the excess latency it causes from three quantities
measured on that same episode — the peak latency, the drain rate, and the
arrival rate — and nothing else:

```
A = mu_d * lambda * peak² / (2 (mu_d - lambda))
```

Across 36 episodes spanning two orders of magnitude in cost (1.4 to 143 million
record·s) the prediction tracks the measurement with **R² = 0.998** and a median
ratio of **1.14** (median relative error 14.6 %). Regressing the measured excess
on `(a + D)²`, where `a` is the actual age of the last completed checkpoint at
the moment of the kill, gives R² = 0.999 and a slope 0.937× the parameter-free
prediction. The quadratic form is not an assumption that survived — it is the
form the data has.

### 5.5 The optimum, and how far the classical rule is from it

With `delta` = 1.277 s, `D` = 4.11 s, `rho` = 0.50:

| `M` | latency-optimal (measured terms) | latency-optimal (closed form) | wasted-work optimal | classical Young/Daly | ratio |
|---|---|---|---|---|---|
| 30 s  | 2.8 s  | 2.7 s  | 5.1 s   | 3.6 s   | 1.8× |
| 55 s  | 3.6 s  | 3.5 s  | 7.0 s   | 4.9 s   | 1.9× |
| 150 s | 5.5 s  | 5.3 s  | 11.5 s  | 8.1 s   | 2.1× |
| 10 min| 9.4 s  | 9.2 s  | 22.9 s  | 16.3 s  | 2.4× |
| 1 h   | 18.4 s | 18.0 s | 56.2 s  | 39.9 s  | 3.1× |
| 1 day | 56.6 s | 55.3 s | 275.3 s | 195.3 s | 4.9× |

The closed form of (1) agrees with the optimum obtained by composing the measured
steady and episode terms to within 3 % at every `M`. Directly, the seven measured
runs at `M` = 55 s put the minimum mean latency at an achieved interval of 4.4 s
— one grid step from the predicted 3.5 s.

The gap to the classical rule is small when failures are frequent and grows
without bound as the cluster becomes reliable, because one answer grows as
`M^{1/2}` and the other as `M^{1/3}`. At an MTBF of one hour — an unremarkable
figure for a small cluster — the classical rule picks an interval 2.2–3.1× too
long; at one day, 3.4–4.9× too long. Every one of those seconds is added
directly to the tail latency of the next failure.

### 5.6 Beyond a threshold the job never catches up

At `tau` = 128 s under `M` = 55 s failures the job does not have a steady state at
all: mean latency is 126 s and rising, because the backlog left by one failure
outlives the gap to the next. The condition is not a capacity condition — the job
runs at 50 % utilisation throughout. Requiring the expected catch-up to finish
inside one MTBF,

```
lambda (tau/2 + D) / (mu_d - lambda) + D  <  M
```

puts the ceiling at 72 s for these parameters. Measured: `tau` = 64 s is stable
(13.6 s mean latency), `tau` = 128 s is not. The classical rule has no term that
can express this ceiling, and at `M` = 55 s it recommends 4.9 s — safe here only
by accident of being far below it.

[PENDING-ARMS] Utilisation, state-size, backend and checkpoint-mode arms are
still running; §5.7 will report whether `delta`, `D` and the exponent hold across
them.

## 6. Limitations

**One machine.** The intended two-node testbed lost its second node before this
work started — the machine answers ICMP and completes TCP handshakes but no
user-space process on it responds, and we have no out-of-band power control. The
cluster here is therefore one JobManager, three TaskManagers and a broker as
separate processes on one host, pinned to disjoint core sets. Process failure,
state restore, source rewind and catch-up are all real; what is missing is
network distance between the failed task and its replacement, which would add to
`D` and so move the optimum up. Our numbers are the optimistic end.

**One failure mode.** We `kill -9` a TaskManager that is hosting tasks. Slow
nodes, partial partitions and JobManager loss have different `D` distributions.

**One workload shape.** A keyed state update with a configurable per-record spin.
The spin is what lets us hold `mu` fixed while state size varies 8×, but a
pipeline with joins or windows would have different alignment behaviour, which
is exactly where the 5.8× gap between the two cost currencies comes from.

**Shared host.** An unrelated MySQL instance holds about one core throughout and
a Kubernetes control plane is resident. We pin around them and log load average
with every sample, but they add variance we cannot remove.

**`delta` is fitted from four points.** The saturated (`tau` <= 4 s) and unstable
(`tau` = 128 s) runs are excluded from that regression on stated criteria, and
reported separately rather than dropped.

## 7. Conclusion

The interval question for a stream processor is not the batch question with
different constants. A durable source turns lost work into replayed work, replay
competes with live arrivals for the same capacity, and the cost that a latency
SLA feels is the area under the resulting backlog — quadratic in the checkpoint
age where the batch cost is linear. The optimum accordingly grows as the cube
root of the mean time between failures rather than the square root, and the two
answers separate by 3× at an MTBF of an hour and 5× at a day.

Two measurement results matter as much as the exponent. A checkpoint delays 5.8×
more record-seconds than the capacity it consumes, so a rule calibrated in
capacity is calibrated in the wrong currency. And the outage a record actually
experiences is 4.1 s where the engine reports 1.25 s, because reopening the state
and refilling the source are not part of what "restored" means. Both push the
right interval down, and both are invisible to the classical derivation.

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
