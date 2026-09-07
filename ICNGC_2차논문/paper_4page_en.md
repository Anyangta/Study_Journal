# Checkpoint Intervals for Latency-Sensitive Stream Processing

> **STATUS 2026-09-07 — DELETE THIS BLOCK BEFORE SUBMISSION.**
> Every number below §3 still comes from the pre-restart dataset and is being
> re-measured. Re-analysis of the re-run found that the shared host's checkpoint
> write throughput swings 4x over the day, which tilted the fitted curves, and
> that the steady term was being fitted on failure runs where almost no steady
> interval exists. Fitting it on the no-failure runs instead already restores
> R2 = 0.98 (100k keys) and 0.86 (200k keys), and moves the reference arm to
> l0 = 99 ms, delta = 1.91 s. Sweeps m6 (reference arm re-measured with shuffled
> interval order, control runs, and held-out MTBFs of 150 s and 300 s) and m8
> (delta against state size, four sizes) are queued into the quiet hours; the
> results tables, Figs. 1-3 and `verify.py`'s expected values must all be
> regenerated from them. See `논문요약/04_진행상황과_남은일.md`.

*ICNGC short paper — 4 pages. Numbers are current as of the runs listed in
`summary.csv`; `verify.py` re-derives every one of them from the raw data.
`[REF: …]` markers are citations the author must supply.*

---

## Abstract

Stream processors with exactly-once state must checkpoint, and the interval is
left to the operator. The advice in circulation is the classical batch rule,
`τ* = sqrt(2 δ M)`, which assumes a failure destroys the work done since the last
checkpoint and that the work is redone at the rate it was first done. Fed by a
durable log, neither assumption holds: the source rewinds and the records are
read again, and that replay competes with live arrivals for the same capacity.
The cost of a failure is therefore the area under the backlog it leaves —
quadratic in the checkpoint age where the classical cost is linear — so the
optimum grows more slowly than the square root, with a local exponent of 0.43
falling to 0.35 over the practical range. We derive the latency-optimal interval
and a stability bound the classical rule cannot express, and measure every term
on a Flink/Kafka deployment under injected TaskManager failures. Two measurements
move the answer more than the exponent does: a checkpoint delays 5.8× more
record-seconds than the capacity it consumes, and the outage a record experiences
is 4.3 s where the engine reports 1.2 s. The failure term is predicted from
per-episode measurements with no fitted parameter (R² = 0.999). The two rules
separate by 2.2× at a one-hour MTBF and 3.5× at one day.

## 1. Introduction

A stream processor with exactly-once state has to checkpoint, and someone has to
choose how often. The problem looks like the classical one — checkpoints cost
something, a failure costs something that grows with the age of the last
checkpoint, so there is an interior optimum — and the classical answer,
`τ* = sqrt(2 δ M)`, is what circulates as advice. [REF: Young 1974]
[REF: Daly 2006]

The inheritance does not survive inspection. Young and Daly assume a failure
destroys the work since the last checkpoint and that it is redone at the rate it
was first done. Fed by a durable log, neither holds: the source rewinds and the
records are read again, and that re-reading does not get the machine to itself,
because input keeps arriving throughout the outage and the catch-up.

That changes the shape of the cost, not just its constants. The classical failure
penalty is linear in the checkpoint age; here the backlog built over that age has
to be drained, and the area under it — what an end-to-end latency SLA sees — is
quadratic. A quadratic term where the classical derivation has a linear one
drives the optimum below the square root, to a cube root once the interval
outgrows the outage. The two rules agree at one point and diverge everywhere
else, in the direction that matters: the more reliable the cluster, the further
the classical rule pushes the interval past what a latency-sensitive job can
afford.

We contribute (i) a latency model of interval selection for a stream processor
with a durable source, plus a stability bound neither classical rule can express;
(ii) a measurement of every term of it on a real Flink/Kafka deployment, with the
service rate held fixed while state size varies 8×; and (iii) two measurements
that shift the answer by more than the exponent does — what a checkpoint costs in
capacity versus in latency, and the outage the engine reports versus the one a
record experiences.

## 2. Model

Let `λ` be the arrival rate, `μ` the service rate, `ρ = λ/μ`, `τ` the checkpoint
interval, `a` the age of the last completed checkpoint when a failure lands, and
`D` the effective outage — everything between the failure and the job serving at
rate again. Failures are independent of the checkpoint schedule, so
`a ~ U(0, τ)` and `E[(a+D)²] = τ²/3 + τD + D²`.

The objective is the mean end-to-end latency over emitted records — what a lag
dashboard reports and what an SLA is written against. The choice matters
arithmetically: during catch-up the job serves at `μ_d`, not `λ`, so summing over
records gives `μ_d/λ` times the time-integrated backlog, and getting that factor
wrong costs a factor of two.

**Failure term.** After the restart the oldest surviving record is `a + D` old;
the backlog `λ(a+D)` drains at `μ_d − λ` while the job emits at `μ_d`, so the
latency summed over records is `A = μ_d λ E[(a+D)²] / (2(μ_d − λ))`.

**Checkpoint term.** A checkpoint acts as an equivalent stall `δ`, contributing
`δ²/(2(1−ρ)τ)` of mean latency per unit time. We measure `δ` rather than
assuming it, and §4.2 shows the right `δ` here is not the capacity a checkpoint
consumes.

Together, with `ρ_d = λ/μ_d`:

```
Lbar(τ) = l0 + δ²/(2(1−ρ)τ) + μ_d (τ²/3 + τD + D²) / (2(μ_d − λ) M)
```

and `dLbar/dτ = 0` gives

```
δ² M (1−ρ_d)/(1−ρ) = τ² (2τ/3 + D)                              (1)
```

For `D ≪ τ` this is a cube root, `τ* ≈ (1.5 δ² M)^{1/3}`; for `D ≫ τ` a square
root, `τ* ≈ δ sqrt(M/D)`, but with a constant the classical rule lacks. Real
deployments sit in the crossover (§4.5). Utilisation nearly cancels in (1) since
`μ_d ≈ μ`, so it barely moves the optimum — but it collapses the range of
intervals that work at all (§4.6).

Accounting in capacity instead, as the classical derivation does, the job spends
`δ/τ` on checkpoints and `(D + ρτ/2)/M` on failures, giving
`τ*_waste = sqrt(2 δ M / ρ)` — Young/Daly with a utilisation correction. These
are two answers the same system gives to two different questions, and their ratio
grows without bound in `M`.

Finally, an interval is usable only if the backlog one failure leaves is drained
before the next arrives:

```
λ (τ/2 + D) / (μ_d − λ) + D  <  M                                (2)
```

Above this bound the job has no steady state. This is not a capacity condition —
it binds far below full utilisation — and it is what the classical rule is most
dangerous about, recommending an interval that grows as `sqrt(M)` with nothing to
stop it crossing (2).

## 3. Setup

**Cluster.** One bare-metal node: Intel i9-9900K (8 cores, 16 threads), 32 GB,
NVMe, Ubuntu 22.04. Flink 1.20.5 standalone — one JobManager, three TaskManagers
of two slots, job parallelism 4, so a failure always leaves slots for an
immediate restart — and Kafka 3.9.2 in KRaft mode, one topic of eight partitions.
State lives in RocksDB with full checkpoints on the local filesystem;
exactly-once, aligned checkpoints, no restart delay, 5 s heartbeat timeout.
Everything runs unprivileged from a home directory. Components are pinned to
disjoint core sets (Kafka 0–3, JobManager 4, TaskManagers 5–12, generator 13–15)
because the host is shared with unrelated lab services; load average and
checkpoint write throughput are logged with every sample. The intended second node is unreachable — it answers ICMP and
completes TCP handshakes but no user-space service on it responds, and we have no
out-of-band power control (§5).

**Drift control.** Pinning cores does not isolate the disk. Over 6 127 completed
checkpoints on this host the median write throughput moves from 268 MB/s at 03:00
to 66 MB/s at 17:00 — a 4x swing at unchanged checkpoint size, tracking the
working day of unrelated services rather than anything we do. A sweep that walks
`tau` upward in wall-clock order folds that drift into the very curve it is
fitting, and at the slow end of the day a checkpoint takes 7-9 s, which erases
the short-interval half of the sweep entirely. Each sweep therefore shuffles its
interval order and repeats a fixed-interval control run every four runs; the
spread among those controls is the error bar we quote for that sweep, and the
throughput measured during each run is reported with it.

**Workload.** A generator emits fixed-rate records carrying an emission timestamp
and a key; the job keys on that field and holds one `ValueState` entry per key.
Entries carry a key-derived pseudo-random pattern, because a zero-filled payload
is squashed by the backend's compression and does not produce the checkpoint size
configured — 200 MB of logical state checkpointed as 36 MB before we caught it. A
per-record spin loop sets the service rate; without it the harness, not the system
under test, is the bottleneck. With the spin fixed, `μ` stays within
93 384–112 469 records/s across an 8× change in state size, so state size moves
`δ` and `D` while leaving `μ` and `ρ` alone.

**Instrumentation.** Flink's `numRecordsOutPerSecond` and `records-lag-max` are
60-second moving averages and cannot resolve a checkpoint or a restart, so the
operator instead emits, every 500 ms per subtask, its processed count and the
distribution of `now − emission_timestamp`; record-weighted means over those are,
by Little's law, the backlog areas the model is written in. The REST API is
polled at 4 Hz for job state and checkpoint events only. Service rates are
calibrated by draining a pre-filled Kafka backlog with no consumer attached —
the `μ` the catch-up term refers to, measured rather than inferred.

**Failures.** At Poisson times (minimum gap 40 s, so episodes do not overlap) the
driver `kill -9`s a TaskManager hosting tasks and immediately starts a
replacement. Per episode we record the exact age of the last completed
checkpoint, the restore interval, the peak latency, the catch-up rate, and the
excess-backlog area until latency returns to within twice its steady value.

## 4. Results

Reference workload: 200 000 keys of 1 KiB state, `λ` = 54 811 records/s against a
calibrated 108 925 (`ρ` = 0.50), checkpoints of 534–701 MB taking 2.1–2.8 s,
`τ` swept over {2, 4, 8, 16, 32, 64, 128} s, failures at one per 55 s, 480 s of
measurement per run after 75 s of warm-up, 40 injected failures of which 24 land
in runs where an episode is well defined (§4.4).

### 4.1 The interval has a floor

Steady-state latency falls as `1/τ` from 265 ms to 98 ms over `τ` = 8–64 s,
converging on `l0` = 57 ms (R² = 0.950), then turns back up below about 8 s
because a checkpoint takes 2.1–2.8 s and one disturbance no longer drains before
the next begins. **The usable interval is bounded below by roughly twice the
checkpoint duration** — not a free parameter but a consequence of state size and
store bandwidth — so extrapolating the `1/τ` term below that bound, as the
classical rule implicitly does, promises a benefit the system cannot deliver.

### 4.2 A checkpoint delays 5.8× more than it costs

Two measurements of the same checkpoint disagree. The **capacity** it consumes —
arrivals the job failed to serve over the checkpoint's own window — is
`δ_cap` = 0.221 s, 2–3 % of capacity, stable from `τ` = 8 s to 64 s. The
**equivalent stall implied by latency**, from fitting `l0 + δ²/(2(1−ρ)τ)` to the
same runs, is `δ` = 1.277 s. A checkpoint delays 5.8× more record-seconds than
the throughput it removes: barrier alignment and buffered records age without
being dropped from the count. Since `δ` enters the optimum squared, feeding the
capacity figure into a latency objective understates the checkpoint term by a
factor of 34.

### 4.3 The outage is three and a half times what the engine reports

Flink reports a median 1.20 s from the `kill -9` to the restore. The oldest
record that emerges after the restart is 4.28 s old once its own checkpoint age
is subtracted (24 episodes, s.d. 0.5 s). The missing 3.1 s is the ramp back to
service — scheduling, reopening ~600 MB of RocksDB state, re-establishing the
Kafka fetches. The interval must cover the outage a record sees, not the one the
engine logs; using the reported figure understates the failure term by more than
an order of magnitude at small `τ`, since it enters squared.

The catch-up rate, by contrast, is close to nominal once the interval is in its
usable range: 104 000 records/s while still behind, 95 % of the calibrated `μ`.
It collapses to 78 000 (72 %) only at `τ` = 2 s, where checkpointing runs
continuously — so the depression is a checkpointing cost, not a cold-state one,
and we do not claim a cold-restore penalty.

### 4.4 Failure cost, predicted with no free parameter

For each episode we predict its excess latency from three quantities measured on
that same episode — peak latency, drain rate, arrival rate —

```
A = μ_d λ peak² / (2 (μ_d − λ))
```

Across 24 episodes spanning two orders of magnitude in cost the prediction tracks
the measurement at **R² = 0.999** with a median ratio of **1.18** (Fig. 2).
Regressing the measured excess on `(a + D)²`, with `a` the actual checkpoint age
at the kill, gives R² = 0.999 and a slope **1.001×** the parameter-free value.
The quadratic form is not an assumption that survived; it is the form the data
has.

Episodes count only where a steady level exists to measure excess against;
saturated and never-recovered runs are reported separately (§4.1, §4.6) rather
than pooled. Including them moves the slope to 0.94 — a warning about validating
a recovery model on runs that never recovered.

The prediction transfers across state size. At 100 MB the slope is 1.036× with
R² = 0.998, while `δ` falls from 1.277 s to 0.842 s and `D` from 4.28 s to
3.68 s; the model holds and its constants move as it says they should.

### 4.5 The optimum, and how far the classical rule is from it

With `δ` = 1.277 s, `D` = 4.28 s, `ρ` = 0.50:

| `M` | latency-optimal, closed form (1) | wasted-work, `ρ`-corrected | Young/Daly | Daly ÷ (1) |
|---|---|---|---|---|
| 30 s | 2.8 s | 5.1 s | 3.6 s | 1.3× |
| 55 s | 3.6 s | 7.0 s | 4.9 s | 1.4× |
| 10 min | 9.4 s | 22.9 s | 16.3 s | 1.7× |
| 1 h | 18.4 s | 56.2 s | 39.9 s | 2.2× |
| 1 day | 56.5 s | 275.3 s | 195.3 s | 3.5× |

The last column compares against Young/Daly as usually quoted; the `ρ`-corrected
wasted-work form is larger still, so the gap in the third column is the
conservative one.

The closed form agrees with the optimum obtained by composing the measured terms
to within 3 % at every `M`, and the seven runs at `M` = 55 s put the measured
minimum at an achieved interval of 4.4 s — one grid step from the predicted 3.6 s
(Fig. 1). At 100 MB of state the same comparison gives a measured minimum of
2.2 s against a predicted 2.6 s: the optimum moves with state size, in the
direction and by roughly the amount the model says. The gap to the classical
rule is modest when failures are frequent and
grows without bound as the cluster becomes reliable, because the wasted-work
answer grows as `M^{1/2}` exactly while the latency answer grows more slowly
(Fig. 3). Its local exponent `d log τ*/d log M` is 0.428 at `M` = 55 s, 0.390 at
10 min, 0.370 at 1 h, 0.350 at 1 day, and tends to 1/3 from above — the cube root
is the asymptote, not the exponent over the range anyone operates in.

We measure the wasted-work objective rather than only deriving it: redundant
processing rises from 2.9 % at `τ` = 2 s to 32.5 % at 64 s, within 0.9–2.1× of
the `age/M` prediction. That objective is real and measurable — it is simply not
the one a latency SLA expresses.

### 4.6 A ceiling, and what utilisation does to it

At `τ` = 128 s under `M` = 55 s failures the job has no steady state: mean latency
is 126 s and rising, because the backlog one failure leaves outlives the gap to
the next — at 50 % utilisation, with capacity to spare. Bound (2) puts the ceiling
at 82 s for these parameters; measured, `τ` = 64 s is stable at 13.6 s mean
latency and `τ` = 128 s is not.

Raising utilisation to `ρ` = 0.80 changes the character of the problem rather than
the location of the optimum. An interior optimum still exists — mean latency is
lowest at `τ` = 8 s — but every interval is unusable, from 21 s of mean latency at
the best one to 84 s at the worst, and at `τ` = 2 and 4 s the job does not keep up
with the input at all. Because these runs are saturated, their throughput is the
sustainable rate at that interval: 83.5 k records/s at `τ` = 2 s rising to 102.8 k
at `τ` = 128 s, a 23 % swing. **Utilisation is not well defined without naming the
checkpoint interval**, and the ceiling for this configuration lies between 0.5 and
0.8.

## 5. Limitations and conclusion

**One machine.** The intended second node was lost before this work began and
cannot be power-cycled remotely, so the cluster is a JobManager, three
TaskManagers and a broker as pinned processes on one host: process failure, state
restore, source rewind and catch-up are all real, but network distance between a
failed task and its replacement is missing, and it would add to `D` and move the
optimum up — our numbers are the optimistic end. One failure mode (`kill -9` of a
TaskManager), one workload shape, and a shared host whose background load we pin
around but cannot remove and whose disk throughput is not stationary (§3). `δ` is
fitted from the unsaturated, stable runs, with the excluded ones reported on
stated criteria rather than dropped.

The interval question for a stream processor is not the batch question with
different constants. A durable source turns lost work into replayed work, replay
competes with live arrivals for the same capacity, and the cost a latency SLA
feels is the area under the resulting backlog — quadratic in the checkpoint age
where the batch cost is linear. The optimum grows strictly more slowly than the
square root — exponent 0.43 falling to 0.35 over the practical range, 1/3 in the
limit — and the two answers separate by 2.2× at an MTBF of an hour and 3.5× at a
day. Two measurements matter as much as
the exponent: a checkpoint delays 5.8× more record-seconds than the capacity it
consumes, so a rule calibrated in capacity is calibrated in the wrong currency;
and the outage a record experiences is 4.3 s where the engine reports 1.2 s.
Both push the right interval down, and both are invisible to the classical
derivation.

## Figure captions

**Fig. 1** (`fig1_latency_vs_tau`). Mean end-to-end latency against the interval
actually achieved, reference workload, failures at one per 55 s. Points are
measured runs, one per requested interval; curves are (1) at three MTBFs,
evaluated from `δ` and `D` fitted without reference to these points. The arrow
marks the interval the wasted-work rule selects. The minimum is flat to its right
and steep to its left, so overshooting the interval is cheap and undershooting is
not.

**Fig. 2** (`fig3_area_validation`). Measured excess-backlog area per failure
episode against the parameter-free prediction `μ_d λ peak²/(2(μ_d − λ))`,
log–log, one point per episode, pooled over three state sizes and spanning two
decades of episode cost. The line is `y = x`, not a fit: every quantity on the
right is measured on the same episode as the left.

**Fig. 3** (`fig2_scaling`). Optimal interval against MTBF for both objectives,
log axes. The shaded region lies above the stability bound (2), where the backlog
one failure leaves outlives the gap to the next and no steady state exists. The
wasted-work answer grows as `M^{1/2}` and walks into that region; the latency
answer grows more slowly and does not.

(`fig0_trace` — latency and throughput through a single failure episode, showing
the checkpoint age, the outage and the catch-up ramp — is the clearest single
picture of the mechanism and should replace Fig. 2 if only three figures fit.)

## References

Placeholders describing what belongs there; fabricated citations are worse than
missing ones.

- [REF: Young 1974] first-order approximation to the optimum checkpoint interval.
- [REF: Daly 2006] the higher-order refinement, the form usually quoted.
- [REF: Flink checkpointing] asynchronous barrier snapshotting, the mechanism perturbed here.
- [REF: Kafka] the durable partitioned log whose replay semantics the argument rests on.
- [REF: unaligned checkpoints] the Flink mechanism evaluated as a robustness arm.
- [REF: streaming latency SLOs] a paper or industrial report motivating end-to-end latency as the objective.
