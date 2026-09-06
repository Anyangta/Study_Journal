# Checkpoint Intervals for Latency-Sensitive Stream Processing

*ICNGC short paper — 4 pages. Numbers are current as of the runs listed in
`summary.csv`; `verify.py` re-derives every one of them from the raw data.
`[REF: …]` markers are citations the author must supply.*

---

## Abstract

Stream processors checkpoint periodically, and the interval is usually chosen
with a rule inherited from batch computing: the Young/Daly optimum, which grows
as the square root of the mean time between failures. That rule answers the wrong
question here. With a durable source a failure does not destroy work, it rewinds
the source, and the rewound records must be re-read through whatever capacity is
left after the live input is served. The cost of a failure is therefore an area
of accumulated backlog, quadratic in the age of the last checkpoint where the
classical cost is linear, and the latency-optimal interval grows strictly more slowly than
`M^{1/2}`, with an exponent that falls from 0.43 to 0.34 over the practical
range of failure rates and approaches 1/3 asymptotically. We derive this and measure every term of it on Flink 1.20
with a Kafka source under injected TaskManager failures. Across 24 episodes
spanning two orders of magnitude in cost, the latency an episode adds is
predicted from quantities measured on that same episode, with no fitted
parameter, at R² = 0.999; the measured optimum is 4.4 s against a predicted
3.6 s, and at half the state size 2.2 s against 2.6 s. At a one-hour MTBF the
classical rule picks 39.9 s where the latency optimum is 18.4 s. Two
measurements explain most of the gap: a checkpoint delays 5.8× more
record-seconds than the throughput it removes, and the outage a record
experiences is 4.3 s where the engine reports 1.2 s.

## 1. Introduction

A stream processor with exactly-once state has to checkpoint, and someone has to
choose how often. The problem looks like the classical one — checkpoints cost
something, a failure costs something that grows with the age of the last
checkpoint, so there is an interior optimum — and the classical answer,
`τ* = sqrt(2 δ M)`, is what circulates as advice. [REF: Young 1974]
[REF: Daly 2006]

The inheritance does not survive inspection. Young and Daly assume a failure
destroys the work done since the last checkpoint and that this work is redone at
the rate it was first done. In a stream processor fed by a durable log, neither
holds. The work is not destroyed: the source rewinds to the offset in the
checkpoint and the records are read again. And the re-reading does not get the
machine to itself — the input keeps arriving during the outage and during the
catch-up, so replay runs on the capacity left over after the live stream is
served.

That changes the shape of the cost, not just its constants. The classical failure
penalty is proportional to the checkpoint age; here the backlog built over that
age has to be drained, and the area under the backlog curve — which is what an
end-to-end latency SLA sees — is quadratic in it. A quadratic term where the
classical derivation has a linear one drives the optimum below the classical
square root — to a cube root once the interval outgrows the outage, and to
something between the two before that. The two rules agree at one point and
diverge everywhere else, in the direction that matters: the more reliable the cluster, the further the
classical rule pushes the interval past what a latency-sensitive job can afford.

We contribute (i) a latency model of interval selection for stream processing
with a durable source, and a stability bound that neither classical rule can
express; (ii) a measurement of every term of that model on a real Flink/Kafka
deployment, with the service rate held fixed while state size varies 8× so that
checkpoint cost and restore cost move independently of utilisation; and (iii) two
measurement results that shift the answer by more than the exponent does — the
gap between what a checkpoint costs in capacity and what it costs in latency, and
the gap between the outage the engine reports and the one a record experiences.

## 2. Model

Let `λ` be the arrival rate, `μ` the service rate, `ρ = λ/μ`, `τ` the checkpoint
interval, `a` the age of the last completed checkpoint when a failure lands, and
`D` the effective outage — everything between the failure and the job serving at
rate again. Failures are independent of the checkpoint schedule, so
`a ~ U(0, τ)` and `E[(a+D)²] = τ²/3 + τD + D²`.

We take the objective to be the mean end-to-end latency over emitted records —
what a lag dashboard reports and what an SLA is written against. The choice
matters arithmetically: during catch-up the job serves at its full rate `μ_d`,
not at `λ`, so summing latency over records gives `μ_d/λ` times the
time-integrated backlog. Getting that factor wrong costs a factor of two.

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

For `D ≪ τ`, `τ* ≈ (1.5 δ² M)^{1/3}` — a cube root. For `D ≫ τ`,
`τ* ≈ δ sqrt(M/D)` — a square root, but with a constant the classical rule
lacks. Real deployments sit in the crossover: with the `δ` and `D` we measure,
the local exponent is 0.43 at a 55 s MTBF and 0.35 at a one-day MTBF, reaching
1/3 only when `τ*` is tens of times `D` (§4.5).
Utilisation nearly cancels in (1) since `μ_d ≈ μ`; it does not move the optimum
much, but it scales the cost at the optimum and, as §4.5 shows, collapses the
range of intervals that work at all.

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
because the host is shared with unrelated lab services; load average is logged
with every sample. The intended second node is unreachable — it answers ICMP and
completes TCP handshakes but no user-space service on it responds, and we have no
out-of-band power control (§5).

**Workload.** A generator emits fixed-rate records carrying an emission timestamp
and a key; the job keys on that field and keeps one `ValueState` entry of
configurable size per key. Entries are filled with a key-derived pseudo-random
pattern: a zero-filled payload is squashed by the backend's compression and does
not produce the checkpoint size configured — 200 MB of logical state checkpointed
as 36 MB before we caught this. A per-record spin loop sets the service rate;
without it the pipeline sustains over 10⁶ records/s and the harness, not the
system under test, is the bottleneck. Holding the spin fixed, `μ` stays within
93 384–112 469 records/s across an 8× change in state size, so state size moves
`δ` and `D` while leaving `μ` and `ρ` alone.

**Instrumentation.** Flink's `numRecordsOutPerSecond` and `records-lag-max` are
60-second moving averages and cannot resolve a checkpoint or a restart, so we do
not use them quantitatively. The operator instead emits, every 500 ms per
subtask, the count of records it processed and the distribution of their
`now − emission_timestamp` latency; those summaries are the primary instrument,
and record-weighted means over them are, by Little's law, the backlog areas the
model is written in. The driver polls the REST API at 4 Hz for job state and
checkpoint events only. Service rates are calibrated by pre-filling a Kafka
backlog with no consumer attached and reading the drain rate — the `μ` the
catch-up term refers to, measured rather than inferred.

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

Between 8 s and 64 s the steady-state latency falls as `1/τ` exactly as the model
says, from 265 ms to 98 ms, converging on a floor of `l0` = 57 ms (R² = 0.950).
Below about 8 s it turns around — 696 ms at `τ` = 4 s, 622 ms at 2 s — because a
checkpoint takes 2.1–2.8 s and one disturbance never drains before the next
begins. **The usable interval is bounded below by roughly twice the checkpoint
duration**, which is not a free parameter but a consequence of state size and
store bandwidth. Extrapolating the `1/τ` term below that bound, as the classical
rule implicitly does, promises a benefit the system cannot deliver.

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

Episodes are counted only where a steady level exists to measure excess against:
runs whose interval is below twice the checkpoint duration, and runs that never
returned to a steady level, are reported separately (§4.1, §4.6) rather than
pooled. Including them halves the apparent agreement — the slope moves to 0.94
and the 400 MB arm's R² to 0.95 — which is itself a warning about validating a
recovery model on runs that never recovered.

The prediction transfers across state size. At 100 MB the slope is 1.036× with
R² = 0.998, while `δ` falls from 1.277 s to 0.842 s and `D` from 4.28 s to
3.68 s; the model holds and its constants move as it says they should.

### 4.5 The optimum, and how far the classical rule is from it

With `δ` = 1.277 s, `D` = 4.28 s, `ρ` = 0.50:

| `M` | latency-optimal, closed form (1) | wasted-work | Young/Daly | ratio |
|---|---|---|---|---|
| 30 s | 2.8 s | 5.1 s | 3.6 s | 1.9× |
| 55 s | 3.6 s | 7.0 s | 4.9 s | 1.9× |
| 10 min | 9.4 s | 22.9 s | 16.3 s | 2.4× |
| 1 h | 18.4 s | 56.2 s | 39.9 s | 3.1× |
| 1 day | 56.5 s | 275.3 s | 195.3 s | 4.9× |

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

To keep the comparison honest we also measure the wasted-work objective rather
than only deriving it. Redundant processing — records read a second time after a
rewind — rises from 2.9 % at `τ` = 2 s to 32.5 % at `τ` = 64 s, within 0.9–2.1×
of the `age/M` prediction (median 1.3×; the excess is in-flight work between the
last barrier and the failure). That objective is real and measurable. It is
simply not the one a latency SLA expresses.

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

**One machine.** The second node of the intended testbed was lost before this work
began and cannot be power-cycled remotely. The cluster is one JobManager, three
TaskManagers and a broker as separate pinned processes on one host. Process
failure, state restore, source rewind and catch-up are all real; what is missing
is network distance between a failed task and its replacement, which would add to
`D` and move the optimum up. Our numbers are the optimistic end. **One failure
mode** (`kill -9` of a TaskManager), **one workload shape**, and a **shared host**
whose background load we pin around but cannot remove. `δ` is fitted from four
points, with the saturated and unstable runs excluded on stated criteria and
reported rather than dropped.

The interval question for a stream processor is not the batch question with
different constants. A durable source turns lost work into replayed work, replay
competes with live arrivals for the same capacity, and the cost a latency SLA
feels is the area under the resulting backlog — quadratic in the checkpoint age
where the batch cost is linear. The optimum grows strictly more slowly than the
square root — exponent 0.43 falling to 0.35 over the practical range, 1/3 in the
limit — and the two answers separate by 3× at an MTBF of an hour and 5× at a
day. Two measurements matter as much as
the exponent: a checkpoint delays 5.8× more record-seconds than the capacity it
consumes, so a rule calibrated in capacity is calibrated in the wrong currency;
and the outage a record experiences is 4.3 s where the engine reports 1.2 s.
Both push the right interval down, and both are invisible to the classical
derivation.

## Figures

- **Fig. 1** `fig1_latency_vs_tau` — mean latency against interval, model curves
  at three MTBFs with the measured points at `M` = 55 s and the classical rule's
  choice marked.
- **Fig. 2** `fig3_area_validation` — measured episode cost against the
  parameter-free prediction, log-log, pooled over three state sizes and spanning
  two decades of cost.
- **Fig. 3** `fig2_scaling` — optimal interval against MTBF for both objectives,
  with the region above bound (2) shaded.

(`fig0_trace` — one failure episode's latency and throughput — is the clearest
picture of the mechanism and should replace Fig. 2 if space allows only three.)

## References

Placeholders describing what belongs there; fabricated citations are worse than
missing ones.

- [REF: Young 1974] first-order approximation to the optimum checkpoint interval.
- [REF: Daly 2006] the higher-order refinement, the form usually quoted.
- [REF: Flink checkpointing] asynchronous barrier snapshotting, the mechanism perturbed here.
- [REF: Kafka] the durable partitioned log whose replay semantics the argument rests on.
- [REF: unaligned checkpoints] the Flink mechanism evaluated as a robustness arm.
- [REF: streaming latency SLOs] a paper or industrial report motivating end-to-end latency as the objective.
