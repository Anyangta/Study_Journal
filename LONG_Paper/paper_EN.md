<!-- =====================================================================
     영문 제출본 v2 — 2026-09-13
     =====================================================================
     v1(paper_EN_v1.md) 대비 변경:
     (1) 외부 검토본의 구조를 채택 (Table V 제거, 수식 번호 (1)~(10),
         IV.A~E 재구성, 초록 압축).
     (2) v1에서 규명해 넣었던 세 가지를 복원 — 빠지면 심사에서 걸립니다:
         (a) Table III↔IV의 d가 서로 다른 subset에서 나왔다는 공개 +
             100 MB fast에서 δ와 임계값이 다른 저장소 상태에서 측정됐다는 한계
         (b) 2노드 δ/d = 0.18–0.23 ("d/2는 일반 법칙이 아니다"의 유일한 근거)
         (c) Table IV의 측정점 개수 (4/4/10)
     (3) Figure 4의 ρ_d를 역산값 0.5563 → 측정값 0.5134로 교체하고 재생성.
         δ_cap = 0.22 s를 캡션에 명시.
     (4) 국소 지수를 0.43/0.35 → 0.41/0.36/0.34로 정정. 논문의 기준
         파라미터에서 재현되는 값이며, 격차 비율도 1.4/2.4/3.8 → 1.3/2.3/3.7
         (보수적 방향).

     ── 표·그림을 어디에 넣는가 (각 float 바로 앞 주석에 개별 지시) ──
     공통 규칙:
       · IEEE 2단: 표 캡션은 **위**, 그림 캡션은 **아래**.
       · 모든 float은 **본문에서 처음 인용된 뒤**에 와야 합니다. 앞서면 안 됩니다.
       · LaTeX은 [t] (단 상단)을 기본으로. 1단 폭이면 table/figure,
         2단 전체 폭이 필요하면 table*/figure*.
       · Table I·II·IV는 열이 많아 2단 폭(table*)이 읽기 편합니다.
       · Figure 1~4는 1단 폭(3.4 in)으로 충분합니다. PDF로 넣으세요(figs/*.pdf).
     ※ 최종 문장은 저자가 직접 검토·수정하십시오. 제출 전 이 주석 블록 삭제.
     ===================================================================== -->

# Checkpoint Interval Optimization for Latency-Sensitive Distributed Stream Processing

**[AUTHOR BLOCK — to be completed by the authors]**

[First Author]\*, [Co-author]\*\*, [Advisor]\*\*
\* Dept. of ..., \*\* Dept. of Computer Science & Engineering, Graduate School,
Dankook University, Yong-in, Republic of Korea
[emails], ymnah@dankook.ac.kr

---

## Abstract

*Abstract*— Stateful distributed stream processing systems checkpoint their state
periodically to enable fault recovery, which imposes an intrinsic trade-off between
checkpointing overhead and recovery latency. This paper formulates checkpoint-interval
selection directly in terms of **record-weighted latency** under durable replay. When a
failure occurs, live input continues to arrive while previously processed records are
replayed; the resulting **replay backlog** therefore drains only at the **residual service
capacity** (μ_d − λ) rather than at the nominal processing rate. Under a linear
backlog-drain approximation, this mechanism yields a recovery cost that grows
**quadratically** with checkpoint age. Combining this quadratic failure term with the
steady-state checkpointing cost produces a latency-optimal interval whose dependence on the
mean failure interval M approaches M^(1/3), in contrast to the √M scaling exhibited by a
**capacity/wasted-work baseline**. We assess the underlying **backlog-drain geometry** on
Apache Flink and Kafka. A geometric estimate derived exclusively from same-episode
measurements contains **no data-fitted model parameters** and exhibits a strong linear
association with the measured excess-latency area: R² = 0.996 with a slope of 1.107 in the
reference condition, and R² = 0.982 with a slope of 1.092 in a second condition. These
results constitute a **structural validation** of the proposed backlog-drain model;
independent validation of the predicted global optimum remains future work.

*Keywords*— Checkpoint Interval Optimization, Record-Weighted Latency, Replay Backlog,
Distributed Stream Processing, Fault Recovery, Apache Flink

---

## I. INTRODUCTION

Stateful distributed stream processors underpin latency-sensitive workloads such as
real-time analytics, financial transaction processing, and IoT monitoring. Frameworks such
as Apache Flink persist operator state and source progress periodically so that execution
can resume from a consistent state after a failure. Although checkpointing is indispensable
for fault tolerance and exactly-once semantics, it consumes CPU, storage, and I/O resources
and can transiently inflate record latency. The checkpoint interval therefore governs a
fundamental trade-off between normal-operation overhead and recovery cost.

A short interval curtails the state and source progress that must be replayed after a
failure, but raises checkpoint frequency and steady-state interference. A long interval
attenuates checkpointing overhead, yet inflates the checkpoint age at failure and hence the
volume of replayed data. Classical checkpoint/restart models, notably those of Young [1] and
Daly [2], resolve this trade-off predominantly through wasted computation or utilization.
Stream-specific studies have since incorporated workload intensity, processing efficiency,
recovery time, and tuple latency [3]–[6].

The distinction drawn in this work is not that classical systems restart whereas stream
processors replay; both may roll back to a prior checkpoint. Rather, a durable streaming
source induces a specific recovery dynamic: **new records continue to arrive while the
replay backlog is being drained.** If the recovery processing rate is μ_d and the live
arrival rate is λ, the backlog recedes not at μ_d but at the residual service capacity
μ_d − λ. This continuous-arrival effect reshapes the recovery-cost geometry experienced by
individual records.

We therefore formulate checkpoint optimization directly from record-weighted excess latency.
The resulting model establishes that the failure cost grows quadratically with checkpoint
age, because a longer replay span simultaneously enlarges the initial backlog and protracts
the interval over which that backlog delays subsequent records. Combined with the
latency-equivalent cost of steady-state checkpointing, the resulting optimum exhibits an
asymptotic scaling distinct from the conventional capacity/wasted-work criterion.

This paper makes three contributions. **First**, it derives a record-weighted latency model
for durable replay and establishes that the failure penalty is quadratic in checkpoint age.
**Second**, it combines this penalty with steady-state checkpoint interference to obtain a
latency-optimal interval together with its asymptotic scaling. **Third**, it assesses the
proposed backlog-drain geometry on Apache Flink/Kafka using same-episode measurements,
thereby separating structural validation of the recovery mechanism from out-of-sample
validation of the global optimum.

## II. RELATED WORK

### A. Classical Checkpoint-Interval Models

Young [1] derived a first-order approximation of the optimal checkpoint interval by
balancing checkpointing overhead against the expected recomputation incurred after a
failure. Daly [2] subsequently supplied a higher-order formulation. Together these
established the classical view of checkpoint selection as a trade-off between checkpoint
cost and lost work.

Distributed snapshot mechanisms address a related but distinct problem: capturing a globally
consistent state. Chandy and Lamport [8] introduced the foundational distributed snapshot
algorithm, which later stream-processing systems adapted to continuously executing
dataflows. Carbone et al. [7] proposed asynchronous barrier snapshotting for distributed
dataflows and subsequently documented Flink's state-management and recovery architecture in
detail [10].

### B. Checkpoint Optimization in Stream Processing

Stream-specific work has extended conventional checkpoint models to account for continuous
processing. Zhuang et al. [3] modeled an online checkpoint-interval adjustment mechanism in
which recovery cost is contingent on the stream-processing workload. Jayasekara et al. [4]
derived a utilization-based checkpoint model for distributed stream processors and validated
it on Apache Flink; subsequent work examined checkpoint-induced latency and throughput
effects [6]. Zhang et al. [5] modeled workload-dependent tuple latency and recovery
behaviour for Flink applications. Sebepou and Magoutis [9] studied checkpointing techniques
that overlap state persistence with ongoing stream processing.

Our focus is complementary. Rather than casting the checkpoint objective primarily as
utilization, processing efficiency, or workload-dependent recovery time, we derive the
failure term directly from the **backlog-drain geometry experienced by records**. The
governing quantity is thus the accumulated record-weighted latency attributable to durable
replay under continuous arrivals.

<!-- ▣ TABLE I 배치: 여기(II.B 끝, 첫 인용 직후). 열이 4개라 2단 폭(table*) 권장.
     캡션은 표 위. -->

**TABLE I. COMPARISON WITH RELATED CHECKPOINT-INTERVAL MODELS**

| Work | Primary Objective | Validation | Distinction |
|---|---|---|---|
| Young [1], Daly [2] | Wasted work / checkpoint cost | Analytical | Classical checkpoint/restart |
| Zhuang et al. [3] | Processing efficiency | Simulation + datasets | Workload-dependent recovery |
| Jayasekara et al. [4], [6] | Utilization | Flink experiments | System-efficiency objective |
| Zhang et al. [5] | Tuple latency + recovery | Flink experiments | Workload-dependent latency |
| **This work** | **Record-weighted latency** | **Flink/Kafka experiments** | **Explicit quadratic backlog-drain cost** |

## III. LATENCY-BASED CHECKPOINT MODEL

### A. System Model

Figure 1 depicts the system model. Kafka furnishes a durable input log, and Flink records
operator state and source offsets periodically. Following a failure, execution resumes from
the most recently completed checkpoint and records beyond that point are replayed.

<!-- ▣ FIGURE 1 배치: 여기(III.A, 첫 인용 직후). 1단 폭 3.4 in, [t] 상단 고정.
     figs/fig_arch.pdf 사용. 캡션은 그림 아래. -->

![Figure 1](figs/fig_arch.png)

**Fig. 1.** Experimental and recovery architecture comprising a durable Kafka source, Flink
stateful processing, and local or remote checkpoint storage.

Let λ denote the input rate, μ the nominal maximum processing rate, and ρ = λ/μ the
utilization. Let τ denote the checkpoint interval and a the age of the most recently
completed checkpoint when a failure occurs. D denotes the **effective outage**, which
captures the observable recovery gap employed by the model rather than pure engine downtime.
During catch-up the processing rate is μ_d, and M denotes the mean failure interval.

At recovery, the volume of data requiring replay is approximated by

```
B₀ = λ(a + D)                                                              (1)
```

Because live records continue to arrive at rate λ, backlog reduction proceeds solely through
the residual service capacity μ_d − λ. Provided μ_d > λ, the catch-up time is

```
T_c = λ(a + D) / (μ_d − λ)                                                 (2)
```

This residual-capacity effect is the central mechanism of the proposed model.

### B. Record-Weighted Recovery Cost

Immediately after recovery, the oldest affected records exhibit approximately a + D seconds
of excess latency. Under an approximately linear backlog drain, record latency recedes toward
the steady-state level as the backlog is cleared. The resulting latency–time trajectory is
therefore approximately **triangular**, as illustrated in Figure 2.

<!-- ▣ FIGURE 2 배치: 여기(III.B). 1단 폭, [t]. figs/fig0_trace.pdf.
     Figure 1과 같은 쪽에 몰리면 하나를 다음 단 상단으로 밀 것. -->

![Figure 2](figs/fig0_trace.png)

**Fig. 2.** Representative recovery episode. Excess record latency recedes as the replay
backlog is drained.

Because records are served at approximately μ_d during catch-up, the accumulated
record-weighted excess latency is

```
A = μ_d · ½(a + D) · T_c = μ_d·λ·(a + D)² / (2(μ_d − λ))                   (3)
```

Equation (3) exposes the defining property of the model: recovery cost is **quadratic**,
rather than linear, in checkpoint age. Increasing a enlarges not merely the initial replay
backlog but also the duration over which that backlog delays subsequent records.

### C. Mean-Latency Objective

Assuming failures are independent of the checkpoint schedule, a ~ U(0, τ), and hence

```
E[(a + D)²] = τ²/3 + τD + D²                                               (4)
```

Let δ denote the **latency-equivalent checkpoint cost**, estimated from the steady-state
latency increase induced by checkpointing. The resulting mean-latency objective is

```
L̄(τ) = ℓ₀ + δ²/(2(1 − ρ)τ) + [μ_d/(2(μ_d − λ)M)]·(τ²/3 + τD + D²)          (5)
```

The first variable term is decreasing in τ whereas the recovery term is increasing; an
interior optimum therefore exists. Differentiating (5) and defining ρ_d = λ/μ_d yields

```
δ²·M·(1 − ρ_d)/(1 − ρ) = τ²·(2τ/3 + D)                                     (6)
```

When D ≪ τ,

```
τ* ≈ (1.5·δ²M)^(1/3)                                                       (7)
```

so the latency-optimal interval asymptotically obeys an M^(1/3) scaling law.

For comparison we adopt a capacity/wasted-work reference model,

```
τ*_cap ≈ √(2·δ_cap·M/ρ)                                                    (8)
```

where δ_cap is the capacity-equivalent checkpoint loss measured from the throughput deficit.
Equation (8) follows the conventional √M dependence. It serves solely as a capacity-based
reference and is not claimed to reproduce the exact utilization optimum of Jayasekara
et al. [4].

### D. Mean-Load Stability

A checkpoint interval is meaningful only insofar as the replay backlog can be drained
sufficiently quickly. Substituting the mean checkpoint age τ/2 yields the first-moment
stability approximation

```
λ(τ/2 + D)/(μ_d − λ) + D < M                                               (9)
```

This expression is not a deterministic bound applicable to every failure episode; it
characterizes the mean-load regime in which backlog accumulation is not expected to persist
across failures.

## IV. EXPERIMENTAL EVALUATION

### A. Experimental Setup

Experiments were conducted on Apache Flink 1.20.5 and Kafka 3.9.2. The single-node campaign
employed local checkpoint storage, whereas the distributed configuration used two servers
with MinIO as S3-compatible remote checkpoint storage. Flink was configured with RocksDB and
full, aligned, exactly-once checkpoints.

<!-- ▣ TABLE II 배치: 여기(IV.A 첫 문단 직후). 2열이지만 행이 10개라 1단 폭으로 충분.
     캡션은 표 위. 지면이 빡빡하면 이 표를 본문 2~3문장으로 흡수 가능. -->

**TABLE II. EXPERIMENTAL CONFIGURATION**

| Item | Configuration |
|---|---|
| Computing nodes | 8-core server × 2 |
| Stream processor | Apache Flink 1.20.5 |
| Source | Apache Kafka 3.9.2, KRaft |
| State backend | RocksDB, full/non-incremental |
| Checkpoint mode | Exactly-once, aligned |
| State size | approx. 100 / 200 / 400 MB |
| Utilization ρ | 0.30–0.80 |
| Main campaign | 255 single-node runs |
| Distributed campaign | approx. 22 two-node runs |
| Checkpoints | more than 13,000 |

The workload was synthetic so that processing demand and state size could be varied
independently. CPU demand was governed by a per-record spin loop, while state size was varied
through the number of keyed state entries. Record latency was instrumented within the
application as the elapsed time between generation and processing, and a record-weighted mean
was collected every 500 ms.

Failures were injected by terminating and restarting a TaskManager. Inter-failure gaps
followed an exponential-gap renewal process with a 40-s minimum separation; the configured
mean was 55 s, whereas the measured mean inter-failure interval was approximately 72.8 s.
Analyses requiring M therefore employ the measured interval rather than the nominal value.

Runs were excluded from the primary model analysis when the checkpoint interval entered the
empirically saturated regime, the processor failed to sustain the input rate, recovery did
not return to steady state, or the episode was censored before backlog drainage completed.
The criterion τ < 2d, where d is the checkpoint duration, serves solely as an **empirical
analysis threshold** for obtaining a separable steady-state 1/τ regime; it is not a physical
lower bound imposed by Flink. Approximately 34% of the collected runs satisfied the primary
analysis criteria.

Checkpoint age was computed from the completion time of the latest successfully completed
checkpoint. Consequently D must be construed as an effective outage rather than as pure
engine downtime.

### B. Structural Validation of the Backlog-Drain Geometry

For each usable failure episode, the input rate λ, the recovery processing rate μ_d, and the
peak observed latency p were measured **from that same episode**. The geometric estimate

```
A_geo = μ_d·λ·p² / (2(μ_d − λ))                                           (10)
```

contains no parameters fitted to the measured latency area.

Because p is itself measured on the episode under evaluation, this test is deliberately a
**structural validation** rather than an out-of-sample prediction of a future failure. The
linear regression used to summarize agreement fits a slope and an intercept; the reported R²
therefore characterizes the association between the parameter-free geometric estimate and the
measurement, and does not constitute a parameter-free R².

For 30 episodes in the 100k-key, ρ = 0.50 condition the comparison yields R² = 0.996 with a
slope of 1.107. For 61 episodes in the 200k-key, ρ ≈ 0.30 condition it yields R² = 0.982 with
a slope of 1.092.

Figure 3 aggregates 218 usable episodes across three state sizes: 31 episodes at 100k keys,
168 at 200k, and 19 at 400k. The measured excess-latency area is approximately 1.21× the
geometric estimate in the median, and the association persists across roughly four orders of
magnitude. The proposed geometry thus captures the dominant recovery-cost structure, while
systematically underestimating its magnitude by approximately 20%.

<!-- ▣ FIGURE 3 배치: 여기(IV.B 끝). 논문에서 가장 중요한 그림 — 1단 폭, [t],
     그리고 **본문 IV.B와 같은 쪽**에 오게 하십시오. figs/fig3_area_validation.pdf.
     지면이 부족해도 이 그림은 마지막까지 유지. -->

![Figure 3](figs/fig3_area_validation.png)

**Fig. 3.** Structural validation of the backlog-drain geometry. The abscissa is the
parameter-free geometric estimate computed from same-episode λ, μ_d, and peak latency; the
ordinate is the measured record-weighted excess-latency area. The solid line denotes the
median measured-to-estimated ratio (1.21×) and the dashed line denotes y = x.

### C. Latency-Equivalent Checkpoint Cost

Checkpoint cost depends materially on the metric by which it is quantified. The
capacity-equivalent loss obtained from the throughput deficit during checkpointing was
approximately 0.15–0.22 s. By contrast, the latency-equivalent parameter δ, estimated from
steady-state latency as a function of 1/τ, ranged from 1.37 to 4.16 s.

<!-- ▣ TABLE III 배치: 여기(IV.C, 첫 인용 직후). 7열이라 2단 폭(table*) 권장.
     캡션 위. 이 표는 끝까지 유지 — R² 열이 표에만 있는 정보입니다. -->

**TABLE III. LATENCY-EQUIVALENT CHECKPOINT COST**

| State | Storage | ρ | δ (s) | d (s) | δ/d | R² |
|---|---|---|---|---|---|---|
| 100 MB | fast | 0.50 | 1.371 | 3.08 | 0.45 | 0.979 |
| 200 MB | fast | 0.50 | 1.907 | 3.12 | 0.61 | 0.863 |
| 100 MB | slow | 0.30 | 3.195 | 5.24 | 0.61 | 0.967 |
| 200 MB | slow | 0.30 | 4.159 | 9.67 | 0.43 | 0.791 |

Across the single-node local-storage conditions examined, δ/d lies between 0.43 and 0.61. In
the two-node remote-storage configuration the same ratio was 0.18–0.23; d/2 may therefore
serve as a coarse empirical initializer for δ in local configurations comparable to those
tested, but it is **not a general relationship**. Storage performance and utilization also
co-vary across the conditions of Table III, so their individual contributions cannot be
disentangled from that table alone.

The larger latency-equivalent cost is consistent with checkpointing perturbing record latency
even when processing does not halt outright: barrier propagation, state snapshotting, and
transient stalls can delay a great many records while producing only a modest aggregate
throughput deficit.

### D. Optimal-Interval Scaling and Observability

Figure 4 contrasts the latency-optimal checkpoint interval with the capacity/wasted-work
baseline as the mean failure interval M varies. For the reference parameters δ = 1.907 s,
D = 4.28 s, ρ = 0.5 and ρ_d = 0.513, the local logarithmic exponent of the latency model is
approximately 0.41 at M = 55 s, 0.36 at one hour, and 0.34 at one day, approaching the
asymptotic value 1/3. The capacity-based reference follows the 1/2 exponent of √M, evaluated
with δ_cap = 0.22 s. The separation between the two predicted intervals widens accordingly,
from approximately 1.3× at M = 30 s to 2.3× at one hour and 3.7× at one day.

<!-- ▣ FIGURE 4 배치: 여기(IV.D). 1단 폭, [t]. figs/fig2_scaling.pdf.
     결론을 담는 그림이므로 Figure 3과 함께 끝까지 유지.
     주의: 이 그림은 make_fig4_scaling.py로 재생성됩니다. ρ_d를 바꾸면
     본문의 지수(0.41/0.36/0.34)와 비율(1.3/2.3/3.7)도 같이 고쳐야 합니다. -->

![Figure 4](figs/fig2_scaling.png)

**Fig. 4.** Model-derived checkpoint interval as a function of the mean failure interval M.
Curves use the reference parameters δ = 1.907 s, D = 4.28 s, ρ = 0.5, ρ_d = 0.513 and
δ_cap = 0.22 s. The capacity/wasted-work reference follows M^(1/2), whereas the
record-weighted latency optimum approaches M^(1/3). The shaded region denotes violation of
the mean-load stability approximation.

The salient implication is not merely that the two models yield different numerical
intervals: **their scaling laws differ.** The divergence between the two predictions
consequently widens as failures become less frequent.

The location of the latency optimum was further compared against the checkpoint intervals
that were empirically observable in each condition.

<!-- ▣ TABLE IV 배치: 여기(IV.D). 7열이라 2단 폭(table*) 권장. 캡션 위.
     지면이 부족하면 이 표를 본문 2~3문장으로 흡수할 수 있으나, 바로 아래
     "subset이 다르다"는 문단은 반드시 남겨야 합니다. -->

**TABLE IV. PREDICTED OPTIMUM AND EMPIRICALLY OBSERVABLE RANGE**

| Condition | δ (s) | D (s) | M (s) | Pred. τ* (s) | 2d (s) | Measured τ (points) |
|---|---|---|---|---|---|---|
| 100 MB, fast, ρ = .50 | 1.37 | 2.76 | 69 | 4.6 | 2.9 | 4–32 (4) |
| 200 MB, fast, ρ = .50 | 1.91 | 4.65 | 142 | 7.3 | 6.2 | 16–68 (4) |
| 200 MB, slow, ρ = .30 | 4.16 | 10.39 | 67 | 8.3 | 17.1 | 21–89 (10) |
| 100 MB, slow, ρ = .30 | 3.20 | — | — | — | — | unavailable |

The D values and the analysis threshold 2d reported in Table IV were measured on each
condition's τ*-analysis runs, that is, on the failure runs; they consequently differ from the
d of Table III, which aggregates the failure-free runs used to estimate δ, and from the
reference D employed in Figure 4. For the 100 MB fast condition in particular, the runs from
which δ was obtained exhibit d = 3.08 s whereas the runs in which the optimum was observed
exhibit d = 1.45 s, so **the two quantities were measured under different storage states.**

For the 100 MB fast condition, measured latency was lowest at τ = 4.2 s, close to the
predicted 4.6 s. That point, however, lies at the left boundary of the measured grid, so only
the increasing branch to the right of the candidate optimum was observed; nor was the
agreement established within runs sharing a common storage state. This therefore constitutes
supporting evidence rather than an independent identification of the global optimum.

For the remaining conditions the predicted optimum fell below either the measured interval
range or the empirical 2d analysis threshold. That threshold is not a Flink operating limit;
it demarcates the range within which the steady-state checkpoint-cost regression could be
reliably separated with the present instrumentation.

Protracted experimentation additionally exposed substantial storage drift: checkpoint write
throughput declined from approximately 288 MB/s to 63 MB/s over 31 h, while the checkpoint
duration grew from roughly 2–3 s to 8–10 s. SLC-cache exhaustion was entertained as a
possible explanation but was not independently verified. Checkpoint intervals were
accordingly randomized and fixed-interval control runs interleaved so as to attenuate
ordering bias.

A smaller two-node experiment employed remote MinIO checkpoint storage. The median engine
restore time increased from approximately 1.20 s in the single-node configuration to 2.39 s
across 12 two-node failure episodes. Because node topology and checkpoint-storage backend
were altered concurrently — and because the single-node comparison values were obtained once
the checkpoint duration had already grown to 8–9 s under storage drift — this difference is
reported solely as a configuration-level observation and is not attributed causally to
network placement.

### E. Limitations

The experiments substantiate the recovery-cost geometry more firmly than the location of the
global latency optimum. Independent held-out validation of τ* was planned under a
**pre-specified** calibration-and-freeze protocol. Ten calibration runs in the two-node
remote-storage configuration, however, yielded a checkpoint duration of 4.13 s and an
empirical analysis threshold of 8.26 s. The preliminary candidate optimum fell below that
threshold, while the calibration parameters proved insufficiently stable for independent
validation: only four points remained in the 1/τ regression (R² = 0.04), and only two usable
failure episodes remained for estimating recovery quantities. **The validation sweep was
therefore terminated before any validation data were collected.**

The evaluation further relies on a synthetic workload and on TaskManager termination as the
principal failure mode. Approximately 34% of runs satisfied the primary analysis criteria,
and substantial run-to-run variation was observed under certain storage conditions. The
quantitative parameter values should accordingly be construed as configuration-specific;
broader validation across storage systems, workloads, node counts, and failure modes remains
necessary.

## V. CONCLUSION

This paper presented a checkpoint-interval model for latency-sensitive distributed stream
processing predicated on record-weighted recovery latency. With a durable source, new records
continue to arrive during replay, so the recovery backlog drains only at the residual service
capacity μ_d − λ. This mechanism induces a quadratic recovery penalty in checkpoint age.
Combining that penalty with the latency-equivalent cost of steady-state checkpointing yields
a latency-optimal interval whose asymptotic dependence on the failure interval approaches
M^(1/3), in contrast to the √M scaling of a capacity/wasted-work reference.

Apache Flink/Kafka experiments furnish structural support for the proposed backlog-drain
geometry. In representative conditions the parameter-free geometric estimate exhibits
R² = 0.996 with a slope of 1.107, and R² = 0.982 with a slope of 1.092, against the measured
record-weighted excess-latency area. Under the tested conditions the latency-equivalent
checkpoint cost was likewise substantially larger than the corresponding capacity-equivalent
loss.

The predicted optimum itself has not yet been independently validated across checkpoint
intervals. Future work will therefore target configurations in which the candidate optimum
lies within the empirically observable regime, while varying storage backend, cluster
topology, workload, and failure mode independently.

## ACKNOWLEDGMENT

[Insert the funding/project acknowledgment supplied by the advisor or laboratory, if
applicable.]

## REFERENCES

<!-- ▣ REFERENCES: IEEE 형식. [8][9][10]의 권·호·쪽수는 반드시 직접 확인하십시오.
     특히 [9]는 쪽수를 비워 두었습니다. -->

[1] J. W. Young, "A first order approximation to the optimum checkpoint interval,"
*Communications of the ACM*, vol. 17, no. 9, pp. 530–531, 1974.

[2] J. T. Daly, "A higher order estimate of the optimum checkpoint interval for restart
dumps," *Future Generation Computer Systems*, vol. 22, no. 3, pp. 303–312, 2006.

[3] Y. Zhuang et al., "An optimal checkpointing model with online OCI adjustment for stream
processing applications," in *Proc. ICCCN*, 2018, pp. 1–9.

[4] S. Jayasekara et al., "A utilization model for optimization of checkpoint intervals in
distributed stream processing systems," *Future Generation Computer Systems*, vol. 110,
pp. 68–79, 2020.

[5] Z. Zhang et al., "Research on optimal checkpointing-interval for Flink stream processing
applications," *Mobile Networks and Applications*, vol. 26, no. 5, pp. 1950–1959, 2021.

[6] S. Jayasekara et al., "Optimizing checkpoint-based fault-tolerance in distributed stream
processing systems: Theory to practice," *Software: Practice and Experience*, 2022.

[7] P. Carbone et al., "Lightweight asynchronous snapshots for distributed dataflows,"
arXiv:1506.08603, 2015.

[8] K. M. Chandy and L. Lamport, "Distributed snapshots: Determining global states of
distributed systems," *ACM Transactions on Computer Systems*, vol. 3, no. 1, pp. 63–75,
1985.

[9] Z. Sebepou and K. Magoutis, "CEC: Continuous eventual checkpointing for data stream
processing operators," in *Proc. IEEE/IFIP Int. Conf. Dependable Systems and Networks*,
2011.

[10] P. Carbone et al., "State management in Apache Flink: Consistent stateful distributed
stream processing," *Proc. VLDB Endowment*, vol. 10, no. 12, pp. 1718–1729, 2017.
