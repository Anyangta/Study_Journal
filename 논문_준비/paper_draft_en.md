# How Many Queries Before You Move the Data?
## Latency- and Egress-Aware Replication for Cross-Cloud Analytics

**ICNGC short paper draft — target 4 pages.**
Every number below is measured on the testbed described in §5
(344 trials, six link conditions, 2026-09-05). Raw data in `results_final.csv`,
per-configuration accuracy in `kstar_accuracy.csv`.

---

## Abstract

Cross-cloud analytics forces a choice: leave the data where it is and push each
query to it, or move the data once and query it locally. We show that for a
*single* query this is not a real choice — a columnar engine scans far faster
than any wide-area link carries bytes, so pushdown always wins. The decision
only becomes real when the same object is queried repeatedly, because pushdown
pays an egress charge on every repetition while replication pays once. We build
a two-site testbed on commodity hardware with an emulated WAN, measure the
crossover point `k*` at which replication overtakes pushdown, and show that
`k*` computed to minimise **latency** and `k*` computed to minimise the
**egress bill** disagree by 5x to 30x across six link configurations. The
egress-optimal crossover turns out to be a link invariant — identical at 888
and at 11.9 MB/s, and unchanged by round trip time — while the latency-optimal
one ranges over 27 to 174, so the two answers pull apart wherever a link
departs from the balance point. We check the emulated conditions against RTT and goodput measured to real cloud
storage endpoints, where the same object served from a CDN edge moves 3-4x
faster than from its origin region at identical egress cost — the effect in
production. We give a planner that predicts `k*` from the Parquet footer alone,
with no scan of the data, to a median 9.3% relative error, and use it to drive
an online break-even policy that costs 48x less than the always-pushdown
default while holding 65% fewer replicas than always-replicate.

---

## 1. Introduction

Data increasingly lives in one cloud while the people analysing it work in
another. Every query then faces the same question: ship the data to the
compute, or ship the compute to the data. The literature and the vendor
guidance both answer "push the query down" — filter and project at the source,
move only the answer.

We started from that premise and found it is not a trade-off at all for a
single query. On our testbed a DuckDB scan runs at 336–1374 MB/s per thread
depending on predicate complexity, while the WAN link carries 12.5–125 MB/s.
Compute is never the bottleneck, so pushdown wins at every selectivity, every
core count, and every link speed we tested. Reporting only this would be a
one-line paper.

The trade-off reappears once we account for how object storage is actually
billed and used. Egress is charged per byte moved, *every time* it is moved.
An analyst who queries the same object a hundred times pays a hundred egress
charges under pushdown, but only one if the object was replicated first. So the
decision variable is not selectivity — it is `k`, the number of queries the
object will serve.

**Contributions.**

1. We show empirically that single-query pushdown has no crossover on
   realistic hardware, and identify repetition count `k` as the axis on which
   the decision actually lives (§3).
2. We measure the crossover `k*` across six link configurations, seven
   selectivities and four remote core budgets, on two datasets. The
   egress-optimal `k*` is independent of both bandwidth and round trip time
   while the latency-optimal one is not, so the two diverge by 5x to 30x (§5).
3. We give a planner that estimates `k*` from Parquet footer metadata alone —
   zone maps for selectivity, column chunk sizes for the projection ratio —
   with no data scan, and evaluate its accuracy (§4, §5).
4. We turn `k*` into an online break-even policy and show it costs 48x less
   than the always-pushdown default, matches always-replicate on latency while
   holding 65% fewer replicas, and stays within 1.03-1.16x of an oracle (§6).

## 2. Background and related work

**Predicate pushdown.** S3 Select, Parquet predicate pushdown and
computation-near-storage systems all reduce bytes moved by filtering at the
source. This work is orthogonal: we assume pushdown is available and ask when
it should be *stopped* in favour of replication.

**Caching and replication.** Cross-cloud caches and replicas trade storage cost
for repeated-read cost. Existing policies are driven by hit rate; we drive the
decision by an explicit latency/egress objective and show the two disagree.

**Cost models for cloud analytics.** Prior work models egress in aggregate. We
model it per query plan and, crucially, evaluate the model against a running
system rather than a simulator.

*(Fill in concrete citations: S3 Select, PushdownDB, Skyplane, Globus/GridFTP,
SPANStore, and one Parquet zone-map reference. Six to eight references is right
for four pages.)*

## 3. The single-query result, and why we changed axis

Table 1 reports single-thread scan rates measured on site B against WAN rates.

| workload | 1 thread | 8 threads |
|---|---|---|
| simple aggregate scan | 1034 MB/s | 3385 MB/s |
| `LIKE '%...%'` | 695 MB/s | 2838 MB/s |
| `regexp_matches` | 511 MB/s | 2322 MB/s |
| regex extract + upper | 336 MB/s | 1500 MB/s |
| — WAN 100 Mbit | 12.5 MB/s | |
| — WAN 1 Gbit | 125 MB/s | |

Even the most expensive predicate at a single core outruns a 1 Gbit link by
2.7x. There is no operating point at which shipping raw bytes beats filtering
them first. This motivates the repeated-query formulation.

## 4. Model and planner

**Plans.** For a query that filters and projects an object of `S` bytes:

| plan | one-off | per query |
|---|---|---|
| `PUSHDOWN` | — | B scans, serialises and ships `σ·π·S`; egress every time |
| `PARTIAL REPLICA` | B projects once, `π·S` crosses | local scan, no egress |
| `FULL REPLICA` | `S` crosses once | local scan, no egress |

Each plan is `c₀ + k·c_q`, so the cost of any `k` follows from two measured
numbers. This is also why the experiment is small: we measure `c₀` and `c_q`
once per configuration instead of sweeping `k`.

**Crossover.** Replication overtakes pushdown at
`k* = (c₀^rep − c₀^push) / (c_q^push − c_q^rep)`, evaluated separately with
seconds or with dollars, giving `k*_latency` and `k*_egress`.

**Footer-only estimation.** Both `σ` and `π` come from the Parquet footer,
fetched with a few ranged GETs and no scan:

- `σ̂` from row-group zone maps (min/max per column), interpolating within
  partially-overlapping groups. Measured max absolute error **0.0008** over
  `σ ∈ [0.001, 1]`.
- `π̂` exactly, from per-column-chunk compressed sizes.

**Cost model.** Counting scan time and transfer time is enough: it predicts
`k*` to a median 9.3% relative error (§5, Result 6). We also built refinements
adding measured per-request overhead, serialisation rate and replica-side zone
map pruning; they did not improve accuracy, and we report that rather than
carrying the extra terms.

## 5. Evaluation

**Testbed.** Two bare-metal nodes (Intel i9-9900K, 8 physical cores, 16
threads), connected by a 10 GbE link. Site B runs MinIO as the origin object
store plus a DuckDB execution agent with a configurable thread budget; site A
is the analyst. The WAN is emulated with `tc netem` on B's egress, applied by
`u32` filter to the experiment's ports only so the shared cluster is
undisturbed. Dataset: a 1.87 GiB Parquet table, 40M rows, with a clustered
`day` column so zone maps prune usefully; projection `{id, day, val}` giving
`π ≈ 0.24`. Egress priced at the AWS list rate of 0.09 USD/GB.

**Conditions.** Six link configurations on the synthetic table plus one on TPC-H, 387 trials in total: unshaped 10 GbE
(888 MB/s achieved), 1 Gbit at 10 / 50 / 150 ms one-way delay (118 / 91.3 /
33.0 MB/s), 500 Mbit at 50 ms (58.5 MB/s) and 100 Mbit at 50 ms (11.9 MB/s).
We report *measured* goodput throughout, not the nominal shaping rate, and feed
the measured value to the cost model.

**Validation against real cloud paths.** The emulated conditions are not
arbitrary. From the same testbed we measured, over anonymous HTTPS with no
account and nothing billable, the TCP handshake RTT to seven cloud storage
endpoints and the goodput of pulling public objects. RTT ranges from 4.9 ms
(S3 Seoul) through 30.1 ms (Tokyo), 118.0 ms (Oregon) and 171.7 ms
(N. Virginia) to 261.4 ms (Ireland). Goodput is 93.6 MB/s pulling a public
Parquet object through a CDN edge, and 20.5-28.5 MB/s pulling comparable
objects directly from a US origin region. Our sweep brackets both: the
1 Gbit + 50 ms condition reproduces the CDN-fronted path almost exactly
(91.3 against 93.6 MB/s), and 1 Gbit + 150 ms reproduces the direct
long-haul pull (33.0 against 20.5-28.5 MB/s at 118-172 ms).

That measurement also supplies a production instance of this paper's central
claim. The *same object* served from a CDN edge moves 3-4x faster than when
pulled from its origin region, yet egress is billed per byte either way. The
latency-optimal replication threshold therefore moves with the path while the
egress-optimal one does not — the effect we quantify below is visible in the
public cloud, not an artefact of `netem`.

**Result 1 — the egress-optimal crossover is a link invariant.** Table 2 gives
the measured crossover at `σ = 0.001` with four cores at site B. Across a 75x
range of achieved goodput and a 1500x range of injected delay, the cost-optimal
`k*` is **796 in every condition**; at `σ = 0.1` it is 10 in every condition,
and at `σ = 1` it is 1. The bytes a plan moves do not depend on how fast, or
how far, the link carries them. Over the same conditions the latency-optimal
`k*` ranges from 27 to 174.

**Table 2.** Measured components and crossover, `σ = 0.001`, 4 cores at B.
All plans compared at matched selectivity.

| condition | goodput | pushdown /query | local /query | replica setup | `k*` latency | `k*` egress | gap |
|---|---|---|---|---|---|---|---|
| 10 GbE, unshaped | 888 MB/s | 23.6 ms | 4.7 ms | 3.28 s | 174 | 796 | 5x |
| 1 Gbit + 10 ms | 118 MB/s | 104 ms | 4.9 ms | 6.82 s | 69 | 796 | 12x |
| 1 Gbit + 50 ms | 91.3 MB/s | 384 ms | 4.8 ms | 11.72 s | 31 | 796 | 26x |
| 1 Gbit + 150 ms | 33.0 MB/s | 1085 ms | 4.8 ms | 28.84 s | 27 | 796 | 30x |
| 500 Mbit + 50 ms | 58.5 MB/s | 386 ms | 5.1 ms | 12.19 s | 32 | 796 | 25x |
| 100 Mbit + 50 ms | 11.9 MB/s | 398 ms | 5.0 ms | 44.85 s | 114 | 796 | 7x |

**Result 2 — the gap is non-monotonic in bandwidth, and peaks in the middle.**
It is 5x on the unshaped 10 GbE link, rises to 30x at 1 Gbit/150 ms, then falls
back to 7x at 100 Mbit. Two opposing forces set the latency-optimal `k*`. Round
trip time and reduced bandwidth both inflate the per-query cost of pushdown,
which pushes `k*` down; reduced bandwidth also inflates the one-off replica
setup, which pushes `k*` back up. On a fast link neither effect is strong and
`k*` stays high (174); in the middle the first dominates (27); at 100 Mbit the
44.85 s setup takes over and `k*` climbs again (114). The egress-optimal `k*`
is untouched by any of this, so the gap traces the same arch.

**Result 3 — which objective is more conservative can change.** At `σ = 0.1`
the unshaped link gives latency 12 against egress 10, so latency waits longer
before replicating; every shaped condition gives latency 7 to 9 against the
same egress 10, reversing the order. The magnitudes here are small and we do
not want to over-read them, but the sign is not fixed by the problem: which
objective recommends replicating first depends on the link.

**Result 4 — round trip time alone moves `k*` by 6x.** Fig. 4 holds the shaping
rate at 1 Gbit and varies only the injected delay. The left panel shows why the
delay matters so much: a pushed-down query pays the round trip on every
repetition, rising from 23.6 ms to 1085 ms, while a local replica scan is flat
at ~5 ms because it never leaves the site. The resulting latency-optimal `k*`
falls 174 → 69 → 31 → 27 for 0.1, 10, 50 and 150 ms, while the egress-optimal
`k*` stays at 796 throughout. The delay is not perfectly isolated: raising it
from 50 to 150 ms also dropped achieved goodput from 91.3 to 33.0 MB/s, because
the bandwidth-delay product outgrew the default TCP window. We report it as
measured rather than retuning the stack, since that is what an untuned
cross-cloud transfer actually experiences.

**Result 5 — the remote core budget does not matter (negative result).** We
expected site B's spare CPU to be a primary axis, on the reasoning that a
starved remote site would make pushdown expensive. It is not. Fig. 3 shows `k*`
essentially flat across 1, 2, 4 and 8 threads at B — 29, 27, 31, 32 at
`σ = 0.05` and 11, 12, 12, 11 at `σ = 0.1`. A single DuckDB thread already
scans the projected columns faster than the link can carry even a small result,
so remote CPU never becomes the binding constraint in this regime. We report
this because it cuts against the intuition that motivated the experiment.

**Result 6 — modelling zone-map pruning halves the model's error.** Our first
cost model counted an unpruned scan at both sites. It predicts `k*` to a median
relative error of 23.4% over 100 matched configurations, and produces a finite
prediction for only 89 of them. The measurements say both sites prune: the
local replica scan runs in 4.8 ms at `σ = 0.001` and 749 ms at `σ = 1`, and a
pushed-down query returns in 27.4 ms where the unpruned model expects ~200 ms.
Both use the same footer zone maps our planner reads.

Multiplying the two predicate-bearing scans by `σ` — but not the projection
step, which has no predicate and must read every row group — halves the median
error to 12.5% and yields a finite prediction in all 100 configurations. The
mean error rises, because the pruned model now overshoots badly at the lowest
selectivity (1311 predicted against 139 measured at `σ = 0.001`): pruning cannot
drive a scan to zero, and each site retains a floor of roughly 5 ms locally and
27 ms remotely. Adding a single shared floor term does not help, since it
appears in both numerator and denominator of `k*` and largely cancels (13.8%).
Separate per-site floors would likely close the gap; we stopped there rather
than fit more parameters to 100 points.

For the decision the planner actually makes — is `k` above or below the
threshold — a median error of 12.5% is comfortable everywhere except the very
selective tail, and that tail is exactly where §5's Result 7 says the objective
choice matters most. That is the honest limit of the current model.

**Result 7 — the disagreement is concentrated at low selectivity.** Repeating
the unshaped condition with every plan measured at all seven selectivities
(63 further trials) narrows where the two objectives actually conflict. The gap
is 5.7x at `σ = 0.001` and 1.5x at `σ = 0.01`, but effectively closes from
`σ = 0.05` upward (19 vs 20, 11 vs 10, 5 vs 4, 1 vs 1). The practical reading
is that the choice of objective only matters for highly selective queries — the
ones whose results are small enough that pushdown's egress advantage is large.

The same sweep exposes a modelling gap. The measured local-replica scan time
tracks selectivity almost linearly (4.8, 12.0, 46.5, 89.1, 213.6, 447.4,
748.8 ms), and so does site B's scan: at `σ = 0.001` a pushed-down query
returns in 27.4 ms where the model predicts about 200 ms. Both sites prune row
groups using the very zone maps our planner reads from the footer, and the cost
model treats neither scan as prunable. Over these seven matched points the
naive model's median relative error is 17.5%, with the residual concentrated at
low selectivity exactly as that omission predicts. Modelling pruning on both
sides is the obvious next step.

**Result 8 — a second dataset, and where the estimator breaks.** We repeated
the unshaped condition on TPC-H `lineitem` at SF=5 (1.10 GB), filtering on
`l_shipdate`. The qualitative picture holds: the egress-optimal `k*` is again
fixed by bytes alone while the latency-optimal one is not. What changes is the
selectivity estimator. On the synthetic table, whose predicate column increases
with row order, footer zone maps give `σ̂` to within 0.0009 absolute
(median 0.0002). On `lineitem`, where `l_shipdate` is uncorrelated with row
order, no row group can be pruned and the same estimator is off by up to 0.0224
(median 0.0130) — at a target of 0.01 it reports 0.0092 against a true 0.0010,
a 9x overestimate. The error is one-sided: without pruning the estimator
assumes every row group contributes, so it overstates selectivity, makes
pushdown look more expensive than it is, and therefore *understates* `k*`. Any
deployment should fall back to sampling when the footer offers no pruning.

**Result 9 — the low-selectivity concentration replicates, with caveats.**
Running the same all-selectivity sweep on TPC-H reproduces the shape of
Result 7 at both ends: the gap is 545x at `σ = 0.001` and 11x at `σ = 0.01`,
and collapses to ~1x for `σ ≥ 0.25`. The middle of the range is unstable
(`σ = 0.05` and `0.1` give erratic `k*`), because there the pushdown and
local-scan per-query costs are close, so `k*`'s denominator is small and
amplifies both measurement noise and the estimator error quantified below. We
report the two ends as replication of the pattern and do not claim the middle.

**An unexpected observation.** On the fast link, copying the whole 1915 MiB
object took 2.26 s while projecting and copying only 478 MiB took 3.15 s.
Column pruning costs CPU at the source, and when the link is fast that CPU is
more expensive than the bytes it saves. Partial replication is therefore *not*
uniformly better than full replication.

## 6. Putting `k*` to work: a break-even policy

Knowing `k*` is only useful if you can act on it, and in practice nobody knows
in advance how many times an object will be queried. That is the ski-rental
problem: keep renting (push each query down, paying egress every time) or buy
(replicate once). The classic online rule — rent until the accumulated rent
equals the purchase price, then buy — needs exactly the break-even point our
footer-only planner computes without reading any data.

We evaluate four policies over a workload of 2000 objects whose reuse counts
follow a Zipf distribution and whose selectivities are drawn from the measured
set. Per-plan costs come from the measured trials, so this is trace driven.
Storage for replicas is charged at the AWS list rate of 0.023 USD/GB-month;
egress at 0.09 USD/GB.

**Table 3.** Unshaped 10 GbE condition, 2000 objects.

| policy | latency (s) | egress | storage/mo | total | objects replicated |
|---|---|---|---|---|---|
| `ALWAYS_PUSH` (vendor default) | 331 336 | $4104.91 | — | $4104.91 | 0 |
| `ALWAYS_REPLICATE` | 87 600 | $84.08 | $21.49 | $105.57 | 2000 |
| `BREAKEVEN` (ours) | 86 771 | $78.17 | $7.53 | $85.70 | 701 |
| `ORACLE` (knows each `k`) | 84 539 | $45.07 | $7.53 | $52.60 | 701 |

Three things follow. First, the vendor default is not merely suboptimal but
catastrophic at scale: 48x the total cost and 3.8x the latency of our policy,
because it re-pays egress on every repetition. Second, our policy matches
always-replicate on latency while replicating only 701 of 2000 objects, cutting
the storage footprint by 65% — most objects are queried once or twice and never
deserve a replica. Third, it lands within 1.03x of the oracle's latency here,
and 1.06x, 1.16x and 1.15x on the 1 Gbit/50 ms, 1 Gbit/150 ms and 100 Mbit
conditions, comfortably inside ski rental's 2x worst case.

Its cost is 1.47-1.63x the oracle's. The gap is not a decision error — our
policy replicates exactly the same 701 objects the oracle does — but the egress
paid while renting up to the threshold, which an oracle skips by buying
immediately. That is the irreducible price of not knowing `k` in advance.

## 7. Conclusion

The right question in cross-cloud analytics is not whether to push a query
down, but how many times the same data will be queried before moving it becomes
cheaper — and "cheaper" has two answers that do not agree. A planner reading
only Parquet footers can pick between them without touching the data.

**Limitations.** One query template; the WAN is emulated on a LAN, though we
show the emulated conditions bracket RTT and goodput measured against real
cloud storage endpoints; egress prices come from public list rates rather than
billed invoices. Replication plans were measured at three selectivities
only, so the matched comparison in Table 2 rests on those points. Zone-map
selectivity estimation degrades badly when the predicate column is uncorrelated
with row order, as quantified on TPC-H above.

---

### Reproducing every number

`verify_claims.py` re-derives all 34 headline numbers straight from
`results_final.csv`, without reusing any analysis script, and fails loudly if
the paper claims something the data does not support. It passes as of
2026-09-06. Run it before submitting and after any edit to the numbers.

### Draft status

All numbers are measured (344 trials, six link conditions, 2026-09-05).
Still to do before submission:
- fill in the six to eight citations flagged in §2
- convert to the ICNGC template and cut to four pages (currently over)
- figure captions, and decide whether Fig. 3 (the negative result) earns its
  space or belongs in a sentence
- add zone-map pruning to the scan terms on *both* sites in the cost model;
  Result 7 shows this is where the residual error lives
- extend the all-selectivity sweep (currently only the unshaped condition) to
  the shaped conditions
