# Experiments X1–X7: reproducible commands and evidence

Run from a **clean repository root** after `python -m pip install -r requirements.txt`. Dispatcher: `python -m experiments {x1|x2|x3|x4|x5|x6|x7}`. For CI, inspect `.github/workflows/audit-smoke.yml`; each experiment reports its own checked preconditions and scoped guarantees.

| Requirement | Entry point | Configuration and principal outputs | Main interpretation |
|---|---|---|---|
| **X1** aggregation | `python -m experiments x1` | `configs/x1_aggregation.json`, `results/x1_aggregation/{oracle,convergence,ops,iterations,events}.csv`, `summary.json` | Same round snapshot for all workers; analytic target-distance contraction |
| **X2** scaling | `python -m experiments x2` | `configs/x2_scaling.json`, `results/x2_scaling/{trials,scaling}.csv`, `scaling.png` | Fixed seeded sparse workload, 1–8 separate server processes |
| **X3** consistency | `python -m experiments x3` | `results/c5_evaluation/{summary,convergence}.csv`, four plots | Sequential vs bounded τ=1,2,4 vs eventual; with/without straggler |
| **X4** optimisation | `python -m experiments d3`, then `python -m experiments x4` | D1/D2 historical logs + `results/d3_benchmark/comparison.csv`, `results/x4_comparison/{comparison.csv,comparison.png,summary.json}` | Bytes vs latency for cache, compression, batching |
| **X5** elasticity | `python -m experiments x5` | Timestamped `results/e4_elasticity/trial_*/` directories: ops, events, timeseries, plots, summary | 2→4 servers under load; in-process movement |
| **X6** failover | `python -m experiments x6` | `configs/x6_failover.json`, `results/x6_failover/summary.json`, event/ops/iteration CSV | Real process crash after Pull/before Push, independent fixed-gradient oracle |
| **X7** failure matrix | `python -m experiments x7` (after X6) | `configs/x7_fault_matrix.json`, `results/x7_fault_matrix/{matrix.csv,summary.json}`, logs | Nine fault cases; document recovered vs fail-closed |

Use `python -m experiments ...` as the canonical launcher for every experiment. Redundant old launchers have been removed. **Some experiments overwrite files in `results/`; preserve the prior submitted evidence elsewhere before rerunning.**

## X1: analytic multiworker aggregation

Seeded, zero-initialised target-difference SGD with synchronous round snapshots and no failures. Let `m` be workers and `η` the learning rate. A round applies `m` gradients computed from the same `w_t`, so for target `w*`:

```text
w_(t+1) - w* = (1 - ηm) · (w_t - w*)
||w_t - w*|| = |1 - ηm|^t · ||w_0 - w*||
```

The independent oracle checks the distance every round. Earlier clean-checkout CI measured a maximum absolute oracle difference of about **4.44e-16** for the recorded default configuration. This validates the *explicit round-synchronous baseline*, not necessarily every asynchronous execution schedule or C4's independently implemented manager gate.

## X2: 1–8-server scaling

`configs/x2_scaling.json` fixes 1,024 width-2 model keys, four workers, 20 iterations each, 32 sparse keys/iteration, seeds 42/43/44, a shared application-level RNG schedule, no replication/faults and eventual consistency. Each server is a **separate Python OS process** listening on loopback; one in-process manager publishes a consistent range map. Partition boundaries distribute the actual measured low-numbered model keys across all N servers. Each trial records operation payload bytes and p99 latencies.

The runner creates **24 raw trials** (eight server counts × seeds 42/43/44) and `scaling.csv` with the three-trial median throughput and p99. In the recorded runs, throughput peaks around two to three servers and subsequently declines. The **numerical medians are remeasured each CI run** and can vary materially with host contention; use `results/x2_scaling/scaling.csv` and the **revision-stamped rendered report** as the authoritative values. A single-host experiment does not establish production scale-out efficiency.

## X3: consistency vs throughput/convergence

The `results/c5_evaluation/summary.csv` contains **30 seeded cases**: five consistency settings (sequential; bounded τ=1,2,4; eventual) × 0/60 ms straggler × three seeds. All results are recomputed in CI, so the medians in historical local trials need not equal the final CI values. Refer to `summary.csv` for the actual throughput, p99, maximum lag and convergence, and to the **rendered report** for medians calculated from the same run.

The expected qualitative trade-off is that relaxed consistency permits more overlap without a straggler, whereas a large worker-side compute delay can dominate all modes. Sequential holds zero iteration lag; bounded modes must respect τ; eventual may admit substantially larger lag.

## X4: communication comparison

Use `python -m experiments d3` first to create the D3 CSV; X4's parser reads the *committed* three-trial D1/D2 logs and the new three-trial D3 output, decoding historical Windows UTF-16 logs if necessary. The following reductions are **application Protobuf bytes**, median across three runs:

| Optimisation | Baseline → optimised bytes | Reduction | Caveat |
|---|---:|---:|---|
| D1 cache | 2,373,522 → 2,144,532 | ~9.65% | Cache overhead can increase p99 latency |
| D2 compression | 2,373,522 → 1,985,158 | ~16.36% | Encoding/decoding CPU cost |
| D1+D2 combined | 2,373,522 → 1,756,168 | ~26.01% | Combined on tested sparse workload |
| D3 Pull batching | 108,800 → 39,000 | ~64.15% | RPCs 1,600 → 200 (87.5% reduction) |

D1/D2 latency fields are **Push p99**, whereas D3 is a **group-of-Pulls p99**. Do **not** plot them on one latency axis as equivalent measurements. Traffic excludes headers and backup replication.

## X5: elasticity

The latest committed E4 trial records 2→4 servers, approximately **0.00675s** measured cutover, 3,926 successful Pushes, and median before/after rates **325.2/326.8 Pushes/s** within five-second windows. Oracle checks final per-key values from successful Push counts. The design uses **in-process** server objects for migration and does not demonstrate arbitrary cross-host online rebalance. Show throughput-over-time and p95 latency plots, highlighting the migration period.

## X6: server failure and exactly-once effect

The committed seeded X6 trial used k=1 backup, three server processes, a live manager, two workers and a confirmed mid-iteration primary kill. It reported **718** acknowledged logical Pushes, independent offline oracle max absolute error **9.09e-13**, approximate detection **0.489s**, promotion **0.019s**, first restored Push **2.10s** after fault and conservative 90%-throughput restoration **3.0s**. The guarantee is narrowly scoped to one confirmed crash-stop with surviving replica and manager; no proof of consensus under partitions. Compare the offline oracle with *acknowledged logical Pushes*, not raw RPC attempt counts.

## X7: failure matrix

The committed `results/x7_fault_matrix/` contains nine configured cases: confirmed server crash, staged migration crashes, postpublication partial migration, worker crash under bounded delay, delayed/dropped Push and lost ACK, dropped transfer staging. Eight cases recovered or handled within the injected model; **one deliberate `EXPECTED_FAIL_CLOSED`** (source crash following partial postpublication migration) preserves safety by leaving writes fenced. A fail-closed outcome is **not** availability or automatic recovery.

## Provenance

- Historical C5/E4/X6/X7 outputs are checked into `results/`.
- Initial X1/X2/D3/X4 results were measured by GitHub Actions on the audit branch and captured as run artifacts; the final audit should preserve the **exact final-revision** results in `results/`.
- Most local benchmark numbers are OS-dependent; report median across seeded trials and identify the precise Git revision, CPU and environment where measured.
- The final report must distinguish in-process E4 from process-separated X2/X6, and measured application payload bytes from wire-level traffic.
