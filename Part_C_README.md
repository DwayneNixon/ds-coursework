# Distributed Systems Coursework — Part C: Distribution, Vector Clocks and Relaxed Consistency

University of Edinburgh · Distributed Systems (INFR11022) · Building a Parameter Server

This document covers **Part C (C1–C5)** of the coursework. It describes the distributed architecture, design choices and alternatives, implementation milestones, tests, experiments, observed results, limitations and the transition to Part D. It follows the same evidence-based, explanatory structure as `Part_B_README.md`.

**Status (17 November 2026):** The user reports that the complete **17-script Part C regression suite passed** after the final C4.5 changes. C5 evaluation ran **30 scenarios** (five consistency configurations × two straggler conditions × three trials); four convergence graphs were generated. Results described here were **run locally in Windows PowerShell and shared in conversation**, not independently reproduced against a pinned Git commit or a complete, freshly supplied working tree. **Part C's tested implementation is complete**, but this is not a claim that all rubric experiments, a million-key deployment, live data migration, or crash recovery are complete.

> **Continuity:** Part A supplies the asynchronous `ParameterClient`, RPC server, task IDs, dependency handling and update rule; Part B supplies the reusable worker loop and metrics/oracle utilities. Part C extends these components rather than replacing the Part B worker loop. See the Part A and Part B READMEs for their separate histories and limits.

---

## 1. Purpose and coursework requirements

The coursework's Part C (page 8) requires:

| Item | Requirement | Implemented approach | Evidence / boundary |
|---|---|---|---|
| **C1 — Partitioning** | Distribute key space over several servers; transparently route/split sparse and dense requests; merge replies | Manager-held contiguous key ranges, `RoutingTable`, `DistributedParameterClient`, logical tasks | Cross-partition and worker-routing tests passed; not a scaling benchmark |
| **C2 — Server manager** | Assign ownership; publish ownership map; clients cache and refresh stale maps | In-memory `ServerManager` with map epochs, `GetMap` RPC, server-side owner validation, `NOT_OWNER` selective retry | Manager, RPC, ownership and retry tests passed; no live state transfer yet |
| **C3 — Per-range vector clocks** | Clock per server partition; split when partially updated; send relevant clock metadata; measure growth | `RangeClocks`, split/merge, Push/Pull clock segments and explicit logical iteration | Clock unit/integration tests passed; range-vs-random growth experiment recorded |
| **C4 — Consistency** | Sequential, eventual, bounded delay τ; task dependencies and clocks to determine satisfaction | Manager `ConsistencyGate`, `CoordinatedClient` adapter, clock-verified Push completion and optional server-enforced `required_clock` for Push/Pull | Coordinator, clock-evidence, straggler and dependency integration passed; coordination and per-key clock checks are distinct mechanisms |
| **C5 — Evaluation** | Compare throughput and convergence across consistency models and τ, with/without stragglers | Same Part B worker harness, two server instances, fixed-gradient correctness and target-difference convergence; repeated trials | 30 scenario runs, summary and convergence CSV, latency/byte analysis and four graphs passed |

**Scope boundary:** The handout separately assigns message-encoding optimisations to **Part D**, live membership/data migration to **Part E**, and replication/failure recovery to **Part F**. Part C does not itself provide those features.

## 2. System architecture and execution flow

```text
                     Existing Part B run_worker(...)
                    pull -> simulated compute -> push
                                  |
                       CoordinatedClient (C4)
               iteration assignment / CanStart polling
              Push proof from returned clock / CompleteIteration
                                  |
                    DistributedParameterClient (C1)
                   cached map, logical task aggregation
                         /                    \
              ParameterClient                ParameterClient
               per owner A                    per owner B
                    |                              |
                   gRPC                           gRPC
                    v                              v
              ParameterServer S1             ParameterServer S2
              keys [0, split)                keys [split, 2^64)
              weights / clocks                weights / clocks
                    ^                              ^
                    |        owner check          |
                    +------------------------------+
                                  |
                         ServerManager (C2/C4)
                     owner map + epoch + ConsistencyGate
                      GetMap / CanStart / CompleteIteration

       Metrics/oracle/observer (Part B) -> C3 growth + C4/C5 results
```

A **logical operation** is one worker-facing Push/Pull. The distributed client may create multiple **physical shard RPCs**. It returns one logical asynchronous task and reassembles values in the caller's requested key order, as well as collecting relevant clock segments.

The manager is authoritative for **routing metadata and iteration progress**; parameter values and per-range clocks live on the owning servers. A server protects a single Push's staged updates and clocks with its local lock. **An operation spanning two servers is not globally atomic**, and reads from different owners are not a transactionally consistent global snapshot.

### Interfaces and responsibilities

| Component | Responsibility |
|---|---|
| `routing.py` | `Partition`, `RoutingTable`, dense/sparse splitting and merge utilities, uint64 key-domain sentinel |
| `distributed_client.py` | Transparent fan-out, logical `DistributedTask`, aggregation, refreshed-map retry and logical-iteration/clock forwarding |
| `server_manager.py` | Authoritative partition ownership map and epoch, `ConsistencyGate` configuration |
| `manager.proto`, `manager_rpc.py` | gRPC manager API: map publication and consistency progress queries/updates |
| `server.py` | Parameter values, owner enforcement, Task-ID state, atomic per-shard update, range-clock metadata and required-clock waits |
| `range_clocks.py` | Partition-wide initial clock; splitting, updating, coalescing, comparing, and key lookup |
| `consistency.py` | Sequential, bounded-τ, eventual iteration-admission rules and completion tracking |
| `coordinated_client.py` | Worker-facing consistency adapter and returned Push clock validation |
| `parameter_server.proto` / generated stubs | Physical Push/Pull data, Task IDs, iteration and clock metadata/requirements |
| `parameter_client.py` | Physical asynchronous RPC client, Task-ID dependencies, request clock-argument validation |
| `harness/worker.py`, `harness/oracle.py`, `harness/metrics.py` | **Reused Part B** training loop, correctness oracle and measurement collection |
| `Part_C_Tests/` | All Part C `test_c*.py` scripts, grouped into a Python package |
| `results/c3_clock_growth.csv` | C3 access-pattern structural-count experiment |
| `results/c5_evaluation/` | C5 CSVs, three-trial data, console log and generated convergence plots |

Module names are those reported and used in the local test commands. The current local repository is the authoritative source of exact method signatures; this document is not an independently frozen source-code snapshot.

---

## 3. C1 — Routing and transparent distributed requests

### Chosen design: manager-held contiguous range partitions

The key domain is uint64: `0` through `2^64 - 1`. Partition intervals use half-open bounds `[begin, end)`, with an internal **`KEY_SPACE_END = 2^64` sentinel** to represent the end of the full domain without losing the maximum valid key. Routes are non-overlapping and have an owner address and stable partition identifier.

| Option | Pros | Trade-offs | Decision |
|---|---|---|---|
| **Range partitioning + manager table** | Naturally matches dense range RPCs and range clocks; easy to reason about ownership and contiguous splits | Hot ranges may be imbalanced; joins require explicit range transfer | **Chosen** |
| Consistent hashing | Useful automatic redistribution with changing node count; good for point lookups | Contiguous ranges can scatter among owners, requiring extra subrequests and clock bookkeeping | Not chosen for this coursework architecture |
| Hardcoded modulo routing | Minimal implementation | Unfriendly to ranges, dynamic maps and safe transfer; all clients must change together | Rejected |

A sparse request such as `[2, 7, 45]` is partitioned by owner. A dense `range(30, 50)` is split at range boundaries, not expanded into unrelated single-key requests. The logical response maps shard results back to the original key positions. One logical Push can therefore span multiple owners while preserving the worker-facing API.

### Asynchronous logical tasks and dependencies

The existing `ParameterClient` returns `Task` objects. `DistributedTask` represents one logical operation with subtask completions; it aggregates successful results (values, acceptance and clock metadata) and propagates failures. The Part B loop still performs Pull → compute → dependent Push without needing explicit knowledge of the partition table.

**C1 evidence:** `test_c1_distributed_client.py` reported **6 passes**. A routed Part B worker integration reached three iterations, six logical operations and **1,074 B** of measured logical-operation payload in that smoke run, with its final-weight oracle passing. This was a small correctness test, **not** a throughput claim.

---

## 4. C2 — Ownership manager, stale maps and selective retries

### Chosen design: central in-memory authoritative map with epochs

`ServerManager` holds the current partition table and its epoch. `manager.proto` defines `GetMap`, exposed through `ManagerService`. Clients fetch and cache routing metadata instead of contacting the manager for each operation.

When a client sends a request using stale ownership, the physical server verifies its currently configured range and rejects keys it does not own, using `NOT_OWNER`. The distributed client refreshes the map and **retries only the rejected shards**, preserving previously successful shard results and avoiding duplicate updates to those shards.

### Why not always ask the manager?

| Design choice | Advantage | Disadvantage |
|---|---|---|
| Query manager before every Push/Pull | Easy to reason about fresh routes | One extra RPC on the common path; manager bottleneck |
| **Cache ownership, refresh on `NOT_OWNER`** | Fast common path and explicit stale-route recovery | Retry path must preserve logical identity, dependencies, iteration and clock metadata |
| Blindly retry entire logical Push | Simple error handler | Can replay already-applied shards and threaten correctness |

**Chosen:** cached ownership, server validation and selective retry.

The ownership change tests exercise **routing and rejection**, not full live migration of the parameter values and clock histories. A real owner transition that preserves updates during bulk copy still belongs to Part E. The current in-memory map and manager do not constitute a durable consensus service.

**C2 evidence:** `test_c2_manager.py` **3**, `test_c2_manager_rpc.py` **6**, `test_c2_server_ownership.py` **5**, and `test_c2_auto_recovery.py` **7** tests were reported passing. The selective retry tests include preservation of the logical iteration on Push retries.

---

## 5. C3 — Per-range vector clocks

### 5.1 Purpose and representation

For each partition, the server tracks a per-range vector `{worker_id: latest_counter}`. This records which worker's logical updates have been observed on a given part of the key space. A new owned partition starts with **one clock segment**, not one Python object per key.

`RangeClocks(begin, end, initial=None)` supports snapshots, count, `clock_for_key(key)`, `record(begin,end,worker,counter)`, cloning and clock comparisons. A partially updated segment is split into left/updated/right pieces; adjacent segments with equal vectors can be **coalesced** again.

Illustrative update:

```text
Initially:   [0 ....................................... 100)
              {}

Worker 7 updates [20, 40) at iteration 3:
              [0,20)       [20,40)          [40,100)
               {}          {7:3}              {}

Another matching adjacent update can coalesce segments.
```

A dense full-partition Push does not need one clock per parameter; a scattered point Push can introduce new boundaries on both sides of each updated key.

### 5.2 Atomic server updates and metadata

The server stages clock changes on **cloned `RangeClocks`** and commits them alongside a successful parameter Push under the server lock. A rejected Push or duplicate Task ID should not create additional clock progress. This extends the earlier Part A staged-commit discipline to clock state.

The protocol carries clocks back to callers:

- `ClockEntry`: worker ID and counter.
- `ClockSegment`: partition ID, begin/end, `end_is_keyspace_end` sentinel flag and repeated entries.
- `PushResponse.clocks` / `PullResponse.clocks`: relevant segments for the affected keys or range.
- `PushRequest.iteration`: optional positive logical iteration, distinct from a per-shard Task-ID sequence. Legacy requests without an explicit iteration continue to use the earlier sequence-based fallback.

**Why a separate iteration field?** If one logical Push is split across multiple shards, each physical client may allocate a different Task ID. The same **logical worker iteration** must be reflected in all partitions' clocks, irrespective of those physical RPC IDs. The distributed client forwards that explicit iteration and aggregates returned segments.

### 5.3 C3 clock growth experiment

`run_c3_clock_growth.py` generated `results/c3_clock_growth.csv` for **10,000 keys, two workers, 400 updates, seed 42**. The comparison was between updates aligned to **10 blocks of 1,000 keys** and random distinct single-key updates.

| Updates | Range-heavy segments | Single-key segments | Range-heavy clock entries | Single-key entries |
|---:|---:|---:|---:|---:|
| 0 | 1 | 1 | 0 | 0 |
| 50 | 10 | 101 | 18 | 50 |
| 100 | 10 | 200 | 20 | 100 |
| 200 | 10 | 398 | 20 | 200 |
| 300 | 10 | 593 | 20 | 300 |
| 400 | **10** | **785** | **20** | **400** |

At 400 updates, the random workload used **78.5× as many segments** as the aligned range workload, and **20× as many recorded worker-counter entries**. This occurs because adjacent keys updated similarly share a clock, whereas isolated keys create fragmented boundary regions.

**Measurement boundary:** These are structural counts, **not measured resident-memory bytes or network bytes**. The benefit depends on spatial locality and how often equal adjacent clocks can merge.

### 5.4 C3 regression evidence

| Test | Reported result |
|---|---|
| `test_c3_range_clocks.py` | 7 passed |
| `test_c3_server_clocks.py` | 7 passed |
| `test_c3_clock_responses.py` | 6 passed |
| `test_c3_iterations.py` | 6 passed |
| `test_c3_distributed_clocks.py` | 7 passed |
| `run_c3_clock_growth.py` | Experiment and five output checks passed |

---

## 6. C4 — Selectable consistency, dependencies and proof of progress

### 6.1 Iteration-admission semantics

The coursework defines the bounded-delay condition: a worker beginning iteration `t` may require all workers to have completed iterations through `t - τ - 1` (lower-bounded by zero). The implemented gate specialises this for each mode:

| Mode | Required other-worker completion before starting iteration `t` | Meaning |
|---|---|---|
| **Sequential** | `t - 1` | No worker gets ahead by an iteration; global iteration barrier |
| **Bounded delay τ** | `max(0, t - τ - 1)` | A worker may run at most τ iterations ahead of other completed progress |
| **Eventual** | `0` | No cross-worker progress requirement for admission |

`ConsistencyGate` stores the **contiguous completed iteration** for each registered worker, rather than using the maximum observed announcement. Out-of-order/future completion announcements are not interpreted as all intervening iterations having succeeded. It provides `can_start`, `wait_until_allowed`, `complete`, `snapshot`, and `required_iteration`.

**Implementation choice:** a small manager-side `threading.Condition` gate, queried over `CanStart` and advanced over `CompleteIteration`, rather than workers exchanging pairwise progress messages or building a separate all-to-all barrier protocol. This gives a simple, testable admission rule for a static configured worker set. It is **not a universal total ordering of every physical RPC**.

### 6.2 Reuse the Part B worker unchanged

`CoordinatedClient` wraps the existing `DistributedParameterClient` and implements the same `pull` / `push` operations. It:

1. Tracks the worker's current logical iteration.
2. Attaches that iteration to Push requests.
3. Confirms the returned Push clocks cover the actual updated target and the worker's expected counter.
4. Only then reports completion to the manager via `CompleteIteration`.
5. Polls `CanStart` before allowing the next iteration, subject to the chosen consistency model.

The Part B `run_worker` receives `on_iteration=client.iteration_finished`. The worker's Pull/compute/Push loop and its operation and iteration callbacks **do not need to be rewritten for each consistency mode**.

**C4.2B reported smoke test:** two servers, two workers, eight parameters of width two, three iterations each, one 150 ms straggler. Sequential, bounded τ=1 and eventual runs all passed manager progress, admission barriers, fixed-gradient SGD oracle and final clock checks; elapsed times were approximately **0.462–0.464 s**. Similar total elapsed times were expected because every mode still waited for the slow worker to finish.

### 6.3 Why check returned clocks, not only RPC acceptance?

A successful shard response alone does not show that **all the keys of a distributed Push** have a clock consistent with the logical iteration. `clock_covers_push(...)` checks dense coverage or each sparse key using aggregated `ClockSegment`s, including the full-keyspace endpoint. It rejects missing regions, old counters and clocks for the wrong worker. Completion is not reported to the manager when the proof fails.

`test_c4_clock_evidence.py` had **7 passes**: sparse/dense cross-partition coverage, missing partition, older clock, wrong worker, maximum key boundary and prevention of falsely reporting completion.

**Guarantee boundary:** The proof checks `counter >= iteration` for the affected keys. A counter is a *latest-observed value*, not a cryptographically authenticated log of every individual iteration. The manager still trusts the completion RPC's caller; crash-safe and adversarial identity guarantees are not provided.

### 6.4 C4.4: observe actual staleness during Pull

The longer straggler integration test used **two workers × eight iterations**, eight dense keys, width two, and **200 ms** simulated computation on one worker. For the dense workload, the observed lag relative to the other worker at iteration `t` was:

```text
lag = max(0, (t - 1) - observed_other_worker_clock)
```

| Mode | Maximum observed iteration lag | Total elapsed time |
|---|---:|---:|
| Sequential | **0** | **1.628 s** |
| Bounded τ=2 | **2** | **1.630 s** |
| Eventual | **7** | **1.628 s** |

All three runs passed manager progress, clock staleness policy, the independent fixed-gradient final oracle and both partitions' final clocks.

**Why the same total runtime?** Every case must wait for the worker doing eight × 200 ms of simulated computation. Relaxed consistency changes **when the faster worker can progress**, rather than eliminating the slow worker's time.

**Measurement boundary:** This lag definition assumes every worker updates each measured key each iteration. In a sparse workload, absence of a worker's counter increment on a key does not by itself mean the worker failed to complete an iteration.

### 6.5 C4.5: server-enforced clock-aware dependencies

The initial consistency adapter checked returned clocks and used a global manager gate, but the coursework also asks to use vector clocks to determine whether **task data dependencies** are satisfied. We therefore added an optional `required_clock` requirement to physical Push and Pull requests, while retaining the pre-existing A3 Task-ID dependencies.

Example worker-facing API:

```python
pull_task = distributed_client.pull(
    [10, 50],
    required_clock={1801: 5},
)
result = pull_task.wait(timeout_ms=5000)
```

This requires the **requested keys on each owner** to show Worker 1801's clock at least 5 before the Pull executes. A dependent Push similarly waits without modifying weights prematurely. The server waits on a condition variable that releases its lock; committed clock advances notify waiting operations. Invalid/duplicate worker-counter entries are rejected, and waits time out rather than blocking indefinitely.

Implementation layers:

- **Server:** `_wait_for_clock_dependencies` checks required vectors against clock segments covering every targeted key/range, before reading or committing.
- **Single-server client:** optional `required_clock` dictionary validated as uint32 worker IDs and positive uint64 counters, serialized as deterministic `ClockEntry` fields.
- **Distributed client:** forwards requirements to **every initial physical shard RPC** and keeps them with logical tasks for relevant retry paths. Stale-map retry metadata forwarding has **not** yet been exercised during a real state-preserving owner transfer.

**Regression/bug resolved:** Initially, the distributed client validated and stored `required_clock` but omitted it from the **initial `ParameterClient.pull()` call**. The cross-partition Pull incorrectly returned `[0.0, 0.0, 0.0, 0.0]` immediately. Adding `required_clock=required_clock` to every routed physical Pull fixed it; all six client tests subsequently passed. The direct server test had already shown that server-side clock waits were functioning.

**C4.5 evidence:** `test_c4_clock_dependencies.py` passed its seven checks (including delayed Push and no partial writes), and `test_c4_client_clock_dependencies.py` passed all six (cross-owner wait, single-server API, distributed Push, per-key isolation, invalid metadata and legacy compatibility).

**Important architectural distinction:** The manager's iteration barrier and per-range `required_clock` are **two complementary mechanisms**. The C4.5 test proves optional clock-aware data dependencies, not that every `CanStart` automatically generates per-key `required_clock` dependencies. For a truly atomic cross-server snapshot or migrating server, additional coordination is needed.

---

## 7. C5 — Throughput, staleness, latency and convergence evaluation

### 7.1 Experimental configuration

`run_c5_evaluation.py` reused the Part B worker and metric/oracle helpers. Each scenario started fresh manager and parameter-server instances, then ran **two workers × 24 iterations = 48 logical Pushes** over **eight dense keys of width two**. The tested modes were sequential, bounded τ=1,2,4 and eventual, both **without a straggler** and **with a 60 ms compute delay on Worker 1602**.

After the first ten-scenario, seed-42 smoke run, `SEEDS=(42,43,44)` and a seeded shuffle of scenario order produced **30 local trials**. The same dense key set was used each time; seeds principally varied scenario ordering and timing, **not independent key subsets**. Results were written to:

```text
results/c5_evaluation/
    summary.csv
    convergence.csv
    summary_initial.csv              # if initial result backup command was followed
    convergence_initial.csv          # if initial result backup command was followed
    run_3seeds.log                    # if console redirection command was followed
    convergence_by_updates_0ms.png
    convergence_by_updates_60ms.png
    convergence_by_time_0ms.png
    convergence_by_time_60ms.png
```

The first two CSVs and all four PNG charts were explicitly reported generated; backup/log filenames are conditional on following the separate PowerShell commands.

**Metric definitions:** throughput here is **48 completed logical Pushes divided by total scenario wall time**; this differs from Part B's event-sampled peak one-second-window throughput. `p50_push_ms`/`p99_push_ms` are percentiles of successful worker Push operations; payload bytes count the recorded worker Push/Pull Protobuf messages. The observer's extra Pulls, manager coordination RPCs and transport framing are outside the recorded byte totals.

For convergence, gradients were `g = w_pulled - w_target`, applied with the default η=0.1. The distance is Euclidean `||w - w_target||₂` over all 16 scalar components. The code used observer Pulls every four completed Pushes, plus initial and final samples. These intermediate reads are not globally atomic across the two servers and contribute some runtime overhead.

### 7.2 Three-trial results without a straggler

| Mode | Median Pushes/s | Observed rate range | Maximum observed lag | Median final target distance | Median fast-worker finish |
|---|---:|---:|---:|---:|---:|
| Sequential | **153.97** | 133.21–154.11 | 0 | 0.114515 | 0.311 s |
| Bounded τ=1 | **345.13** | 321.50–354.99 | 1 | 0.108004 | 0.134 s |
| Bounded τ=2 | **401.67** | 352.02–450.62 | 1 | 0.102402 | 0.118 s |
| Bounded τ=4 | **404.14** | 286.51–415.84 | 1 | 0.104053 | 0.114 s |
| Eventual | **409.75** | 371.47–441.25 | 1 | 0.103153 | 0.116 s |

Eventual had a **2.66×** higher median throughput than sequential in these local, no-straggler trials (409.75 / 153.97). This is evidence of reduced synchronization overhead **in this setup**, not a universal speedup. The ranges for τ=2, τ=4 and eventual overlap; three trials are not sufficient for a reliable fine-grained ranking. Initial scenario order also mattered in the single-seed smoke test, hence the later shuffled repetitions.

### 7.3 Three-trial results with a 60 ms straggler

| Mode | Median Pushes/s | Observed rate range | Maximum observed lag | Median final target distance | Median fast-worker finish |
|---|---:|---:|---:|---:|---:|
| Sequential | **31.42** | 31.38–31.51 | 0 | 0.095387 | 1.480 s |
| Bounded τ=1 | **31.56** | 31.04–31.59 | 1 | 0.095238 | 1.410 s |
| Bounded τ=2 | **31.06** | 30.68–31.56 | 2 | 0.094920 | 1.375 s |
| Bounded τ=4 | **31.31** | 31.06–31.34 | 4 | 0.093679 | 1.228 s |
| Eventual | **31.25** | 31.17–31.38 | **23** | 0.018094 | **0.070 s** |

Total throughput stayed near 31 Pushes/s because all tests waited for the same slow worker to complete its 24 delayed iterations. **Fast-worker finishing time**, by contrast, dropped from 1.480 s sequentially to 0.070 s with eventual consistency. This identifies a responsiveness benefit that aggregate scenario throughput hides.

The bounded modes respected their configured staleness ceilings. Eventual allowed up to **23 iterations of lag**. Surprisingly, eventual's final median distance was **lower** here than sequential's; that is a **scheduling- and workload-dependent observation**, not evidence that stale gradients always converge better. With target-difference gradients, interleaving and the time at which updates are read influence the trajectory.

### 7.4 Push latency (median of three per-trial percentiles)

| Mode | No straggler: p50 / p99 | 60 ms straggler: p50 / p99 |
|---|---|---|
| Sequential | 1.132 / 2.547 ms | 1.081 / 1.631 ms |
| Bounded τ=1 | 1.673 / 2.265 ms | 1.147 / 1.821 ms |
| Bounded τ=2 | 1.649 / 2.753 ms | 1.302 / 2.523 ms |
| Bounded τ=4 | 1.618 / 2.103 ms | 1.165 / 2.161 ms |
| Eventual | 1.635 / 2.316 ms | 1.145 / 2.379 ms |

Relaxed consistency's main observed throughput improvement did **not** require faster individual Push RPCs: it came from reducing inter-iteration waiting. There is no stable monotonic p99 ranking across the five modes in three trials. These are **medians of each trial's percentile**, not a single pooled percentile over all requests.

### 7.5 Communication totals (median serialized worker payload bytes)

| Mode | No straggler | With 60 ms straggler |
|---|---:|---:|
| Sequential | 22,662 | 22,662 |
| Bounded τ=1 | 22,662 | 22,634 |
| Bounded τ=2 | 22,662 | 22,606 |
| Bounded τ=4 | 22,662 | 22,550 |
| Eventual | 22,662 | 22,053 |

All configurations ran the same number of logical worker operations. The straggler-case totals differed by at most **609 bytes (about 2.7%)**, plausibly due to differences in returned clock metadata under different update interleavings; the precise per-message cause was not separately traced. This does **not** show a new communication optimisation. Total measured bytes exclude coordinator RPCs, observer reads and gRPC/TCP framing.

### 7.6 Generated convergence charts

`analyse_c5_curves.py` reported PASS for both graph types under both straggler settings:

- `convergence_by_updates_0ms.png` and `convergence_by_updates_60ms.png` — median distance at common completed-Push counts, with faint individual-trial curves.
- `convergence_by_time_0ms.png` and `convergence_by_time_60ms.png` — individual-trial distance versus elapsed time, because sampling timestamps differ.

**Why both x-axes?** Update-count curves address efficiency per applied update; time curves address convergence achieved per second. These are not interchangeable. The output PNGs were reported generated, but their visual appearance was not independently inspected in the conversation.

### 7.7 Evaluation design decisions

| Decision | Rationale | Alternative / limitation |
|---|---|---|
| Reuse Part B `run_worker` | Keeps implementation focused on distributed-system behavior, preserves tested metrics callbacks | Separate bespoke workload would be easier to fit but less comparable |
| Run isolated fresh server/manager instances per case | Avoids stale Task IDs, parameter carry-over and contaminated clocks | Per-scenario start-up and JIT/cache variation affects short timings |
| Use **fixed** gradients in correctness tests | Offline oracle detects lost/doubled updates independent of scheduling | Fixed gradients alone say little about convergence |
| Use **target difference** in C5 | Models dependence on stale pulled values and gives a meaningful target-distance curve | A small synthetic convex task is not a real neural network |
| Measure fast-worker finish and total finish | Distinguishes coordination waiting from slow-worker dominated global duration | More workers/machines needed for general scalability claims |
| Three shuffled trials | Reduces fixed ordering bias, shows variability | Three trials and one small dense dataset are still limited evidence |
| Protobuf payload-byte accounting | Consistent with existing Part B instrumentation | Not network wire bytes; excludes some RPCs and observer |

---

## 8. Tests and reproduction instructions

### 8.1 Python environment

From the repository root in Windows PowerShell:

```powershell
python -m pip install grpcio grpcio-tools matplotlib
```

Regenerate stubs after any `.proto` change:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parameter_server.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. manager.proto
```

Use the project root as the working directory. The test files have been moved into **`Part_C_Tests/`** and include imports between test modules. An empty `Part_C_Tests/__init__.py` makes package invocation possible; adding both root and test folder to `PYTHONPATH` preserves existing short-form imports without editing tests:

```powershell
New-Item -Path .\Part_C_Tests\__init__.py -ItemType File -Force
$env:PYTHONPATH = "$(Get-Location);$(Get-Location)\Part_C_Tests"
```

### 8.2 Full 17-script Part C regression

```powershell
$tests = Get-ChildItem -Path ".\Part_C_Tests" -Filter "test_c*.py" |
    Sort-Object Name

foreach ($test in $tests) {
    Write-Host "`n========== $($test.Name) ==========" -ForegroundColor Cyan
    python -m "Part_C_Tests.$($test.BaseName)"

    if ($LASTEXITCODE -ne 0) {
        throw "REGRESSION FAILED: $($test.Name)"
    }

    Write-Host "PASS: $($test.Name)" -ForegroundColor Green
}

Write-Host "`nALL PART C REGRESSION TESTS PASSED" -ForegroundColor Green
```

**User-reported terminal output (17 November 2026):** `ALL PART C REGRESSION TESTS PASSED`.

| Milestone | Script | Focus |
|---|---|---|
| C1 | `test_c1_distributed_client.py` | Route/split/merge, logical distributed operations |
| C2 | `test_c2_manager.py` | Ownership and epoch |
| C2 | `test_c2_manager_rpc.py` | Map RPC |
| C2 | `test_c2_server_ownership.py` | Server rejection when not owner |
| C2 | `test_c2_auto_recovery.py` | Refresh map, retry rejected shards and preserve successful ones |
| C3 | `test_c3_range_clocks.py` | Split/merge/compare range clocks |
| C3 | `test_c3_server_clocks.py` | Commit, duplicate and reject behavior |
| C3 | `test_c3_clock_responses.py` | Returned Push/Pull clock segments |
| C3 | `test_c3_iterations.py` | Logical iteration separate from Task ID |
| C3 | `test_c3_distributed_clocks.py` | Distributed clock forwarding/aggregation |
| C4 | `test_c4_consistency.py` | Local gate rules |
| C4 | `test_c4_manager_consistency.py` | Manager consistency RPCs |
| C4 | `test_c4_worker_integration.py` | Two-server, two-worker three-mode smoke |
| C4 | `test_c4_clock_evidence.py` | Full target coverage before completion |
| C4 | `test_c4_straggler_staleness.py` | Measured Pull staleness with slow worker |
| C4.5 | `test_c4_clock_dependencies.py` | Direct server-side clock wait and protection |
| C4.5 | `test_c4_client_clock_dependencies.py` | Single/distributed client clock requirements |

These are **test script names**, not individual assertion totals; individual focused suite counts were recorded earlier in the development conversation. The whole suite was executed in the user's Windows environment, not inside this documentation-generation environment.

### 8.3 Re-run C3/C5 experiments

```powershell
python run_c3_clock_growth.py
python run_c5_evaluation.py
python analyse_c5.py
python analyse_c5_curves.py
```

The C5 runner's current `SEEDS` and randomized scenario loop should include `(42, 43, 44)` for the 30-case comparison. For baseline comparison, preserve previous CSVs before rerunning: the script **overwrites** the main summary/convergence CSVs. Retain an exact copy of the configuration, package versions and Git revision for a future reproducibility report. The scenario may print one line per worker iteration; redirected console logs are useful.

---

## 9. Problems encountered and how they were resolved

| Issue | Symptom | Diagnosis and fix |
|---|---|---|
| Initial C3 Push clock integration | `UnboundLocalError` for `worker_id` / `sequence` | Unpack Task-ID before the optional explicit `iteration` branch; fallback only when iteration absent |
| Distributed versus physical Task IDs | Per-shard sequence numbers did not represent shared worker iteration | Added explicit optional Push `iteration` and forwarded it on all shards/retries |
| Initial C4.3 proof coverage | Correct acceptance is insufficient for multi-shard progress | Aggregate clock segments and verify all dense/sparse affected keys before reporting completion |
| C4.5A dependent Push test | `assert not pending_push.done()` initially failed | Inspect actual RPC result rather than assuming success; increase test dependency timeout from 0.3 s to 2.0 s; subsequent full direct test passed |
| C4.5B distributed Pull finished prematurely | Returned zero vectors with two clock segments before prerequisite update | Requirement was validated/stored but **not forwarded to initial physical Pulls**; add `required_clock=required_clock` to the per-route `ParameterClient.pull(...)` call; full six-check test then passed |
| Short sequential/eventual total-time comparisons | Almost identical C4.4 total elapsed despite different lag | Expected because every scenario waits for the slow worker; report fast-worker completion separately |
| C5 single-seed scenario-order bias | First sequential no-straggler measurement substantially slower | Repeated seeds 42/43/44 with shuffled case order; report medians and observed ranges |

Keep failing intermediate outputs as engineering evidence, but **do not represent a test as passing until its corrected run was observed**. The final regression was reported successful after the client forwarding fix.

---

## 10. Design trade-offs and alternatives in one place

| Problem | Chosen | Not chosen / reason | Cost / caveat |
|---|---|---|---|
| Key ownership | Contiguous ranges with manager map | Consistent hashing; scatters dense keys | Explicit load balancing and migration required later |
| Ownership metadata | Client-cached epoch map, refresh on `NOT_OWNER` | Manager query per operation | Retry complexity, possible stale reads without transfer |
| Distributed API | Logical Task combining shard Tasks | Worker manually calls each server | Multi-shard operations are not globally atomic |
| Clock granularity | Split/coalesced range clock segments | One clock per key; too expensive for range locality | Random point accesses fragment the map |
| Version identity | Explicit logical `iteration` | Reuse physical per-shard task sequences | Iterations must be supplied consistently by coordinated workers |
| Consistency scheduler | Central manager gate, polling | Pairwise barriers or uncoordinated waits | Static worker set, trusted completion messages, polling overhead |
| Clock verification | Returned Push segments checked before completion | Trust any successful RPC response | Clock counters are not durable proofs of every intermediate update |
| Clock-dependent tasks | Optional `required_clock` on physical and distributed Push/Pull | Only client-side prerequisites | Waits can time out; requirements are uniform across requested keys |
| Workload | Reused Part B emulator | Real ML/GPU training | Small synthetic target-diff problem |
| Correctness check | Fixed-gradient oracle + target-distance evaluation | Only one metric | Different checks answer different questions |
| Benchmark reporting | Median of 3 randomized trials and observed ranges | Single unqualified timing | Single-machine scheduling noise remains |

---

## 11. What is complete — and what remains outside Part C

### Verified within the tested scope

- Transparent sparse/dense routing over two parameter-server instances; physical shard work hidden behind the logical client.
- Manager ownership maps, epoch changes, owner checking, stale-map refresh and selective rejected-shard retry.
- Per-partition range clocks, splitting/merging, atomic per-shard clock updates, explicit logical iteration metadata and clock-bearing responses.
- Sequential, bounded-τ and eventual admission using coordinator progress; worker-facing wrapper retains Part B loop.
- Clock coverage verification before declaring a Push iteration complete; optional server-side clock prerequisites for Push and Pull, including cross-partition client use.
- Measured no-straggler and straggler throughput, latency, max lag, fast-worker completion, serialized payload bytes and target convergence; repeated trials and four plots.
- User-reported green **17-script Part C regression** following final dependency fix.

### Open limitations and obligations

1. **Dynamic ownership and value migration:** C2 refreshes metadata and routes rejected shards but does not copy or replay in-flight values/clocks to a new owner; Part E must implement a safe two-phase transfer or forwarding mechanism. Clock requirement preservation during a real move is not yet directly demonstrated.
2. **Global atomicity:** Cross-server Pushes and Pulls are composed from independent physical requests; there is no global transaction/snapshot.
3. **Manager robustness:** Single in-memory ownership/progress authority, no durable replicated manager, no heartbeat-based failure detection (Part F), and no authentication of completion reports.
4. **Worker membership:** The configured `ConsistencyGate` expects a known worker set; joins/leaves mid-training and their effect on clocks/admission are Part E tasks.
5. **Clock semantics:** Latest-observed counters may skip explicit intermediate iterations; no durable per-iteration history. Clock requirements on sparse keys cannot be treated as global worker completion unless the workload guarantees each key is updated every iteration.
6. **Experiment scale:** Two local server instances, two workers, eight dense keys in C5; no end-to-end million-key experiment, 1–8 server scaling curve or cross-machine network evaluation.
7. **Measurement completeness:** Worker serialized Protobuf payload, not wire bytes; excludes manager and observer traffic. No validated long 1-second throughput time series around failure/recovery events.
8. **Benchmark uncertainty:** Three trials with near-identical dense access, scheduling-dependent performance and convergence; no statistical inference or confidence intervals.
9. **Part A historical risks:** Persisted task IDs across restarts, wider-width tests, huge dense request bounds and full server-side update-rule registration contract require separate review. Some earlier Part A findings were superseded by newer protocol/client revisions; do not copy stale statements as if current.
10. **Rubric experiments:** C3/C4/C5 experiments are useful evidence for the associated ideas, but no dedicated full **X1 multiworker analytic contraction**, **X2 one-to-eight-server scaling**, or complete externally configured **X1–X7** suite has been reported. C5 is a substantive X3-style comparison, not a claim that the entire X3 specification/format has independently passed.

The coursework's code submission/README must also record a verified Git revision and actual environment; those details were not supplied with the current test outputs.

---

## 12. Part D handoff — start in a new conversation

**Part C checkpoint:** Complete for tested cases. Preserve `Part_C_Tests/`, `Part_C_README.md`, `agents.md`, `observations.md` and `results/c3_clock_growth.csv` / `results/c5_evaluation/` (where present). Do not casually rewrite existing server/client contracts.

The coursework (page 8) asks for **at least three** measurable communication optimisations and the byte-saving metric:

```text
saving = 1 - (optimised_serialized_bytes / baseline_serialized_bytes)
```

Current planned choices (not implemented):

| Order | Technique | Target workload | Correctness challenge | Metrics |
|---|---|---|---|---|
| **D1** | **Key caching**: reuse a signature for repeated sparse key lists | Repeated fixed sparse sets, ideally Zipf/hot-keys | Cache misses, eviction, signature/identity, per-owner + ownership-epoch invalidation | Bytes, cache hit rate, p50/p99 latency |
| **D2** | **Value compression**: zstd/snappy or another validated lossless codec | Repeated or compressible gradient/weight payloads | Encoding/decoding correctness, worse case expansion, CPU overhead | Compressed vs baseline bytes and latency |
| **D3** | **Batching small requests** | Many small independent Push/Pulls to same owner | Preserve operation ordering, Task IDs, failure isolation, dependencies and response mapping | RPC count, bytes, p50/p99 latency, throughput |

These are choices to evaluate, not claims that they will improve every workload. Specifically, key caching should be compared on repeated **sparse** access, not only on dense `KeyRange` calls (which already avoid sending individual keys). Start from a **no-optimisation baseline**, run independent correctness tests for each switch, and repeat timing/byte comparisons with and without it. An optional feature flag and compatibility tests keep Part C behavior stable.

**Start the new chat with:** `agents.md`, `observations.md`, `Part_C_README.md`, coursework PDF, and the latest implementation files for protocol/client/server. Request the current source before editing protocol methods; earlier uploaded `server.py`/`parameter_client.py` snapshots predate some Part C changes.

### Recommended first D1 steps

1. Confirm exact Part D options and choose one reproducible repeated-sparse-key workload.
2. Measure serialized request/response bytes and latency with caching **off**.
3. Design a cache key scoped to owner address, partition/epoch and key-list signature; specify cache-miss resolution and invalidation.
4. Implement caching as optional, initially with single-server tests, then distributed cross-owner routing and stale-map tests.
5. Repeat the baseline and report actual saving; do not infer compression from less CPU work or vice versa.

---

## 13. Related documentation and evidence provenance

- **`Distributed Systems - Coursework.pdf`** — official task definitions, page 8 Part C/D and the experiment requirements in Section 5.8.
- **`Part_B_README.md`** — prior harness components, API, instrumentation and single-server baseline results.
- **`agents.md`** — durable cross-chat architectural context and next-action handoff.
- **`observations.md`** — dated PASS/failure facts, exact numerical observations and unresolved items.
- **`Part_C_Tests/` and `results/`** — local regression scripts and generated experiment evidence.

**Evidence policy:** Results are user-reported Windows runs. The author of this document did not inspect a pinned repository revision, replay the complete suite, inspect the four PNG plots, or validate a cross-machine deployment. The report must preserve these distinctions.

**Summary:** Part C extends the single-node parameter server into a transparently routed multi-server service, with manager-owned ranges, per-range vector clocks, stale-map retry, selectable consistency policies, clock-aware task requirements and a repeated performance/convergence evaluation. The final reported regression is green across **17 scripts**, and the 30 C5 benchmark scenarios provide a clear, bounded comparison of the synchronization–staleness trade-off. **Part D begins in a separate chat.**
