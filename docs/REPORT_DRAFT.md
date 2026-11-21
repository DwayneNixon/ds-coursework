# Building a Parameter Server — informal report draft

**Course:** Distributed Systems · University of Edinburgh (INFR11022)  
**Status:** Draft for final human review, layout and ≤8-page export. Source code and the exact final-revision evidence should be checked against this text before submission.

> **Pagination:** The handout limits the finished report to **eight pages**. This Markdown is a concise source draft, **not** a verified eight-page PDF. Use a diagram and plots selectively, with a caption that includes the run's seed/revision.

## 1. Aim and API

We built a Python/gRPC parameter server for distributed SGD-style training. A parameter is addressed by a `uint64` key and stores a vector of double-precision values, with configurable width. Clients issue asynchronous `push(target, gradients, width, deps=...)` and `pull(target, deps=...)` operations on sparse key lists or compact dense ranges. They receive `Task` handles with `wait()` and `done()`, and can choose a server-side update rule (default `w ← w − ηg`), including registering a supported linear rule through RPC.

Each Push is validated and locally atomic; its deterministic Task ID and request fingerprint suppress identical retries. Task dependencies are checked at both the client and the server, including failed predecessors. The default request budget is 200,000 parameter values per RPC, protecting server resources while permitting much larger models through multiple requests. Clients must assign **unique stable worker IDs** and preserve Task IDs for retries.

## 2. Architecture

**Figure 1 (insert):** use the Mermaid diagram in `docs/ARCHITECTURE.md`, showing Worker → CoordinatedClient → DistributedParameterClient → shard clients → primary servers, manager, backup RPCs and metrics/oracle. The `ds/` package contains server, client, routing, clock, membership, heartbeat, replication and failover implementations; `harness/` supplies seeded workloads, independent numerical oracle, fault scenarios and measurements.

A manager publishes a versioned contiguous `[begin,end)` partition map that covers the `uint64` keyspace. The distributed client splits sparse and dense requests by owner, merges Pull values in caller order and can refresh a stale map on `NOT_OWNER`. A single owner holds the atomic commit lock; **a Push or Pull spanning several servers is not a globally atomic transaction/snapshot**.

Each partition uses range-compressed vector clocks, splitting intervals where updates affect a subset. The manager also tracks active workers' iterations. The available coordination modes are sequential, bounded delay `τ` and eventual. Clock-based causal requirements are separate from global iteration gating.

## 3. Design choices and alternatives

We chose contiguous range partitioning because it supports compact dense RPCs and migration of intervals. A consistent-hash ring could avoid some ownership-map bookkeeping but would complicate range requests. Central management makes membership and consistency decisions explicit; it is a single coordination dependency.

For communication, we implemented three compatible optimisations: cached sparse key-list IDs, reversible zlib encoding of float64 payloads, and Pull batching. Their benefit depends on data repetition and message size, so we measured bytes and p99 latency rather than assuming all optimisations improve speed.

For elasticity, the manager performs an initial range snapshot, tracks writes during the copy and briefly fences an interval for final dirty-key catch-up and ownership publication. We also support graceful server removal and dynamic worker membership. The E4 under-load migration is **in-process**; it demonstrates cutover accounting and correctness, not a general distributed cross-machine transfer service.

For fault tolerance, servers emit heartbeats, maintain ordered backup replicas and delay success acknowledgement until backup acceptance. A confirmed crash can promote a seeded replica, temporarily stop writes and reseed new redundancy before allowing progress. A stronger replicated state machine/consensus protocol would support broader partition and failure models but was outside the implemented scope. The recorded guarantees assume a live manager and surviving replicas.

## 4. Method and reproducibility

The reusable Part B worker runs a seeded `Pull → compute → Push` loop. Tests cover sparse skew, dense ranges, stragglers, 1,024-key X2 scaling, failure injection and independent target/fixed-gradient numerical oracles. B5 records timestamped operation metrics, iterations, convergence, scenario events and a JSON summary. Application bytes mean **serialized Protobuf payloads**, not full transport traffic.

GitHub Actions runs each selected experiment from a clean checkout with Python 3.13 and dependencies pinned by compatible bounds. A separate workflow launches all discovered Part A–F test scripts in isolated Python processes with fresh single-server state. Include the **final passing revision SHA** and a link to its CI evidence in the submitted report.

## 5. Evaluation

### X1: Is aggregation correct?

In synchronous round mode, `m` workers Pull the same snapshot and all commit target-difference gradients before the next round. The analytic contraction is `||w_t−w*|| = |1−mη|^t ||w_0−w*||`. Our independent per-round oracle agreed to a maximum absolute error of approximately **4.44 × 10⁻¹⁶** for the recorded configuration. **Figure 2:** plot measured and theoretical distances from `results/x1_aggregation/oracle.csv`. The formula belongs to this controlled synchronous schedule, not arbitrary asynchronous interleavings.

### X2: Does server count improve throughput?

With a fixed seeded workload, three trials each across **1–8 real server processes**, observed median throughput in Pushes/s was:

| Servers | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Median Pushes/s | 102.4 | 188.8 | 202.3 | 169.7 | 159.9 | 137.9 | 132.5 | 115.2 |

The best measured point was three servers (**1.97×** relative to one); eight achieved **1.13×**. Increasing fan-out, reassembly and local resource contention are possible bottlenecks. **Figure 3:** `results/x2_scaling/scaling.png` compares throughput and Push p99 with server count. These localhost results do not establish multi-host scalability.

### X3: What does relaxed consistency change?

The C5 comparison ran **30 scenarios** (five modes × 0/60 ms straggler × three seeds). Without a straggler, median Push throughput improved from **154/s sequential** to approximately **345/s at τ=1**, **402/s at τ=2**, **404/s at τ=4**, and **410/s eventual**. With a 60 ms straggler, all modes were near **31 Pushes/s**, indicating worker compute time dominated. Sequential observed zero iteration lag; bounded modes respected their τ setting; eventual lag reached 22 with a straggler. Convergence and final errors differed by schedule. **Figure 4:** existing `results/c5_evaluation/` convergence plots, comparing equal update counts and wall time separately.

### X4: Communication trade-offs

Three-trial median comparisons showed cached key IDs saved approximately **9.65%** total payload bytes, compression **16.36%**, caching plus compression **26.01%**, and D3 batch Pulls **64.15%** (1,600→200 physical RPCs, **87.5%** fewer). **Figure 5:** `results/x4_comparison/comparison.png`. The cache/compression costs can increase Push p99; D3's p99 measures a **group of Pulls** and is not directly comparable with Push p99.

### X5: Elasticity while training

An E4 two-to-four-server transition under ongoing Push traffic preserved a final Push-count oracle across 3,926 successful updates. The latest trial measured a **0.00675s** cutover and 5-second-window throughput of **325.2 before** and **326.8 Pushes/s after**. **Figure 6:** `results/e4_elasticity/trial_20261121_150052_626785/{throughput,latency}.png`. This is an in-process migration experiment and should not be compared directly with process-isolated X2 timings.

### X6: Crash survival and exactly-once effects

In a three-server, k=1-replica configuration, a primary was killed after a worker Pull and before its Push. The recorded trial completed **718 acknowledged logical Pushes**, matched the offline fixed-gradient oracle to **9.09 × 10⁻¹³**, and measured detection **0.489s**, promotion **0.019s**, first recovered Push **2.10s** and conservative throughput restoration **3.0s**. The oracle counts logical committed updates, not raw retry attempts. This establishes the configured *single confirmed crash-stop* guarantee, **not** safety under arbitrary partitions.

### X7: Failure matrix and limits

The nine-case matrix includes source/destination crashes during migration staging, one partial postpublication source crash, worker death under bounded delay, delayed/dropped Pushes, a lost success reply and dropped transfer staging. Eight cases recovered or were handled within the explicit fault-injection model. One partial postpublication source failure was **EXPECTED_FAIL_CLOSED**: writes remained blocked because availability could not be recovered safely. The matrix intentionally does *not* describe this row as recovered.

## 6. Conclusion

The project demonstrates a parameter server with asynchronous Push/Pull, partition routing, range clocks, selectable consistency, measured communication optimisations, a live elasticity path, and tested single-crash replication/promotion. Numerical oracles and fault trials provide evidence for the supported configurations. Its main limitations are single-manager dependency, no cross-shard transactions, predominantly in-memory state, in-process E4 migration, and the stated fail-closed postpublication case. The controlled X2 study also shows that adding local servers need not improve throughput monotonically.

**Before submission:** replace figure references with actual labeled images, cite the final CI SHA, verify raw outputs and report pagination, and clearly state any changes between the historical measurements and the final tested commit.
