# Building a Parameter Server — Architecture, Evaluation and Recovery

**Course:** Distributed Systems · University of Edinburgh (INFR11022)  
**Evidence revision:** 358274afb485 · **CI run:** 44947728275

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

GitHub Actions runs each experiment from a clean checkout with Python 3.13 and compatible dependencies. A separate workflow launches every Part A–F regression script in an isolated Python process with fresh server state. The evidence revision and workflow run are recorded above. Experimental timing is host-dependent; reproducible seeds and raw CSV results are retained with the repository.

## 5. Evaluation

### X1: Is aggregation correct?

In synchronous round mode, `m` workers Pull the same snapshot and all commit target-difference gradients before the next round. The analytic contraction is `||w_t − w_target|| = |1 − mη|^t · ||w_0 − w_target||`. Our independent per-round oracle agreed to a maximum absolute error of approximately **4.44 × 10⁻¹⁶** for the recorded configuration. Figure 2 overlays the measured and theoretical contraction distances. The formula belongs to this controlled synchronous schedule, not arbitrary asynchronous interleavings.

### X2: Does server count improve throughput?

With a fixed seeded workload, three trials each across **1–8 real server processes**, observed median throughput in Pushes/s was:

| Servers | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Median Pushes/s | 78.2 | 137.1 | 149.3 | 134.1 | 124.4 | 105.9 | 100.5 | 88.9 |

The highest measured throughput was at **3 servers** (149.3 Pushes/s, **1.91×** the one-server baseline); eight servers achieved **1.14×**. The performance is non-monotonic: increased RPC fan-out, result reassembly and contention on a shared localhost host are plausible causes, not individually established bottlenecks. Figure 3 displays both throughput and Push p99 latency for these same trials. These results do not imply multi-host scale-out.

### X3: What does relaxed consistency change?

The C5 comparison ran **30 scenarios** (five modes × 0/60 ms straggler × three seeds). Without a straggler, median Push throughput improved from **154/s sequential** to approximately **345/s at τ=1**, **402/s at τ=2**, **404/s at τ=4**, and **410/s eventual**. With a 60 ms straggler, all modes were near **31 Pushes/s**, indicating worker compute time dominated. Sequential observed zero iteration lag; bounded modes respected their τ setting; eventual lag reached 22 with a straggler. Convergence and final errors differed by schedule. Figure 4 compares convergence at equal update counts; equal elapsed time can lead to different numbers of updates.

### X4: Communication trade-offs

Three-trial median comparisons showed cached key IDs saved approximately **9.65%** total payload bytes, compression **16.36%**, caching plus compression **26.01%**, and D3 batch Pulls **64.15%** (1,600→200 physical RPCs, **87.5%** fewer). Figure 5 compares application payload bytes and percentage reduction. The cache/compression costs can increase Push p99; D3's p99 measures a **group of Pulls** and is not directly comparable with Push p99.

### X5: Elasticity while training

An E4 two-to-four-server transition under ongoing Push traffic preserved a final Push-count oracle across 3,926 successful updates. The latest trial measured a **0.00675s** cutover and 5-second-window throughput of **325.2 before** and **326.8 Pushes/s after**. Figure 6 shows the observed Push rate around the server-add transition. This is an in-process migration experiment and should not be compared directly with process-isolated X2 timings.

### X6: Crash survival and exactly-once effects

In the process-separated three-server, k=1-replica experiment, a primary was killed after Pull and before Push. The measured trial completed **748 acknowledged logical Pushes** and matched the independent fixed-gradient oracle with a maximum absolute error of **9.09e-13**. Detection took **0.501s**, promotion **0.029s**, and the first recovered Push occurred **0.631s** after the fault. A conservative postfault-only 90% throughput recovery diagnostic was **1.6s**. These values are from the CI run identified above; they are not worst-case bounds. The oracle counts acknowledged logical updates rather than repeated RPC attempts. The demonstrated guarantee is one confirmed crash-stop with a surviving replica and live manager, not arbitrary network partitions.

### X7: Failure matrix and limits

The nine-case fault matrix (below) distinguishes recovered or explicitly handled failures from safety-preserving unavailability.

| Failure injection | Observed outcome |
|---|---|
| Primary crash mid-iteration | Recovered / handled |
| Migration source crash (staging) | Recovered / handled |
| Migration destination crash (staging) | Recovered / handled |
| Partial postpublication migration | Safe refusal (fail-closed) |
| Worker crash (bounded delay) | Recovered / handled |
| Delayed Push request | Recovered / handled |
| Dropped Push request | Recovered / handled |
| Lost Push acknowledgment | Recovered / handled |
| Dropped transfer staging | Recovered / handled |

The postpublication source-crash case is **EXPECTED_FAIL_CLOSED**: writes can remain blocked because safe availability cannot be established. This is not automatic recovery and does not establish partition tolerance.

## 6. Conclusion

The project demonstrates a parameter server with asynchronous Push/Pull, partition routing, range clocks, selectable consistency, measured communication optimisations, a live elasticity path, and tested single-crash replication/promotion. Numerical oracles and fault trials provide evidence for the supported configurations. Its main limitations are single-manager dependency, no cross-shard transactions, predominantly in-memory state, in-process E4 migration, and the stated fail-closed postpublication case. The controlled X2 study also shows that adding local servers need not improve throughput monotonically.

