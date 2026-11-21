# Building a Parameter Server — Architecture, Evaluation and Recovery

**Course:** Distributed Systems · University of Edinburgh (INFR11022)  
**Evidence revision:** {{REVISION}} · **CI run:** {{CI_RUN}}

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

GitHub Actions reruns X1–X4 and X6/X7 from a clean checkout with Python 3.13 and compatible dependencies; X5 uses the separately recorded in-process E4 scale-up trial. A second workflow runs all Part A–F regression scripts in isolated processes with fresh server state. The revision and CI run are recorded above. Timings vary by host, and reproducible seeds and raw CSVs are retained.

## 5. Evaluation

### X1: Is aggregation correct?

In synchronous round mode, `m` workers Pull the same snapshot and all commit target-difference gradients before the next round. The analytic contraction is `||w_t − w_target|| = |1 − mη|^t · ||w_0 − w_target||`. Our independent per-round oracle agreed to a maximum absolute error of approximately **4.44 × 10⁻¹⁶** for the recorded configuration. Figure 2 overlays the measured and theoretical contraction distances. The formula belongs to this controlled synchronous schedule, not arbitrary asynchronous interleavings.

### X2: Does server count improve throughput?

With a fixed seeded workload, three trials each across **1–8 real server processes**, observed median throughput in Pushes/s was:

{{X2_TABLE}}

{{X2_INTERPRETATION}}

### X3: What does relaxed consistency change?

{{X3_SUMMARY}}

### X4: Communication trade-offs

{{X4_SUMMARY}}

{{X4_TABLE}}

### X5: Elasticity while training

An E4 two-to-four-server transition under ongoing Push traffic preserved a final Push-count oracle across 3,926 successful updates. The recorded trial measured a **0.00675s** cutover, five-second-window throughput of **325.2 before** and **326.8 Pushes/s after**, and Push **p95 latency of 2.05ms before versus 1.92ms after**. Figure 6 shows the observed Push rate around the server-add transition. This is an in-process migration experiment and should not be compared directly with process-isolated X2 timings.

### X6: Crash survival and exactly-once effects

{{X6_SUMMARY}}

### X7: Failure matrix and limits

The nine-case fault matrix (below) distinguishes recovered or explicitly handled failures from safety-preserving unavailability.

{{X7_TABLE}}

**EXPECTED_FAIL_CLOSED** preserves safety by fencing uncertain writes, but does **not** restore availability or tolerate arbitrary network partitions.

## 6. Conclusion

The project demonstrates a parameter server with asynchronous Push/Pull, partition routing, range clocks, selectable consistency, measured communication optimisations, a live elasticity path, and tested single-crash replication/promotion. Numerical oracles and fault trials provide evidence for the supported configurations. Its main limitations are single-manager dependency, no cross-shard transactions, predominantly in-memory state, in-process E4 migration, and the stated fail-closed postpublication case. The controlled X2 study also shows that adding local servers need not improve throughput monotonically.

