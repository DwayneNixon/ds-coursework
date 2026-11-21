# Requirement-to-source audit (A1–F5 and X1–X7)

**Audit baseline:** the code audit originated from GitHub `main` at `c213bfcb1386759f758ffa147f368738068e7756`. Fixes and organisation changes are isolated in the draft PR `audit/fix-runner-paths`. **Use the latest passing PR-head CI and its SHA** as final evidence; the historical part READMEs describe earlier states.

Status definitions: **covered** = direct implementation and matching tests/measurements exist for the stated tested scope; **partial** = core behaviour exists but a stated requirement has a boundary or evidence gap; **pending final validation** = pending all-script regression and reproducible final artifact review. No status here asserts production-level fault tolerance.

| Item | Assessment | Current implementation/evidence | Important qualifications |
|---|---|---|---|
| **A1** Parameter storage, aggregation and async RPC | Covered within tested scope | `ds/server.py`, `ds/parameter_client.py`, `parameter_server.proto`, `tests/part_a/test_concurrent_pushes.py`, `test_atomic_push.py`, `test_request_limits.py` | Sum semantics represented by locally atomic SGD applications; no global cross-shard transaction. Client-supported linear rule registration; stable unique IDs needed |
| **A2** Ergonomic API and quick start | Covered | Public client API and root `README.md` quick start, examples, errors | Client connection and worker identity configuration still explicit |
| **A3** Task dependencies | Covered | Client `Task` dependencies and server `_wait_for_dependencies`; `tests/part_a/test_server_dependencies.py` | Local/server task graph, not distributed cycle detection |
| **A4** Single-node tests and B checkpoint | Covered; verify new layout CI | 23 Part A tests; `experiments/b4_scenarios.py` fresh-state integration | B4 scenario hardcodes worker IDs 701/702/703, so it is not a fully generic driver |
| **B1** Pull–compute–Push worker loop | Covered | `harness/worker.py`, `experiments/b1_worker.py`, `run_b2.py` | Explicit barriers in X1 are optional; normal worker remains asynchronous |
| **B2** Seeded sparse skew, dense ranges, million-key generator, stragglers | Partial / covered in separate tests | `harness/workload.py`; `test_b2_sampling.py`, `test_b2_dense_api.py`, `test_b2_worker_dense_range.py`, `test_b2_million_key_distributed.py`; `harness/oracle.py` supports million-key targets | Million-key **logical model** now exercised end-to-end with two in-process gRPC shards, seeded sparse workers and cross-shard dense RPCs; not one million materialised weights or a million-key dense transfer |
| **B3** Independent oracle | Covered | `harness/oracle.py`, X1 analytic oracle, X6 fixed-gradient offline oracle | No universal oracle for arbitrary async schedules |
| **B4** Scenario faults and elasticity | Partial integration | `harness/scenarios.py`, `harness/faults.py`, `experiments/b4_scenarios.py`, F and E dedicated runners | B4 is configured kill/add/delay; later E/F event engines aren't all driven by one generic scenario schema; no arbitrary packet partition proof |
| **B5** CSV/JSON metrics | Covered with omissions | `harness/metrics.py`, `harness/observer.py`, ops/iterations/convergence/events/summary CSV/JSON | Some baseline iteration staleness entries remain `None`; meaningful staleness measured separately in C5. Byte counts exclude transport/backup |
| **C1** Partition and transparent routing | Covered | `ds/routing.py`, `ds/distributed_client.py`; C1 tests | Per-shard atomicity only |
| **C2** Manager, epochs, cached map and retry | Covered | `ds/server_manager.py`, `ds/manager_rpc.py`, C2 tests | Manager is single coordinator in tested deployments |
| **C3** Per-range clocks and growth | Covered | `ds/range_clocks.py`, C3 growth experiment | Keyspace fragmentation grows under random single-key access |
| **C4** Sequential/eventual/bounded τ + clock/dependency checks | Covered for tested configurations | `ds/consistency.py`, `ds/coordinated_client.py`, C4 tests | Gate and clock requirements serve different purposes |
| **C5** Consistency comparison | Covered | `experiments/x3_consistency.py`, `results/c5_evaluation/` | 5 modes × 2 straggler conditions × 3 seeds (30 cases) |
| **D1** Sparse key caching | Covered | `ds/server.py`, `ds/parameter_client.py`, D1 tests, cached-key result logs | Cache misses and IDs handled; small messages may not benefit |
| **D2** Lossless value compression | Covered | `ds/value_compression.py`, D2 tests and trial logs | Compression has CPU and latency trade-offs |
| **D3** Pull request batching | Covered | `ds/distributed_client.py` Pull batch, `experiments/d3_benchmark.py`, `experiments/x4_comparison.py` | Current measured batching is for sparse Pull groups |
| **E1** Join server and range handoff under load | Covered with deployment limit | `ds/server_manager.py`, `ds/server.py`; E1 tests | Direct in-process server method transfers for E4; independent process migration not demonstrated |
| **E2** Graceful server removal | Covered | `tests/part_e/test_e2_graceful_removal.py` | Tested local manager/transfer model |
| **E3** Dynamic workers | Covered | Manager/consistency active set, `tests/part_e/test_e32_dynamic_workers.py` | Worker IDs cannot be reused after leave |
| **E4** Two→four servers, throughput/latency and oracle | Covered in-process | `experiments/x5_elasticity.py`, `results/e4_elasticity/` | X5 benchmark uses in-process transfer, not process-separate scale-out |
| **F1** Heartbeat detection | Covered | `ds/server_heartbeat.py`, `ds/server_manager.py`, F11/F12 tests | Detection speed ↔ false positives; manager must remain alive |
| **F2** Replica ACK fencing and batch option | Covered for k=1 cases | `ds/replica_transport.py`, `ds/server.py`, F21–F24 tests | No consensus/partition tolerance; server state primarily volatile |
| **F3** Promotion, repair, exactly-once retries | Covered for confirmed single crash | `ds/failover_controller.py`, `ds/failover_retry.py`, F31–F34, X6 | Partial postpublication source failure can remain fail-closed; two unrelated server deaths not proven |
| **F4** Worker exclusion on failure | Covered for tested timeout model | `ds/worker_heartbeat.py`, F41/F42 tests | In-flight old worker Pushes are not individually fenced at primary in all scenarios |
| **F5** Failure matrix | Covered with explicit fail-closed | `experiments/x7_failures.py`, 9-case matrix | Eight handled/recovered, one `EXPECTED_FAIL_CLOSED`; no broad network-partition proof |
| **X1** Analytical aggregation | Covered | `experiments/x1_aggregation.py` and measured oracle | Explicit synchronized rounds |
| **X2** 1–8 server scaling | Covered | `experiments/x2_scaling.py`, three-seed process-separated benchmark | Non-monotonic local results; preserve final-run artifacts |
| **X3** Consistency/straggler evaluation | Covered | `experiments/x3_consistency.py`, 30 cases | Local, small model and measured schedules |
| **X4** Byte/latency optimisation comparison | Covered | `experiments/x4_comparison.py`; D1/D2 original logs, new D3 CSV | D3 Pull-group p99 differs from Push p99 |
| **X5** Elasticity in workload | Covered with caveat | `experiments/x5_elasticity.py`; E4 trial plots | E4 manager/server movement is in-process |
| **X6** Real crash and offline oracle | Covered for single confirmed crash | `experiments/x6_failover.py` and recorded X6 | No generic multi-failure/partition guarantee |
| **X7** Multiple failures and messages | Covered with fail-closed | `experiments/x7_failures.py`, 9-case matrix | One intentionally unavailable safety-preserving case |

## Audit warnings that should remain visible

1. **B4/E4/F scenario cohesion:** the reusable worker, client and metrics are shared, but X5–X7 orchestration is not one interchangeable JSON scenario engine. Describe this accurately.
2. **Worker identity:** a newly constructed `ParameterClient(worker_id=old_id)` starts its sequence again. The server will deduplicate matching old IDs. This is a documented limitation/policy rather than a silently resolved failure.
3. **Scaling representativeness:** X2's 1,024-key, 4-worker benchmark is a fixed local workload, not the realistic million-key heavy workload from the handout's example. Sparse Zipf/dense and 1e6 key generator are separately tested.
4. **Recovery:** X6 matches its independent oracle for the acknowledged logical operations in one confirmed crash-stop model; this does not prove linearizability under arbitrary partitions.
5. **Results:** historical X3/X5/X6/X7 data are committed; X1/X2/X4 canonical output should be committed from a final passing CI run rather than trusting earlier run artifacts.
6. **Submission:** a Markdown draft and demo guide exist; a ≤8-page formatted report and an actual ≤10-minute recorded video still require human preparation/verification. No automatic claim of finished media.

These distinctions are deliberate safeguards against overstating coursework completeness.
