# Distributed Systems Coursework — Part F: Fault Tolerance

University of Edinburgh · Distributed Systems (INFR11022) · Building a Parameter Server

This document covers **Part F (F1–F5)** and the related **X6/X7 experiments**. It follows the detailed, evidence-led approach of `Part_E_README.md`: official requirements, design alternatives, architecture and invariants, incremental milestones, real tests and measured results, reproduction instructions, known limitations, and the final project-audit handoff.

**Status — 21 November 2026:** **Part F implementation and the planned local F1–F5 / X6–X7 tests are COMPLETE within the tested fault model.** The user ran the X6 fixed-gradient real-server-crash experiment, obtaining **718 successful logical Pushes**, a maximum offline-oracle error of **9.094947017729282e-13**, and recovery timings below. The user also ran the nine-row X7 matrix: **eight recovered/handled outcomes and one correctly classified `EXPECTED_FAIL_CLOSED` outcome**, with `X7 matrix status: PASS`. Passing the matrix does **not** mean every injected failure recovered transparently or that arbitrary failures are tolerated.

> **Continuity from Parts A–E:** A supplies float64 parameter values, async Push/Pull, Task IDs and SGD; B supplies reproducible worker loads, deterministic gradients, oracle and evidence schemas; C supplies owned key ranges, ownership epochs, vector clocks and consistency; D supplies sparse-key caching/compression/batching; E supplies live migration and dynamic worker membership. F adds heartbeat-driven failure detection, acknowledged in-memory replicas, confirmed-crash promotion, worker-timeout exclusion and controlled failure experiments. This is an extension of the same system, not a replacement.
>
> **Evidence provenance:** Numerical results and PASS messages were supplied from the user's **Windows PowerShell** runs on **21 November 2026**. The source files for the incremental F milestones and the Part E README were available for static inspection when preparing this document. Some earlier tests were run in a local compatibility fixture during implementation; that is distinguished from user-run full-project evidence. The user's final Git revision, full dependency lock, complete local results directories and an independent clean-checkout all-parts audit have **not** been supplied. These limitations matter before declaring the *overall coursework project* finished.
>
> **Important:** The older `Part_E_README.md` is a historical Part E checkpoint and says several failure paths were not yet implemented. This README reports the later Part F additions; it does not retroactively change the tests executed at the Part E checkpoint.

---

## 1. Requirements and scope

The official handout, `Distributed Systems - Coursework.pdf`, **Part F, pp. 9–10**, specifies:

| Item | Requirement | Implemented/tested Part F response | Evidence |
|---|---|---|---|
| **F1 — Failure detection** | Heartbeats and configurable timeout; explain speed versus false positives | Manager server-health registry; server heartbeat sender; periodic monitor; suspicion distinct from confirmed death | F1.1 and F1.2 PASS; real subprocess kill detected |
| **F2 — Replication** | Each primary range replicated to **k ≥ 1** neighbours; Push ACK only after replication; configurable per-Push versus batch | Deterministic clockwise neighbour plan; snapshot seed; ordered after-images, clocks and dedup history; per-Push and opt-in batched ACK-after-backup; process-separated gRPC transport | F2.1–F2.4 tests PASS |
| **F3 — Server failure and recovery** | Promote replica, publish ownership, redirect/retry in-flight work idempotently, continue training; report `T_rec=T_detect+T_promote+T_resume` | Confirmed-death promotion, ownership epoch, repair/reseed/release, failover controller, preserved Task-ID retry, crash handling around fenced migration | F3.1–F3.4 PASS; X6 real crash/oracle PASS; known partial-cutover limit |
| **F4 — Worker failure** | Crashed worker cannot block sequential/bounded gates forever; state correctness effects | Independent worker heartbeats, manager timeout exclusion, historical progress retention, opt-in `CoordinatedClient` sender | F4.1 16/16; F4.2 8/8; E3.2 regression PASS |
| **F5 — Failure matrix** | Kill server mid-iteration/during E1 transfer, kill worker under bounded delay, delay/drop messages; state exact guarantees | Configured nine-case X7 runner: real process crashes, deterministic RPC-boundary faults, separate recovered and fail-closed classifications | X7 9/9 verified; one expected fail-closed |

The handout's **X6** (§5.8, p. 14) asks for fixed gradients, a mid-run server kill, final weights equal to an **offline** oracle, and recovery time. **X7** (§5.8, p. 15) asks for the F5 failure matrix and outcomes. Both were exercised. Broader harness integration and X1/X2 gaps belong to the final A–F project audit, not to additional Part F implementation work in this phase.

### 1.1 Deliberately selected fault model

- **Local cluster, independent parameter-server OS processes**, real gRPC calls for the worker data plane and replica/migration-control transport. The manager/controller is a live coordinator, not a replicated consensus service.
- **One confirmed crash-stop server failure** at a time in the successful promotion experiments; a process supervisor callback confirms that the server actually exited before promotion. **SUSPECTED** alone does not authorize promotion, reducing split-brain risk.
- **At least one live, seeded, current replica** of the failed range. X6 uses three server processes and `k=1`; other configurations may be supported by the placement code but are not established by the X6 evidence.
- **In-memory replication:** a successful ACK means the required backup(s) have applied/accepted the update in memory under the tested protocol, **not** that either copy has been written to durable storage.
- **One-shot controlled delay/drop faults** for the message matrix, rather than unrestricted packet loss, network partition, concurrent node failures or Byzantine behaviour.
- No manager replication, leader election, disk checkpoint/recovery, process restart automation or globally atomic multi-owner transactions. These are optional or outside the selected scope.

---

## 2. Architecture after Part F

```text
          Training workers                      Manager/control plane
  DistributedParameterClient             ServerManager + ManagerService
  (map/epoch refresh, Task IDs,           - ownership and replication plans
   failover retry, clocks, D1/D2)         - server/worker health registries
             |                            - ConsistencyGate / membership epoch
      CoordinatedClient (opt-in)          - promote / restore / move_range_replicated
      - join, barrier, completion         - AutomaticFailoverController
      - independent WorkerHeartbeat       - confirmed process death hook
             |                                      |
             +---------- manager gRPC -------------+
             |                                      |
          Push/Pull gRPC                         replica/control gRPC
             |                                      |
  +----------------------+      +----------------------+      +----------------------+
  | ParameterServer S1   |----->| ParameterServer S2   |----->| ParameterServer S3   |
  | primary partitions   |      | own primaries        |      | own primaries        |
  | in-memory replicas   |<-----| in-memory replicas   |<-----| in-memory replicas   |
  | Task-ID dedup        |      | clocks, epochs       |      | clocks, epochs       |
  +----------------------+      +----------------------+      +----------------------+
        example clockwise ring, k=1; arrows represent backup destinations

  Failure experiment and evidence plane:
  run_f51_x6.py + f5_evidence.py -> results/f51_x6/{ops,iterations,
      convergence,events}.csv + summary.json
  run_f52_x7.py + f52_x7_matrix.py + focused fault tests ->
      results/f52_x7/{matrix,events,ops,iterations,convergence}.csv,
      summary.json, case_logs/, case_metrics/
```

An **ownership epoch** versions key-to-primary routing and the backup plan; a **worker-membership epoch** versions active consistency participants; a **per-partition replication sequence** orders individual committed changes. None of these is a substitute for the others. The authoritative manager and its epoch must survive for the tested recovery workflow to complete.

### 2.1 Main source and test map

| Module | Part F responsibility | Relationship to earlier parts |
|---|---|---|
| `server_manager.py` | Heartbeat registry and monitor; neighbour replication plan; snapshots/activation; confirmed-crash promotion and repair; worker timeout registry; fenced crash-aware migration | Extends C manager and E migration/membership logic |
| `manager_rpc.py`, `manager.proto` | Existing `Heartbeat`; new `WorkerHeartbeat` endpoint; retains GetMap, CanStart, CompleteIteration, JoinWorker, LeaveWorker | Manager Protobuf bindings must be regenerated after protocol edits |
| `server_heartbeat.py` | Independent server-to-manager heartbeat sender | Server startup integration, configured by command-line arguments |
| `server.py` | Replica export/import, ordered update/ACK path and batching; promotion and repair gates; migration fencing, staging, rollback/finalization | Retains A–E data-plane semantics and update rules |
| `replica_transport.py` | gRPC-backed proxies and transport for replica and migration control across distinct server processes | F2.4 moves beyond E's direct-object-only control path |
| `failover_controller.py` | Observe suspected server, verify dead process, promote and repair; retry until controller-ready | F3.3 automation; no automatic process restart |
| `failover_retry.py` | Physical retry support after a recoverable owner failure | F3.3, invoked on recoverable errors under defined boundaries |
| `parameter_client.py`, `distributed_client.py` | Preserve logical Push/task identity, dedup-safe retry, refresh owner on failover | Reuses C routing and D caching/compression; opt-in `failover_retries` |
| `consistency.py` | Crash exclusion from active barriers while preserving completed history | Extends C consistency and E dynamic membership |
| `worker_heartbeat.py`, `coordinated_client.py` | Independent worker heartbeat and optional automatic lifecycle on training operations | F4.2 avoids changing the Part B worker emulator |
| `f5_evidence.py` | Independent fixed-gradient oracle, weight comparison, phase and throughput metrics, CSV/JSON export | Reuses B deterministic gradient function and output conventions |
| `run_f51_x6.py`, `f51_x6_config.json` | Real server crash during continuous fixed-gradient training | X6 measurement driver |
| `f52_x7_matrix.py`, `run_f52_x7.py`, `f52_x7_config.json` | Run and classify nine fault cases; validate live X6 artifacts; output reproducible matrix | X7 report driver; focused subprocess tests |
| `test_f11_*.py` through `test_f52_*.py` | Incremental unit/process fault-injection and evidence validation | Runs in project root unless deliberately reorganised later |

**Protobuf nuance:** `manager.proto` adds the worker-health `WorkerHeartbeat` request/reply; the existing `Heartbeat` is for **servers**, not training workers. `replica_transport.py` uses the already-available server-side RPC/transport mechanisms; the F4.2 client integration did not require further `.proto` edits. Generated `*_pb2*.py` files should match the checked-in `.proto` sources.

### 2.2 End-to-end invariants targeted

1. A successful **replicated Push ACK** occurs only after the required backup(s) accept the corresponding ordered record or batch; failure leaves the operation unacknowledged/fenced rather than falsely claiming durability.
2. Backup state includes committed **weights**, relevant **range-vector-clock segments**, and **Task-ID/fingerprint dedup** records; copying weights alone is insufficient for safe replay.
3. Replica writes are sequenced by **primary ID, partition ID, ownership epoch and monotonically increasing replication sequence**; stale, conflicting and out-of-order records are rejected.
4. A server timeout creates **suspicion**; confirmed crash-stop evidence is needed before transferring ownership. The manager updates the routing epoch so stale clients must refresh.
5. A promoted primary is **not automatically writable**: replica reseeding/repair must finish, then a controlled release enables acknowledged new writes.
6. A lost ACK can be retried using the **same logical Task ID and identical request**; the new/current owner must not apply the same completed update twice within the supported identity/range scope.
7. In crash-aware migration, **unpublished destination state is speculative**; after publication the manager must **not roll back the ownership epoch**. The source/destination repair protocol must be resumable or remain safely fenced.
8. Worker timeout changes **active barrier membership** but must not erase historical completion values or range clocks.
9. Experimental PASS labels must be based on actual test assertions/evidence; an **expected fail-closed** state is not equivalent to availability or recovered training.

These are verified for the explicitly tested configurations and injected boundaries—not proof of a general exactly-once distributed transaction protocol.

---

## 3. F1 — Detect server failures

### 3.1 Alternatives and design decision

| Approach | Advantage | Limitation | Decision |
|---|---|---|---|
| Manager actively probes each server | Simple single monitoring component | Probe errors conflate server failure and link failure | Not primary mechanism |
| **Periodic server-initiated heartbeat** | Easy independent process integration and timestamps at one coordinator | Timing assumptions; false suspicion during scheduling/network stalls | **Chosen** |
| Automatic promotion as soon as a heartbeat expires | Low latency | Can produce two primaries if the old server is merely unreachable | **Rejected**: require confirmed death |
| Quorum/consensus failure detector | Stronger coordination under partitions | Additional replicated control plane and complexity | Not selected |

`ServerManager` registers each server as `REGISTERED`, accepts manager-timestamped heartbeats into `ALIVE`, then transitions overdue servers to `SUSPECTED`. A confirmed promotion marks the old authority unusable. Recovered heartbeat reception can clear suspicion *before* a confirmed crash, but a failed/fenced process cannot simply heartbeat itself back into authority. Server-side `server_heartbeat.py` sends periodic liveness RPCs independently of worker Push/Pull traffic. The monitor lifecycle is explicitly started/stopped (no unsolicited background monitor at object construction).

**Trade-off:** A smaller timeout improves detection latency but increases false-suspicion risk under CPU contention, GC pauses and delayed packets. Increasing the timeout reduces false positives but lengthens the outage before promotion can start. Choose `heartbeat_timeout_s` and the monitor interval for the local workload; do **not** treat `SUSPECTED` as proof a server stopped. The F3 controller checks a process-supervisor `is_confirmed_dead` callback.

### 3.2 Evidence

- `test_f11_heartbeats.py`: **8/8 PASS** in the reported original F1.1 milestone (registry, validation, timeout/suspicion, state isolation and compatibility cases).
- `test_f12_automatic_failure_detection.py`: **7/7 PASS** in the reported original F1.2 milestone; actual subprocess termination was detected. One observed F1.2 kill-to-detection measurement was approximately **0.589 s**. Other reported monitor/process observations varied with scheduling.
- X6 later measured **`T_detect = 0.4892331000883132 s`** under the X6 configuration, rather than assuming the F1 microbenchmark latency applies to every run.

---

## 4. F2 — Replicate each range before acknowledging a Push

### 4.1 Placement and initial state

`ServerManager.configure_replication(server_ids, k)` makes a deterministic **clockwise ring** from the supplied server ordering. For each primary partition, the backup IDs are the next `k` *distinct* servers. The supported configuration requires `1 <= k < number_of_servers`; `k` is a replication count **excluding** the primary. `seed_replicas(servers)` copies an initial consistent snapshot to those backups **before live training begins**; activation verifies matching range, epoch, value width, learning rate, update rule, weights, clocks and Task-ID metadata.

Example with ring `S1,S2,S3` and `k=1`: S1's primaries back up to S2, S2's to S3, and S3's to S1. A backup can simultaneously own its own primary ranges. Replicas are **not** advertised as owners to ordinary workers until promoted.

### 4.2 Ordered after-image replication

The normal data-plane Push still executes under the server's parameter and Push-serialization locks. For a successful operation the primary builds an **after-image** of affected weights, the complete relevant clock snapshot, the Task ID/fingerprint and a partition-local sequence number. Copies are applied to designated backups in sequence. Replaying the identical last record is idempotent; a conflicting replay or unexpected sequence is rejected. Batching validates and applies the ordered set on a backup as a unit, not as a partially exposed prefix.

After-image copying is intentional: custom update rules need not be linear, so merely replaying the gradient on a backup can calculate the wrong state.

### 4.3 Per-Push versus batched replication

| Policy | Configuration | When a worker receives success | Benefit/cost |
|---|---|---|---|
| **Per Push** | `batch_size=1` | After that Push's ordered record is accepted by configured backup(s) | Small individual recovery unit; more replication messages |
| **Batched** | `batch_size>1`, `batch_timeout_s>0` | After its **whole batch** has been accepted on required backup(s) and the batch has committed | Fewer messages; extra waiting, potential exposure of unacknowledged in-flight batch on crash |

Both modes keep **ACK-after-replication**. Batching here changes the **replication/send granularity**, not the fact that a completed ACK requires backup acceptance. Before a batch flush, the worker **cannot assume** its unacknowledged Push survived; after a successful ACK, it can rely on the tested surviving in-memory replica(s), **not** disk persistence or survival of both/all replica failures. A timeout or lost ACK is an **ambiguous outcome**, requiring unchanged Task-ID retry. Do not claim that batching acknowledges writes before backup or that ACK implies durable disk storage.

Typical manager setup, while quiescent:

```python
manager.configure_replication(server_ids, k=1)
manager.seed_replicas(peers)
manager.activate_live_replication(
    peers, batch_size=1, batch_timeout_s=0.05,
)
# For bounded aggregation: batch_size=4 (or another positive integer).
```

### 4.4 Cross-process control transport and observed tests

F2.1–F2.3 initially proved replication with co-located Python objects. **F2.4 added `replica_transport.py`** so primary/backup operations can reach real *separate server processes* via gRPC. It also validated process-kill survival of replicated state, rather than treating an in-process reference as independent fault tolerance.

| Milestone | Reported test script | Reported result |
|---|---|---|
| F2.1 — Plan, snapshots and seeding | `test_f21_initial_replicas.py` | **11/11 PASS** |
| F2.2 — Ordered live per-Push replication | `test_f22_live_replication.py` | **12/12 PASS** |
| F2.3 — Batched, ACK-fenced replication | `test_f23_batched_replication.py` | **15/15 PASS** |
| F2.4 — gRPC replicas and process crash | `test_f24_process_replication.py` | **6/6 PASS**; actual process-death cases |

**Limit:** Seeds and replication state live in process memory. Initial seeding must occur before writes begin. Multiple concurrent backup failures, unreliable distributed transactions, persistent redo logs and replica reconfiguration under arbitrary network partitions were not validated.

---

## 5. F3 — Recover a failed primary and keep training

### 5.1 State machine and write fencing

F3 separates four operations to avoid an unsafe shortcut from `SUSPECTED` to a writable new primary:

```text
Heartbeat expires -> SUSPECTED
        |
        +-- no process-death confirmation --> stay suspected; do not promote
        |
        +-- confirmed crash-stop --> choose seeded, healthy replica
                   -> prepare promotion (validate epoch/state)
                   -> publish new owner / ownership epoch
                   -> commit promotion (initially protected)
                   -> repair/reseed backup plan on survivors
                   -> release write fences after replication verified
                   -> clients refresh map and retry with original identity
```

The manager and survivors must still be running. The replica contains the failed primary's latest **acknowledged** weights, vector clocks and completed Task-ID history. The new epoch fences stale replica records and directs stale worker routes to refresh. A promoted primary may temporarily answer only protected/read-only operations until repair succeeds; it must not accept unreplicated new Pushes as if redundancy were healthy.

### 5.2 Exactly-once *effects* within the tested scope

`failover_controller.py` monitors suspects and checks an actual process death callback. F3.3a adds automatic promotion/repair and `failover_retry.py`; F3.3b integrates opt-in retries into the existing `DistributedParameterClient`, preserving Task IDs and physical retry semantics. The client must not assign a new Task ID to an ambiguous committed Push. Existing map refresh and selective `NOT_OWNER` handling remain in use, alongside D1 epoch-scoped key-list caching and D2 compression settings.

Example client construction:

```python
worker = DistributedParameterClient.from_manager(
    manager_address,
    worker_id=101,
    value_width=2,
    failover_retries=True,
    failover_timeout_s=18.0,
)
```

**Boundary:** The implementation deliberately refuses some ambiguous **partition-boundary changes** instead of pretending one logical request can always be replayed exactly once after a split. A multi-owner logical Push is not a single atomic transaction; arbitrary restart/reuse of worker identity or Task-ID allocation is not fully solved. The supported claim is **deduplicated effects for tested acknowledged/retried requests**, not unlimited global exactly-once semantics.

### 5.3 Replication repair and uncertain acknowledgments

After promotion, backup assignments may change. `restore_replication_after_failover()` coordinates repair snapshots, new per-range replica state and explicit release. The F3.2 tests cover repair after actual failures, state/clock preservation, blocked writes until a valid backup exists, and idempotent release/retries. A lost reply to a repair/release RPC must not allow a later retry to overwrite a **newer** acknowledged Push. Pending phase/epoch bookkeeping is therefore central to the recovery protocol.

### 5.4 Server failure during E1 range migration (F3.4)

**Part E's ordinary E1 path** uses live two-phase snapshot and dirty-state catch-up, with a **short moving-range cutover fence**, and continues serving unaffected data during copying. **Part F's crash-safe `move_range_replicated(...)` path is deliberately more conservative:** it fences **new Pushes across all participating primaries for the duration of the replicated transfer**. Thus it trades E1's maximum during-copy availability for a simpler, testable crash-rollback boundary; do **not** describe this F3.4 path as fully live under the copy.

The F3.4 sequence is:

```text
1. Verify ring live, replication seeded/active and old map epoch current.
2. Fence/drain Pushes on the surviving ring; preserve last ACKed replicas.
3. Source snapshots the moving range; destination stages it.
4. Freeze + catch-up -> destination installs speculative incoming state.
5. If source/destination died BEFORE map publication:
     - discard unpublished destination install and cancel old transfer where live;
     - confirm the crash, promote from the OLD replica, rebuild replication;
     - keep old partition authoritative (never publish speculative range).
6. If no crash: publish new owner exactly once (next ownership epoch).
7. Record pending postpublication repair BEFORE further RPCs.
8. Finalise outgoing source, reseed new-epoch backups, release fences.
9. If postpublication RPC reply is lost: resume same published epoch,
   not a second publication or an overwrite of later acknowledged Pushes.
```

**Known unsupported postpublication case:** If a **partial** range moved and the source then crashes after publication while it still owns retained left/right subranges, F3.4 cannot safely reconstruct all retained state for that published cutover. It therefore **fails closed** and keeps writes fenced. This preserves safety at the expense of availability: **training may remain blocked**. It is a *deliberate limitation*, not recovered service, and appears as `EXPECTED_FAIL_CLOSED` in X7. A complete-range publication has a separately tested successful crash recovery path.

### 5.5 F3 verified milestones

| Milestone | Main test | Reported result |
|---|---|---|
| F3.1 — Confirmed-death promotion, owner epoch, Task-ID dedup | `test_f31_replica_promotion.py` | **7/7 PASS** |
| F3.2 — Replication repair, write fences and release | `test_f32_replication_repair.py` | **7/7 PASS** |
| F3.3a — Automatic controller and physical retry | `test_f33_automatic_recovery.py` | **10/10 PASS** |
| F3.3b — Distributed client retry/routing/cache/compression | `test_f33b_distributed_worker_recovery.py` | **13/13 PASS** |
| F3.4 — Migration crash windows, rollback/release/retry | `test_f34_migration_crash_recovery.py` | **12/12 PASS** in **19.525 s** (user Windows run) |

F3.4's twelve cases included: full/partial successful migration, source death after staging/installation and after full publication, destination death during staging, crash before publication of a partial transfer, failure after a partial publication (**expected fail closed**), noncrash rollback, and lost finalize/release/postpublication callbacks.

---

## 6. F4 — Crashed workers and consistency barriers

### 6.1 Why E3 graceful leave is insufficient

E3 lets a worker call `LeaveWorker` after reporting its final Push. A process killed during training never executes that cooperative RPC, so sequential (`tau=0`) or bounded-delay (`tau>=1`) consistency may wait forever for its progress. F4 keeps **active membership** separate from **historical completion history** and introduces independently refreshed liveness.

The manager's `configure_worker_failure_detection(timeout_s=...)` enables worker monitoring after `configure_consistency(...)`. Worker heartbeat state moves from `REGISTERED` to `ALIVE`; once absent beyond the timeout the manager calls `ConsistencyGate.exclude_failed_worker(worker_id)` and marks it `EXCLUDED`. Exclusion wakes barrier waiters and increases membership epoch without erasing completed iterations. An excluded identity cannot reactivate an old session by sending a late heartbeat or completion report, and historic IDs cannot be reused.

The all-workers-dead case is explicit: an empty active set is **not** allowed to pass a barrier vacuously. A fresh worker ID can subsequently join according to the documented membership rules.

### 6.2 Worker heartbeat RPC and automatic client lifecycle

`manager.proto` extends `OwnershipManager` with `WorkerHeartbeat(WorkerHeartbeatRequest) -> WorkerHeartbeatReply`. Request carries `worker_id: uint32`; response includes `accepted: bool` and `membership_epoch: uint64`. The manager timestamps **arrival** using its own monotonic clock; worker wall-clock time is not trusted. The liveness signal does **not** mean the worker has completed any iteration.

`worker_heartbeat.py` sends independent background heartbeats. `CoordinatedClient` enables them **only when opted in**, preserving older C/E call sites:

```python
worker = CoordinatedClient(
    distributed,
    manager_address,
    worker_id=11,
    total_iterations=10,
    worker_heartbeats=True,
    heartbeat_interval_s=0.1,
    heartbeat_rpc_timeout_s=0.4,
)
```

The heartbeat starts automatically on first training work (including a barrier wait, Pull or Push); for a dynamic newcomer it starts **after** `JoinWorker` succeeds. It continues while the training thread is stalled at a legitimate barrier. On successful `LeaveWorker`, the sender stops; on a failed leave it remains live for retry. `close()` stops the local sender but **does not** misreport a graceful departure. A crash without leave is therefore detected by timeout.

**Trade-off and correctness limit:** An unresponsive but not-dead worker may be excluded during network/scheduler stalls, altering which workers influence sequential/bounded progress. A request already in flight may still commit at a parameter server even though its worker was excluded by the manager; F4 **does not fence the old session's in-flight Push at every server**. Final correctness claims must count actually committed/acknowledged updates, and tests must not interpret timeout exclusion as rollback.

### 6.3 Evidence and regression

| Test | User-reported result | Notable observation |
|---|---|---|
| `test_f41_worker_failure.py` | **16/16 PASS** in 1.028 s (first run), later 16/16 PASS in 0.965 s | Real killed worker excluded after **0.538 s**, later **0.500 s** |
| `test_f42_coordinated_worker_heartbeats.py` | **8/8 PASS** in **4.583 s** | Real coordinated worker kill excluded after **1.022 s**; surviving worker progressed and weight oracle held |
| `python -m Part_E_Tests.test_e32_dynamic_workers` | **All named E3.2 checks PASS** | Initial/join/leave, exact six-Push final `[-0.6,-0.6]`, history `{1101:3,1102:1,1103:3}`, legacy `CanStart` |

These are different scenarios/configurations; do not quote one exclusion latency as a universal bound.

---

## 7. X6 — Continuous fixed-gradient server-crash experiment

### 7.1 Design and parameters

`run_f51_x6.py` runs the actual primary-server failure, not a simulated failure-result row. It uses the `harness.oracle.fixed_gradient` function to produce deterministic gradients; `f5_evidence.py` independently calculates final expected weights from **completed logical worker iterations**, ignoring the number of network attempts/retries. An independent observer fetches the final model through normal distributed Pulls, and the numerical comparison must pass before evidence is considered successful.

Recorded `f51_x6_config.json`:

| Factor | Value |
|---|---|
| Experiment / seed | `X6_fixed_gradient_server_kill` / **42** |
| Parameter servers | **3** independently launched processes |
| Replication | Clockwise **k=1**, `batch_size=1` |
| Workers | IDs **101, 102** |
| Parameter width | **2** float64 values per key |
| Gradients | **fixed**, deterministic per worker/iteration/key/component |
| Worker compute delay | **25 ms** |
| Pre-fault workload | **6.2 seconds** configured; five-second comparison baseline |
| Post-fault workload | **6.2 seconds** configured |
| Injected event | Kill **first primary** after a worker's Pull but before that iteration's Push |
| Outputs | `results/f51_x6/` |

The runner starts a manager, three heartbeat-enabled server processes, seeded live gRPC replicas and the automatic controller. It waits for an explicit worker checkpoint **between Pull and Push**, records the kill event and forcibly terminates that process. Workers continue through retries. The final observer Pull, independent oracle, event log and operation count form mutually checked evidence. **Scheduling can vary completed-operation counts between repeats even when gradient generation is seeded**; seed 42 reproduces the input streams, not a bit-for-bit timing trace.

### 7.2 Numerical oracle and exactly-once evidence

For each key `k` and vector component `j`, SGD with the recorded learning rate applies the fixed gradient once per **successfully completed logical** iteration touching the key:

```text
expected_weight[k,j] = initial_weight[k,j]
                       - learning_rate × sum(fixed_gradient(worker,iteration,k,j)
                                              for each recorded completed Push touching k)
```

The oracle is derived without replaying actual retry counts. A duplicate committed Push or a lost recorded committed update would change the observed final weight and fail the comparison (subject to numerical tolerance and the observable update set). This is strong evidence for **exactly-once effects in this single-crash trial**, not a proof of global exactly-once under arbitrary concurrency/partitions/restarts.

**User-reported result:**

```text
X6 fixed-gradient server crash: PASS
Completed Pushes: 718
Maximum oracle error: 9.094947017729282e-13
Detection/promotion/resume: {
  'T_detect_s': 0.4892331000883132,
  'T_promote_s': 0.01908909995108843,
  'T_resume_to_first_push_s': 1.591653999988921,
  'controller_ready_s': 2.0780318001052365,
  'T_rec_first_push_s': 2.0999762000283226
}
Throughput-based recovery: 0.09999999997671694
Results: results/f51_x6
```

The error is close to float64 numerical round-off. The experiment did **not** capture a durable-disk guarantee, and 718 is the count for **this particular run**, not a fixed target for future reproductions.

### 7.3 Decomposition of recovery time

The coursework requests:

```text
T_rec = T_detect + T_promote + T_resume
```

| Component | X6 observed value | Definition in this experiment |
|---|---:|---|
| `T_detect` | **0.489233 s** | Injected process kill → manager detected failed primary |
| `T_promote` | **0.019089 s** | Detection → promoted ownership |
| `T_resume` | **1.591654 s** | Promotion → first observed successfully completed recovered Push |
| **`T_rec_first_push`** | **2.099976 s** | Kill → first successful recovered Push; sum above |
| `controller_ready_s` | **2.078032 s** | Kill → controller records replication repair/release ready |

`controller_ready_s` and `T_rec_first_push` are **different endpoints**. Do not replace the first successful worker Push with the controller's readiness event when reporting the handout decomposition.

### 7.4 Throughput-based recovery: important carryover correction

The experiment uses **one-second sliding throughput windows**, sampled every **0.1 seconds**, and takes the median of five pre-fault one-second windows as the baseline. The user separately inspected the saved `summary.json`:

| Quantity | Observed |
|---|---:|
| Pre-fault throughput median `theta_pre` | **70.0 Pushes/s** |
| 90% threshold | **63.0 Pushes/s** |
| Raw one-second sliding-window recovery | **~0.100 s** |
| **Post-fault-only 90% throughput recovery** | **3.000 s** |

**Why both values are recorded:** At `t_fault + 0.1`, the usual one-second window still spans **0.9 seconds before the crash**, so it can meet 63 Pushes/s without the recovered system having processed enough new work. Thus **0.100 s must not be reported as genuine post-crash throughput restoration**. The additional `postfault_only_recovery_s` counts **only completions after fault injection** inside each sampled window, reaching the threshold at **3.000 s**. The first resumed Push (~2.100 s) and restored throughput (3.000 s) are complementary metrics. This bias/correction is a key interpretation for the coursework report.

### 7.5 X6 artifacts

```text
results/f51_x6/
    ops.csv          # successful/failing per-operation attempts and timing fields
    iterations.csv   # per-worker per-iteration records
    convergence.csv  # §5.7-compatible header; no target_diff convergence curve here
    events.csv       # kill_server + recovery_ready, with phase and kill confirmation
    summary.json     # seed, oracle agreement/error, recovery, throughput samples
```

The `summary.json` reports worker-facing Protobuf payload byte estimates only; this **excludes** backup replication traffic, gRPC/TCP headers and cross-process infrastructure overhead. The `convergence.csv` is intentionally header-only for a fixed-gradient oracle case; the measured numerical error to the offline final model is in `summary.json`, not a target-difference convergence curve.

---

## 8. F5 / X7 — Nine-case failure matrix

### 8.1 Methodology

`f52_x7_config.json` records **seed 42** and all cases. `run_f52_x7.py` first **validates the existing real X6 evidence** (summary, numerical oracle, confirmed process-kill event, operations count and seed consistency); it does **not** rerun the expensive continuous X6 experiment for matrix row 1. The remaining **eight cases** run individual, isolated actual tests using `python -m unittest -v`. Their external processes/gRPC or deterministic test-only RPC shims inject faults at specified boundaries, assert resulting state, and write case logs/metrics. The matrix generator rejects missing cases, failed/skipped test results, stale case metrics, mismatched X6 seed, invalid oracle evidence and a false claim that fail-closed means recovered.

The X7 entry points are separate from the generic Part B declarative `scenario` scheduler. They reuse the established client/harness/manager infrastructure and recorded configs but **do not demonstrate a fully unified generic Part B fault-injection engine for every X7 case**. Treat that as a project-audit/reproducibility integration consideration.

### 8.2 User-verified X7 outcomes (21 November 2026)

| Case | Injected fault / timing | Verified result | Precisely supported conclusion |
|---|---|---|---|
| `x6_primary_mid_iteration` | Kill primary **after Pull, before Push** | **PASS** (validated stored real X6 run) | 718 completed logical Pushes; fixed-gradient oracle matches; one confirmed crash-stop |
| `migration_source_during_staging` | Kill source after incoming E1 state staged, **before publication** | **PASS** | Discard speculative destination state; promote old primary range from surviving replica |
| `migration_destination_during_staging` | Kill destination during staging, **before publication** | **PASS** | Original source remains authoritative; failover of destination's own range handled |
| `migration_partial_postpublication` | Kill source after **partial migration publication** | **EXPECTED_FAIL_CLOSED** | Writes remain fenced; no false re-publication; **availability NOT recovered** |
| `worker_bounded_crash` | Kill real worker under bounded delay **tau=1** | **PASS** | Timed-out worker excluded; survivor resumes, history retained; no old-session server-Push fence |
| `delayed_push_request` | Delay client→primary Push at test RPC boundary | **PASS** | Both acknowledged updates ordered and replicated; finite delay tolerated |
| `dropped_push_request` | Drop first Push *before it reaches primary* | **PASS** | Same-Task-ID retry applies once after a one-shot loss |
| `dropped_push_ack` | Let Push commit on primary/backup, discard ACK | **PASS** | Replaying same Task ID and payload does not apply twice |
| `dropped_transfer_stage` | Drop manager→destination staging RPC | **PASS** | Cancel unpublished transfer safely, maintain old epoch, continue replicated Pushes |

Observed console output:

```text
[1/9] x6_primary_mid_iteration ... PASS
[2/9] migration_source_during_staging ... PASS
[3/9] migration_destination_during_staging ... PASS
[4/9] migration_partial_postpublication ... EXPECTED_FAIL_CLOSED
[5/9] worker_bounded_crash ... PASS
[6/9] delayed_push_request ... PASS
[7/9] dropped_push_request ... PASS
[8/9] dropped_push_ack ... PASS
[9/9] dropped_transfer_stage ... PASS

X7 matrix status: PASS
Verified: 9 / 9
Expected fail-closed: ['migration_partial_postpublication']
Results: results/f52_x7
```

**Interpretation:** X7's 9/9 means **all nine rows produced their expected, verified classifications**. It contains **eight `PASS` recovered/handled rows** and **one `EXPECTED_FAIL_CLOSED` row**. Calling it "nine recoveries" would be wrong.

### 8.3 Evidence outputs

```text
results/f52_x7/
    matrix.csv             # case, phase, result, classification, scope, evidence path
    events.csv             # scenario_begin / scenario_result
    summary.json           # PASS/INCOMPLETE/FAIL, seed, all case metrics and known limits
    ops.csv                # schema header only for focused cases
    iterations.csv         # schema header only for focused cases
    convergence.csv        # schema header only for focused cases
    case_logs/*.txt        # the invoked unittest command and captured stdout/stderr
    case_metrics/*.json    # test-emitted observed metrics where available
```

The operation/iteration/convergence CSVs for X7 contain **headers**, not the continuous workload samples: continuous Push evidence remains under **`results/f51_x6/`**. `matrix.csv` includes specific evidence location, scenario phase, tested guarantee and known limit. The user ran `test_f52_x7_matrix.py` and reported **11/11 PASS**; the new message-fault tests were additionally checked locally in the development compatibility fixture (**5/5 PASS**). The user-run full nine-case matrix is stronger confirmation than the local fixture but does not independently reproduce X6 in that same command.

---

## 9. Chronological implementation and test ledger

This section distinguishes repeated incremental test suites from the two final experiments; individual tests may reuse older fixtures, so counts should not be combined into a claim of independent coverage.

| Stage | Source/behaviour added | Result reported |
|---|---|---|
| F1.1 | Health registry, manual timeout/suspect | 8/8 |
| F1.2 | Server heartbeat sender and automatic monitor | 7/7 |
| F2.1 | Clockwise k-neighbour placement, snapshot seed | 11/11 |
| F2.2 | Ordered replication and fail-closed ACK | 12/12 |
| F2.3 | Opt-in timed batch replication | 15/15 |
| F2.4 | Real cross-process replica transport | 6/6 |
| F3.1 | Confirmed-death promotion + ownership epoch | 7/7 |
| F3.2 | Replica repair/reseed + release | 7/7 |
| F3.3a | Automatic controller + physical retry | 10/10 |
| F3.3b | Distributed-worker failover retries | 13/13 |
| F3.4 | Fenced migration crash handling | 12/12 on Windows, 19.525 s |
| F4.1 | Worker liveness registry, timeout gate exclusion | 16/16 on Windows; real kill |
| F4.2 | Coordinated worker automatic heartbeat | 8/8 on Windows; real kill |
| E3.2 regression | Existing dynamic join/leave/clock test | All named checks PASS after F4.2 |
| F5.1 | Offline oracle/evidence arithmetic and X6 driver | 8/8 evidence tests on Windows; X6 real crash PASS |
| F5.2 | Nine-case matrix, message-fault tests | 11/11 evidence tests on Windows; X7 9/9 classified PASS |

**Additional earlier regression evidence:** During the F2/F3 implementation, prior heartbeat, replication and client tests were re-run in the assistant's local compatibility environment. The user separately verified the focused F3.4, F4.1/F4.2, E3.2, F5.1 and F5.2 outputs pasted into the conversation. These are not the same as a final clean-clone execution of *every* A–F test on the finished checkout. That full audit is explicitly the next project phase.

---

## 10. Reproduction instructions (Windows PowerShell)

### 10.1 Environment and protocol stubs

Run in the repository root with the Python environment previously used for Parts A–E. Dependencies include `grpcio`, `grpcio-tools`, `protobuf`, plus the existing harness/metrics dependencies. Exact pinned versions and fresh-environment installation results were not supplied, so this section gives verified **commands**, not an independently reproduced setup time.

After taking the latest `manager.proto` containing `WorkerHeartbeat`, regenerate the manager stubs if necessary:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. manager.proto
```

Ensure generated server bindings correspond to the project's current server `.proto` and transport service; do not mix legacy/generated modules from a different checkpoint. There were **no new Protobuf changes in F4.2, F5.1 or F5.2**. A useful syntax smoke check:

```powershell
python -m py_compile server.py server_manager.py replica_transport.py `
    manager_rpc.py coordinated_client.py worker_heartbeat.py `
    failover_controller.py failover_retry.py f5_evidence.py `
    run_f51_x6.py f52_x7_matrix.py run_f52_x7.py
```

### 10.2 Focused F1–F4 tests

```powershell
python test_f11_heartbeats.py
python test_f12_automatic_failure_detection.py
python test_f21_initial_replicas.py
python test_f22_live_replication.py
python test_f23_batched_replication.py
python test_f24_process_replication.py
python test_f31_replica_promotion.py
python test_f32_replication_repair.py
python test_f33_automatic_recovery.py
python test_f33b_distributed_worker_recovery.py
python test_f34_migration_crash_recovery.py
python test_f41_worker_failure.py
python test_f42_coordinated_worker_heartbeats.py
python -m Part_E_Tests.test_e32_dynamic_workers
```

Some tests create and kill their own local subprocesses/gRPC servers; do not leave a conflicting manually launched cluster listening on their test ports. The E3.2 module command reflects the user's actually successful package path. Other `test_f*.py` scripts were originally generated in the **repository root**, not reported relocated to `Part_F_Tests/`. The final audit may choose a new layout, but should then fix imports and commands and rerun every test.

### 10.3 X6 real continuous-training crash

```powershell
python test_f51_evidence.py
python run_f51_x6.py --config f51_x6_config.json
```

Keep `results/f51_x6/summary.json`, `events.csv`, `ops.csv`, `iterations.csv` and `convergence.csv`. A fresh run may produce a different Push count/timing than the reported **718**, but should **pass the independent oracle** and output valid recovery timings. Inspect the two throughput figures:

```powershell
(Get-Content results/f51_x6/summary.json -Raw | ConvertFrom-Json).recovery_times.throughput |
    Select-Object theta_pre, threshold_90pct, throughput_recovery_s, postfault_only_recovery_s
```

Do not substitute `throughput_recovery_s` for `postfault_only_recovery_s` when explaining genuine post-fault throughput restoration.

### 10.4 X7 full fault matrix

Run **X6 first** because X7 validates its saved real evidence:

```powershell
python test_f52_x7_matrix.py
python run_f52_x7.py --config f52_x7_config.json
```

The expected status for the recorded successful configuration is:

```text
X7 matrix status: PASS
Verified: 9 / 9
Expected fail-closed: ['migration_partial_postpublication']
```

Optional per-case diagnosis (the runner marks partial selections `INCOMPLETE`, as intended):

```powershell
python run_f52_x7.py --config f52_x7_config.json --case dropped_push_ack
python -m unittest -v test_f52_x7_faults
```

Preserve `results/f52_x7/matrix.csv`, `summary.json`, `events.csv`, `case_logs/`, and `case_metrics/`. The X7 runner checks for stale/missing metrics on its new tests rather than reusing a previous green result. Preserve terminal logs and configs/seed alongside result files; **the current Git commit hash and dependency versions remain to be recorded**.

### 10.5 Placement of source and results

During incremental development the F milestone files and tests were delivered at the repository root; the user has not yet reported moving them. **Do not pre-emptively move or rename files based on this suggested target layout.** The next full-project audit will propose and verify a coherent A–F directory structure, and only then update imports/tests/docs:

```text
project-root/                       # current logical organisation, not audited exact tree
    server.py, server_manager.py, manager_rpc.py, manager.proto
    server_heartbeat.py, worker_heartbeat.py, replica_transport.py
    failover_controller.py, failover_retry.py
    parameter_client.py, distributed_client.py, coordinated_client.py
    f5_evidence.py, run_f51_x6.py, f51_x6_config.json
    f52_x7_matrix.py, run_f52_x7.py, f52_x7_config.json
    test_f*.py
    Part_F_README.md, agents.md, observations.md
    results/
        f51_x6/{ops.csv,iterations.csv,convergence.csv,events.csv,summary.json}
        f52_x7/{matrix.csv,ops.csv,iterations.csv,convergence.csv,
                events.csv,summary.json,case_logs/,case_metrics/}
```

---

## 11. Problems addressed, key decisions and why they matter

| Concern/problem | Chosen solution | Verified point / residual risk |
|---|---|---|
| Heartbeat suspicion mistaken for crash | Separate manager state `SUSPECTED` from process-death confirmation | F1 and F3; still requires trustworthy live supervisor |
| Primary ACK before backup | Commit/ACK after ordered backup update or batch acceptance | F2.2/F2.3; RAM only, not disk |
| Backup copy lacks deduplication/clocks | Replicate parameter after-images, vector-clock state, Task-ID fingerprints | F2 process and F3 promotion cases |
| Replica promoted with no backup | Protect writes, reseed/repair then explicitly release | F3.2; temporary reduced availability |
| Lost ACK after successful Push | Keep same Task ID and request fingerprint on retry | F3 and X7 `dropped_push_ack` PASS |
| Stale owner after failover | New ownership epoch + selective refresh/failover retry | F3.3b; cross-partition ambiguous transactions deliberately refused |
| Failure during unpublished E1 state transfer | Global write fence, rollback speculative state, promote original replica | F3.4 prepublication source/destination kill PASS |
| Failure after publication | Remember pending phase, retry finalization/repair in same epoch | Lost finalize/release callback tests PASS; partial retained-range crash fails closed |
| Slow healthy worker incorrectly excluded | Independent heartbeat while blocked at barrier | F4.1/F4.2 PASS |
| Actual dead worker blocks sequential/bounded training | Timeout-based active-set exclusion, historical clocks retained | F4 real worker kill PASS; old in-flight server Push not fenced |
| Misleading 0.100 s throughput metric | Report post-fault-only alongside sliding-window statistic | X6 3.000 s conservative threshold; both kept in evidence |
| False green failure-matrix result | Validate X6 event/oracle/ops, require real unittest result, classify fail-closed | F5.2 11/11 evidence unit tests and X7 9/9 matrix |

---

## 12. Known limitations and precise guarantee boundaries

1. **In-memory durability only.** An acknowledged Push is replicated to tested live backup processes but not persisted to disk. Simultaneous primary+backup loss may lose state. No leader election, replicated manager or consensus prevents all split-brain scenarios.
2. **Confirmed crash-stop, not arbitrary partitions.** Promotion is allowed only when an external callback confirms a dead process. A timed-out but still-running primary is not safely promotable on suspicion alone. The tests do not establish availability under indefinite asymmetric partitions.
3. **Acknowledgment versus end-to-end exactly-once.** Within the same worker/session/Task-ID and the supported partition scope, a replay suppresses duplicates. Worker process restarts with reused IDs, arbitrary sparse cross-boundary moves, two-phase multi-owner logical Pushes, and arbitrary ambiguous error/retry sequences are not globally solved.
4. **Replication timing.** Batches wait for their flush and backup ACK. A crash may discard *unacknowledged* work without violating the supported guarantee. The backup protocol is memory resident and the user must not infer disk durability.
5. **F3.4 pauses new Pushes globally in the active ring** for its replicated migration path. It does **not** preserve E1's usual during-copy liveness or guarantee uninterrupted throughput throughout transfer. Ordinary graceful E1 and crash-safe F3.4 are distinct paths.
6. **Postpublication partial source crash may remain fenced.** The X7 test intentionally returns `EXPECTED_FAIL_CLOSED`, not recovery. There is no complete reconstruction path in that failure window; without operator intervention training can remain blocked.
7. **Worker timeout false positives and ghost operations.** Worker exclusion can remove a temporarily unreachable but live worker, and a previously sent server-side Push may still commit after exclusion. No globally enforced worker-session epoch fencing is implemented at all parameter servers.
8. **Tested message fault model is narrow.** A one-shot controlled delayed request, dropped request, dropped ACK and staging RPC drop are not a statistical loss/jitter sweep and do not show unlimited retry under sustained loss.
9. **One coordinator and limited concurrent failures.** The manager is a single point of failure in these tests; simultaneous server failures and coordinator loss have not been recovered. Automatic restarting of killed servers is not supplied.
10. **Workload size and host scope.** The quantitative X6/X7 evidence comes from a small local three-process cluster and synthetic workers; no WAN, large tensor/state volume, multi-host experiments or storage-performance benchmarking was reported.
11. **Experimental harness alignment.** F5 uses dedicated reproducible drivers and unittest-based test harnesses, not one universal Part B declarative scenario engine/cluster adapter. This distinction should remain visible during the final rubric audit.
12. **Data scope of metrics.** X6 `total_bytes` reflects worker-facing serialized Protobuf payload estimates, not whole-network/wire/replica traffic. X7's header-only ops/iterations/convergence CSVs are not per-operation samples.
13. **Reproducibility/provenance.** The user's final Git SHA, full dependency list/lockfile and uploaded raw X6/X7 results files are still missing from this documentation context. Windows console output confirms successful local execution but is not an independently performed clean-clone re-run.
14. **Project-wide requirements are still under audit.** Earlier Parts A–E and X1–X5 may have unresolved rubric/evidence gaps, even though Part F's selected milestones passed. The user explicitly requested a **separate full-project inspection, code cleanup, restructuring and total regression audit** after the Part F docs are finalised.

These caveats are part of the technical result, not a reason to mark the complete failure matrix as untested. **The tested safety properties passed; some availability guarantees are intentionally absent.**

---

## 13. Final Part F verification checklist

- **F1:** manager hears periodic server heartbeats, reports configurable-timeout suspicion and keeps separate confirmed-death logic; local kill detection PASS.
- **F2:** at least one backup per owned range; per-Push and timed-batch modes; ACK-after-replica and ordered state/clocks/dedup; process crash fixture PASS.
- **F3:** confirmed primary failure triggers promotion, new ownership epoch, deduplicated retry, backup re-establishment and resumed Push; X6 decomposition recorded and oracle passes; migration crash stages explicitly classified.
- **F4:** real worker crashes release sequential and bounded-delay barriers, retain historical worker progress, reject old session and preserve backward-compatible E3 membership behaviour.
- **F5/X7:** actual nine-row configured matrix PASS with **one expected fail-closed** row; message faults are explicit and have tested limits.
- **X6:** 718 completed Pushes in the reported run, maximum oracle error ~`9.09e-13`, first recovered Push **2.100 s**, post-fault-only 90% throughput recovery **3.000 s**.
- **Documentation:** `Part_F_README.md`, `agents.md`, and **`observations.md`** should all be copied to the user's repository. No `findings.md` is requested.
- **Not yet a whole-project release claim:** exact final checkout, full A–F regression suite, refactor/restructure, X1/X2 gap closure and report/demo are reserved for the **next new chat**.

**Part F closeout status:** **Implementation/testing: DONE for recorded local fault model. Documentation: prepared in this set. Overall project: NOT YET AUDITED OR DECLARED COMPLETE.**

---

## 14. Related documents and evidence provenance

- `Distributed Systems - Coursework.pdf` — Part F pp. 9–10, harness/output requirements §5, X6/X7 §5.8 pp. 14–15, deliverables/rubric thereafter.
- `Part_A_README.md`, `Part_B_README.md`, `Part_C_README.md`, `Part_D_README.md` — previous protocol/client, oracle/harness, consistency and communication features.
- `Part_E_README.md` — detailed design/test evidence for graceful live migration, worker membership, and the E4/X5 scale-up experiment. Part F does not erase Part E's historic tests or design boundaries.
- `agents.md` — durable project guide, Part F contracts and the next **full A–F audit** handoff.
- `observations.md` — dated incremental F1–F5 milestones, observed PowerShell results, defect/limitation ledger and X6/X7 evidence.
- F-specific tests from `test_f11_heartbeats.py` through `test_f52_x7_matrix.py`, configurations `f51_x6_config.json`, `f52_x7_config.json`, and their original result directories.

**Summary:** In the recorded local crash-stop model, Part F detects primary failures, replicates acknowledged updates before returning success, promotes seeded replicas, repairs backup placement, resumes workers, and excludes crashed workers from progress barriers. The X6 fixed-gradient crash run verified 718 completed Pushes with ~`9.09e-13` numerical oracle error; first-Push recovery was **2.100 s** and genuine post-fault-only 90% throughput restoration **3.000 s**. X7 classified **all nine required/extended scenarios correctly**, including **one deliberate postpublication partial-transfer fail-closed outcome**. This establishes the selected coursework fault-tolerance demonstrations while retaining honest availability, durability and global atomicity limits. The requested next step is a complete **A–F project-wide audit and restructuring** in a new chat.
