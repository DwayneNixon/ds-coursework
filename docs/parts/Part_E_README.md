# Distributed Systems Coursework — Part E: Elasticity

University of Edinburgh · Distributed Systems (INFR11022) · Building a Parameter Server

This document covers **Part E (E1–E4)**. It follows the comprehensive, evidence-based structure of `Part_C_README.md` and `Part_D_README.md`: coursework requirements, design alternatives and decisions, architecture and interfaces, incremental implementation, tests and observed defects, repeated measurements, reproduction instructions, limitations, and handoff to Part F.

**Status (21 November 2026):** **E1 live server addition, E2 graceful server removal, E3 dynamic worker membership and E4 a 2→4-server experiment are implemented and PASSED in the user-run local test scenarios.** The reported full Part C and discovered Part D regression suites also passed after E3. The E4 evaluation produced individual-operation CSVs, event records, one-second throughput and latency series, JSON summaries, and marked throughput/latency plots. Three independent formal E4 trials each passed the fixed-gradient final-parameter oracle. This is **verified within the reported coursework-harness scope**, not a proof of arbitrary failures, multi-process orchestration or production correctness.

> **Continuity:** Parts A–D remain the foundation. Part A provides the float64 parameter store, asynchronous gRPC Push/Pull, Task IDs, deduplication and update rules. Part B provides worker emulation, numerical oracles and performance instrumentation. Part C provides manager-owned range partitions, ownership epochs, routing refresh, per-range vector clocks and consistency barriers. Part D provides sparse-key caching, optional lossless value compression and explicit sparse-Pull batching. **Part E extends these components** rather than replacing them. In particular, a stale-route retry is not a substitute for transferring existing weights and clocks.
>
> **Evidence provenance:** The measurements and PASS outputs below were supplied by the user from Windows PowerShell on **21 November 2026**. Selected source snapshots were available for static inspection before and during development, but **a complete, final post-E3/E4 local repository revision and Git SHA were not independently inspected or executed in this documentation session**. The latest local files and generated stubs are authoritative. Read the limitations before treating this as a complete release audit.

---

## 1. Purpose and official requirements

The coursework handout, `Distributed Systems - Coursework.pdf`, **Part E: Elasticity, p. 9**, defines four tasks:

| Item | Requirement | Demonstrated local implementation | Evidence |
|---|---|---|---|
| **E1 — Add a server** | Assign a newcomer a range; obtain previous-owner state while service continues; preserve Pushes arriving during transfer; old owner relinquishes; clients learn the new map | Snapshot plus dirty-key/clock catch-up, short fenced cutover, manager ownership epoch change, stale-route recovery | E1.1, E1.2 and concurrent E1.3 tests PASS |
| **E2 — Graceful removal** | Hand a departing server's ranges to others without data loss | `remove_server()` invokes the existing range migration for each owned partition; source can subsequently stop | Two-partition removal and post-shutdown training test PASS |
| **E3 — Dynamic workers** | Join/leave training with changing vector-clock and consistency bookkeeping | Historical progress plus active-membership set; membership epoch; Join/Leave manager RPCs and coordinated worker API | Eight gate tests and end-to-end gRPC membership test PASS |
| **E4 — Demonstration** | Double the number of servers during a workload, plot throughput/latency over time with event marked, verify final answer | Continuous four-worker, deterministic, local 2→4 scale-up; timestamped Push metrics, marked plots, final count-based SGD oracle | Two exploratory runs and three formal repeats PASS |

The related **X5** experiment in §5.8 (p. 14) asks whether elasticity is smooth: **add servers under load and plot throughput over time with the transition marked**. Our E4 runner is an X5-style local scenario; it was **not yet registered as a scenario through the pre-existing generic Part B injection engine**, and this difference must be stated when describing harness integration.

Part **F** (p. 9 onward) separately requires heartbeats, replicas, promotion after a crash, worker failure handling, and a failure matrix including a server crash during E1 transfer. Those guarantees are **not provided by the graceful E implementation**.

### 1.1 Exact scope selected for a student coursework project

The aim was demonstrable correctness and comprehensible implementation, not a production cluster controller. The measured arrangement runs **real gRPC workers and parameter-server endpoints** locally, while the **manager calls live `ParameterServer` Python objects directly** to coordinate state migration in the same process. This is suitable for the supplied in-process integration tests but **not remote cross-process state-transfer orchestration**. No persistent transfer log, replicated consensus, distributed transactions or automatic process restart was added.

---

## 2. Architecture after Part E

```text
              Worker A      Worker B      Worker C (joins/leaves)
                  |              |                |
             CoordinatedClient (optional C4 / E3)
               - JoinWorker / LeaveWorker
               - CanStart / CompleteIteration
               - active membership + historical progress
                  |              |                |
                  +---- DistributedParameterClient ----+
                         - ownership-epoch routing
                         - globally shared Task-ID allocator PER worker instance
                         - split/merge, NOT_OWNER auto-refresh
                         - D1 key-cache signature scoped by epoch
                         - D2 compression flags propagated on new connections
                                  |
                              gRPC/Protobuf
                                  |
                    ParameterServer S1 ... S4
                    - weights and committed Push IDs
                    - per-partition RangeClocks
                    - outgoing snapshot + changed-key catch-up
                    - incoming staged state + final import
                    - short freeze of moving keys at cutover
                                  ^
                                  |
                        ServerManager / ownership map
                        - epoch and contiguous uint64 ranges
                        - move_range / remove_server
                        - consistency gate + membership epoch
                        - direct server-object calls for migration

              E4 workload: four live worker threads
              2 owners -> add third -> add fourth
              metrics: successful Push timestamps + latency
              oracle: final weights from successful Push counts
```

**Two different epochs:** the **ownership epoch** versions the key-to-server routing table, while the **membership epoch** versions the set of training workers. A worker joining does not redistribute keys; a server joining does not reset a worker's historical vector-clock entries.

### 2.1 Responsibility and file map

| File / component | New Part E responsibility | Reused behavior |
|---|---|---|
| `parameter_client.py` | Optional shared Task-ID allocator for physical per-owner clients | Standalone client numbering, original Push byte replay, D1/D2 |
| `distributed_client.py` | One thread-safe allocator shared across connections belonging to one distributed worker; propagate it to newly discovered owners | C2 routing and selective retry; D1/D2/D3 |
| `range_clocks.py` | `extract(begin,end)` and `from_segments(begin,end,segments)` | Segmented clocks, `record`, merge, clone |
| `server.py` | Source snapshot, changed-key tracking, freeze/cancel; incoming staging/delta/install; outgoing clock split and relinquishment | Locked atomic Push, Pull, dedup, custom update rules, compression, key caches |
| `server_manager.py` | Serialize migrations; `move_range()`; `remove_server()` | Authoritative partition table and epoch management |
| `manager.proto`, generated `manager_pb2*.py` | `JoinWorker`, `LeaveWorker`; optional worker identity on `CanStart` | `GetMap`, `CanStart`, `CompleteIteration` wire contracts |
| `manager_rpc.py` | Membership RPC handlers and worker-aware admission | Ownership-map gRPC service |
| `consistency.py` | Active workers, historical progress, membership epoch, registration and departure | Sequential, eventual, bounded-delay consistency |
| `coordinated_client.py` | `join_training()`, `leave_training()`, variable start iteration, explicit no-next-turn finish | Push clock evidence and completion reporting |
| `test_e*.py` | E1–E4 user-run integration, concurrency and oracle tests | Existing Part C server factory / gRPC support |
| `e4_reporting.py` | Exports `ops.csv`, `events.csv`, `timeseries.csv`, `summary.json`, two PNGs | Python CSV/JSON; Matplotlib plotting |

`parameter_server.proto` did **not** need new migration RPCs in the tested design. Migration state is moved by the local manager between server objects. The manager's E3 worker registration **did** extend `manager.proto`, so the corresponding stubs must be regenerated.

### 2.2 Core invariants aimed for

1. While the initial snapshot is captured, a Push must not partly commit outside the existing server lock.
2. Every committed **in-range** update after the snapshot must be represented in the final catch-up, with its resulting weights and clock state.
3. Once the moving range is frozen, its state must stop changing on the source before the final delta is sent.
4. The destination must be populated and ready before the manager advertises it as the new owner.
5. The original owner must stop accepting **new** Push/Pull operations on moved keys after cutover; stale clients recover through `NOT_OWNER`.
6. The original owner's **retained** left/right ranges must preserve their clocks, not restart at zero.
7. A worker leaving must not block future barriers; its historical clock counters must remain meaningful.
8. A correct final parameter vector is checked from successfully acknowledged updates, rather than inferred from timing alone.

These are the intended semantics for the tested graceful, no-crash execution paths. **No single global atomic transaction spans every server and the manager**; failure between cutover steps remains a limitation (§13).

---

## 3. E1 design choices: alternatives considered

### 3.1 Range assignment

| Design | Benefit | Drawback in our codebase | Decision |
|---|---|---|---|
| **Manager-held contiguous range splits** | Reuse Part C routing, small change set, obvious transferred interval | Requires explicit partition-clock split and stable ownership epochs | **Chosen** |
| Consistent-hash ring / virtual nodes | Common approach to balanced repartitioning | Would replace working C1 table/split logic and complicate this coursework milestone | Not chosen |
| Recompute `key mod server_count` | Very simple initial assignment | Remaps many keys, makes most joins/removals expensive | Rejected |
| Move an empty/unallocated range only | Easiest special case | Does not demonstrate preserving nonzero state under load | Insufficient for E1 |

A transfer begins with one owned partition and an arbitrary **nonempty subinterval** of its half-open range. The manager can split that source partition into **retained left, moved middle and retained right**. The `2^64` keyspace-end sentinel remains a Python integer internally; the original Part C Protobuf end marker remains unchanged.

### 3.2 Update preservation during migration

| Algorithm | Advantage | Cost / correctness risk | Decision |
|---|---|---|---|
| Stop all requests, copy, then resume | Very simple | Does not meet the requirement that service continue during bulk copying | Rejected for E1 |
| **Two-phase snapshot + changed-state catch-up** | Source continues serving during copy; reuses server lock; short final fence | Requires accurate dirty tracking, complete clock snapshots, correct cutover order | **Chosen** |
| Forward every new Push to the destination | Avoids a final copy of changed values | Complicates ordering, retries, duplicate handling and transition ownership | Not selected |
| Dual-write to both servers during transfer | Potentially continuous updates on both | Hard to avoid partial success and duplicated/nonlinear rule execution | Rejected for this stage |
| Durable external replicated log | Handles additional crash cases | Outside Part E's student-level graceful scope; Part F would need a broader protocol | Deferred |

**Implementation detail:** the catch-up copies **the latest committed parameter after-images**, not merely the original gradients. This matters because Part A supports configurable update rules: replaying an old gradient on a destination with different rule state is not equivalent to transferring the source's committed result. The **final delta also includes a complete clipped clock snapshot**, not just the last changed counter.

### 3.3 Cutover and coordinator placement

We chose a **brief freeze of the moving interval** and manager-controlled publication. Operations on **unaffected keys** can continue. The manager coordinates **live service objects**, so all metadata moves as ordinary Python dictionaries/clock segments. A remote gRPC migration-control protocol, durable checkpoints, remote authentication, and independent server processes were not added.

This is not production-grade failover. It is a transparent, testable choice that demonstrates the handout's two-phase transfer idea and creates reusable logic for E2.

---

## 4. E1.1 — Transfer foundations and shared Task IDs

### 4.1 Why the old per-server sequences became unsafe

In Part C, one logical distributed worker held several per-owner `ParameterClient` instances. Each physical instance could allocate `(worker_id, 1)` independently. That was fine for separated, static `completed_pushes` dictionaries; it became a **collision risk when completed-Push metadata had to be imported to another server**.

**Chosen E1.1 change:** supply each physical client with an optional allocator callback. `DistributedParameterClient` protects one Push counter and one Pull counter with `threading.Lock`, and gives all its physical clients **the same callback**. Every newly discovered connection gets this allocator too. Standalone `ParameterClient` instances preserve their previous default behavior. Push sequences occupy the lower region of uint64; Pull sequences retain their separate high-bit region.

For example, the same worker may now send `(1001,1)` to S1, `(1001,2)` to S2, then `(1001,3)` to S1, with no collision **within that live distributed worker instance**.

**Important limitation:** This does not allocate a cluster-wide ID across two independent processes reusing the same `worker_id`, nor does it persist counters across worker restarts. A rerouted logical operation can also change the serialized physical request fingerprint; the allocator alone cannot solve ambiguous RPC outcomes.

### 4.2 Clipped vector-clock state

Part C's `RangeClocks` already records and coalesces segments. E1 adds:

- `extract(begin,end)`: copy exactly the overlapping clock intervals and clip them to the requested range; the source is unchanged.
- `from_segments(begin,end,segments)`: validate contiguous coverage, no gaps/overlaps, valid uint32 worker IDs and positive uint64 counters, then restore independent dictionaries.

This supports both a mid-partition range and an interval ending at `2^64`. We deliberately **did not** introduce a second vector-clock type or convert dense-range clocks into one clock per key.

### 4.3 E1.1 test evidence

`python test_e1_foundations.py` reported **8/8 unittest cases PASS** in 0.016 s, covering shared/concurrent sequences, standalone behavior, newcomer connection configuration, clipped clocks, copied-state isolation, malformed segments and the maximum-keyspace boundary.

The initial broad Part C regression uncovered historical tests still expecting the pre-E1 per-owner Task-ID numbering or manual `NOT_OWNER` handling:

- `Part_C_Tests/test_c2_stale_recovery.py` expected a logical `NOT_OWNER` failure, although C2's current `DistributedTask` correctly performs automatic selective recovery. The corrected test verifies the **physical rejection** and the **successful logical retry**, plus final weights. It passed all eight printed checks.
- `Part_C_Tests/test_c3_distributed_clocks.py` hard-coded per-owner physical Push sequences (e.g. `3` and `2`) and the legacy clock counter `4`. It was updated to check **unique physical IDs across owners** and to read the actual legacy Task-ID sequence. It passed all eight printed checks.
- An `IndentationError` in `distributed_client.py` after editing `refresh_map()` was fixed by restoring the method's block structure. All eight E1.1 tests subsequently passed.

**No correctness assertion was simply deleted:** the updated tests continue checking expected vector clocks, physical `NOT_OWNER`, no duplicate updates, and routed data correctness. The complete Part C suite later passed on the user's machine.

---

## 5. E1.2 — Source snapshot, change tracking and destination import

### 5.1 Source-side state

`ParameterServer` extends the existing in-memory state with:

```text
completed_pushes          Task ID -> serialized request fingerprint (existing)
completed_push_bounds     Task ID -> bounding interval (new)
_transfer                 at most one active outgoing transfer per source
_incoming_transfer        at most one staged incoming transfer per destination
```

The range-transfer snapshot includes:

- `begin`, `end` and the owning source partition identity;
- copies of the stored **nonzero/explicit** parameter vectors in the moving range (other keys still use default zero semantics);
- the extracted, segmented vector clocks for the **entire interval**;
- applicable successful Push fingerprints/affected-key bounds for requests known to lie entirely within the moving interval.

`begin_range_transfer(begin,end)` takes this snapshot **while holding the parameter-server lock** and turns on changed-state tracking. The original owner continues to answer normal Push/Pull calls while the destination stages the snapshot.

### 5.2 Recording updates between the two phases

Successful `_execute_push()` calls still calculate and validate a complete update before committing. Within the original server lock, the implementation now also:

1. Commits the prepared weights and prepared clocks.
2. Records the completed Task ID/fingerprint and the touched-key bounding interval.
3. Adds changed keys that intersect the moving range to `_transfer["dirty_keys"]`.
4. Tracks applicable successful Push IDs for the catch-up.
5. Notifies dependent tasks.

**Atomicity point:** dirty-key tracking happens in the **same critical section** as the committed parameter update, so a successful Push cannot be committed and then missed by a snapshot's change set merely because another thread interrupted between those operations.

For simplicity, `completed_push_bounds` is a **min/max bounding interval** for sparse requests, not the exact set of touched keys. A sparse request spanning both moved and retained regions is not safely covered by the whole-request deduplication import. See §13.

### 5.3 Freeze, catch-up and cancellation

`freeze_range_transfer()` marks the moving interval as temporarily frozen, captures the changed **latest weights**, full interval clock segments and matching completed-Push records. `_wait_for_transfer()` blocks requests overlapping the frozen interval using the existing condition variable, releasing the lock while waiting. Keys outside the interval continue to serve.

`cancel_range_transfer()` clears the transfer state and wakes waiting requests. This gives the source-only test a safe escape path. It is **not** a complete cross-node rollback protocol once incoming state is installed or a manager map is published.

### 5.4 Destination staging

The destination uses three stages:

```text
stage_incoming_transfer(partition_id, snapshot)
    -> validate/copy metadata, weights, clocks, dedup records
    -> keep it in _incoming_transfer (not routable yet)

apply_incoming_delta(delta)
    -> update staged changed-key after-images
    -> replace staged clocks with latest complete interval clocks
    -> add new completed-Push fingerprints

install_incoming_transfer()
    -> require catch-up completed
    -> check partition/key/Task-ID collisions
    -> install weights, clocks and completed-Push state under lock
```

The destination rejects premature installation and invalid or conflicting transferred state. `cancel_incoming_transfer()` can discard staging **before installation**. The normal ownership map still rejects Pulls to a destination that has received data but has not yet been assigned the keys.

### 5.5 E1.2 tests and observed outputs

| Test | Verified behavior | User-reported result |
|---|---|---|
| `test_e12_source_transfer.py` | Initial state, snapshot independence, Push/Pull during bulk copy, dirty values, clock updates, Push history, selective fence, outside-range availability and cancel/resume | **PASS**; 11 named PASS lines including final stage marker |
| `test_e12_destination_import.py` | Source and destination state independence; premature install rejection; snapshot + delta; final weights, clock and matching task metadata; destination rejects ownership before cutover | **PASS**; 14 named PASS lines including final marker |
| `test_e12_live_cutover.py` | Manager split/epoch; migrated and retained values; clocks on both retained sides; dedup history; `NOT_OWNER` refresh; continued training and required-clock Pull | **PASS**; 15 named PASS lines including final marker |

All are **user-run local gRPC integration tests**; no live server process crash was injected.

---

## 6. E1.2 cutover — ownership change and exact tested sequence

The manager's `move_range(source, destination, destination_address, begin, end)` validates a moving interval inside one source-owned partition, builds a new, fully covering `RoutingTable` at **ownership epoch +1**, and splits the original partition into up to three disjoint pieces.

```text
1. Current authoritative map: source owns [L, R)
2. Candidate map: source [L, a) | destination [a, b) | source [b, R)
3. Source: begin_range_transfer(a, b)              # snapshot
4. Destination: stage_incoming_transfer(...)      # not owner yet
5. Source continues Push/Pull; records changed moving keys
6. Source: freeze_range_transfer()                 # fence [a,b)
7. Destination: apply_incoming_delta(...)
8. Destination: install_incoming_transfer()
9. Destination: install new ownership table
10. Manager: publish the epoch-incremented map
11. Source: finish_outgoing_transfer(new_table)
    - extract retained [L,a) and [b,R) clocks
    - relinquish moved weights and old partition clock
    - invalidate D1 key cache, install new map
    - release blocked calls; stale ones receive NOT_OWNER
12. Distributed clients refresh the map and retry confirmed rejects
```

**Clock split matters:** A key on the old server outside the transferred interval must not lose its clock merely because its partition ID changed. `finish_outgoing_transfer()` calls `RangeClocks.extract()` for each retained side before removing the old partition-clock entry. The destination uses its imported clipped clock for the moving side.

**Old-client behavior:** clients may still hold the old map during migration. After the source relinquishes ownership, it rejects a newly submitted request to the moved range with `FAILED_PRECONDITION / NOT_OWNER`. The existing C2 logical client refreshes the manager map and retries only those confirmed rejected subrequests. D1 key-list cache IDs are ownership-epoch scoped and server caches are invalidated when the map changes. The D2 compression configuration is preserved when creating new physical clients (covered in E1.1).

**Sequential migrations from different sources:** in E4, another source server can hold an older table if it was uninvolved in the prior transfer. A pre-transfer source/destination ownership-map synchronization was added in `move_range()` before the two migrations to different original owners were exercised. This is an **in-process coordination shortcut**, not a general distributed map-installation protocol.

### 6.1 Concurrent cutover test (E1.3)

`test_e13_concurrent_migration.py` uses `threading.Event` hooks to make two windows deterministic:

- **After snapshot staging:** issue a real gRPC Push to the moving key and another to a retained key; both succeed while the source serves.
- **After freeze, before cutover:** issue another moving-key Push; it remains pending, while the manager completes state import and map publication.
- **After cutover:** the blocked Push receives the new ownership decision, routes to the destination and completes. Final weights and clocks are checked against each accepted update.

The user reported all **nine** printed checks PASS, ending with `E1.3 concurrent live migration: PASS`, including **no missing/duplicated test updates** and **required-clock Pulls after cutover**. This is stronger than an idle migration test, but it is **not** a broad randomized stress test, remote network test or crash-tolerance proof.

---

## 7. E2 — Graceful server removal

### 7.1 Designs considered

| Option | Advantages | Drawbacks | Decision |
|---|---|---|---|
| **Reuse `move_range()` for each owned partition** | Tested two-phase transfer, simple code, same clock/dedup logic | Intermediate ownership epochs; partial progress if a later transfer fails | **Chosen** |
| Copy entire departing server state as one blob | Simple for a single destination | May include unrelated metadata and different partitions; hard to fence all access without long stop | Not chosen |
| Immediately remove server and let clients reroute | Fast control-plane change | Loses nonzero state and clocks | Rejected |
| Replicate and promote a standby | Could support crashes as well as graceful leave | Requires Part F replica protocol | Deferred |

### 7.2 Implementation and test

`ServerManager.remove_server(source,destination,destination_address)` identifies all partitions still assigned to the source and calls **the same `move_range()`** for each. It verifies the source owns no ranges afterward; the harness then stops the departing server. Each transferred partition advances the ownership epoch once.

`test_e2_graceful_removal.py` performed this sequence:

1. Start with one owner containing nonzero keys `10, 30, 50, 70, 90`.
2. Move `[20,40)` and `[60,80)` to S2; S2 now owns **two disjoint partitions**.
3. Apply more worker Pushes on both S2 ranges and an unaffected S1 range.
4. Gracefully move both S2 partitions back to S1, advancing epoch **3 → 5**.
5. Verify all five parameter values, the five per-key clocks and matching completed-Push history.
6. Confirm the old owner rejects moved-key requests and a stale distributed client refreshes its map.
7. **Stop S2's gRPC server** and continue training on S1; verify a required-clock Pull and unchanged final oracle.

The user reported all **15** named lines PASS, including `E2 graceful server removal: PASS`.

**Scope:** one departing server, one chosen recipient, two moved partitions, cooperative/graceful shutdown, and no injected RPC or coordinator failure. If the second partition transfer fails, the first remains moved; the method is **not an all-or-nothing removal transaction**. Automatic destination selection, draining arbitrary outstanding requests, and immediate death mid-transfer are not demonstrated.

---

## 8. E3 — Workers joining and leaving mid-training

### 8.1 Alternatives and membership model

| Approach | Benefit | Main trade-off | Decision |
|---|---|---|---|
| Freeze the full worker set at startup | Easy barrier logic | Fails E3 | Rejected |
| Delete a departed worker's entire clock history | Small active table | Can break `required_clock` and misrepresent past Pushes | Rejected |
| **Separate historical progress from active membership** | Preserves vector-clock evidence; barriers only wait for active workers | Requires explicit registration/departure, start-iteration baseline | **Chosen** |
| Require all workers to restart at iteration 1 after a join | Simple identity semantics | Disrupts running training, defeats elasticity | Rejected |
| Heartbeat timeout and automatic exclusion | Tolerates a crash | Failure detection policy and correctness belong to F4 | Deferred |

`ConsistencyGate.completed` remains a mapping **worker ID → highest completed iteration**, including departed workers. New `active_workers` controls barrier admission. `membership_epoch` increments on each join/leave, independent of the server ownership-map epoch.

### 8.2 Joining a worker

`register_worker(worker_id)` rejects invalid/reused IDs, obtains the current minimum active completed iteration `b`, stores the newcomer at progress `b`, activates it, and returns `b+1` as the **first iteration it should execute**.

**Interpretation:** the initial progress `b` is a **coordination baseline**, not proof that the newcomer performed or produced updates for iterations `1..b`. The server-side vector clocks for that worker start when it actually Pushes. Reusing an old worker ID is rejected because persisted historical range clocks may reference its old counter.

A worker-aware `CanStart` verifies active membership and the next expected worker iteration. The field is **optional** in the Protobuf request to preserve existing Part C callers that supply only `iteration`.

### 8.3 Leaving gracefully

`leave_worker(worker_id)` removes the worker from `active_workers` but **retains its completed count**. The gate refuses to remove the last active worker in this simple model. Subsequent barriers exclude the leaver; the server's historical `RangeClocks` is not erased.

`CoordinatedClient.join_training()` calls the new manager RPC and sets its local `iteration` to the returned value. `leave_training()` requires **no pending unreported Push**, calls `LeaveWorker`, checks the acknowledged progress and prevents more Pushes from that client. The previously existing `iteration_finished(iteration)` now accepts `wait_for_next=True` by default; a departing worker supplies `False` so it can leave without blocking on the next barrier.

The manager cannot independently prove that a client has no in-flight RPC it failed to report; this is a **cooperative graceful-departure contract**.

### 8.4 gRPC protocol additions

| Interface | New field / method | Purpose |
|---|---|---|
| `CanStartRequest` | `optional uint32 worker_id = 2` | Validate a specific active worker while allowing old clients to omit it |
| `OwnershipManager` | `JoinWorker(JoinWorkerRequest) -> JoinWorkerReply` | Return starting iteration and membership epoch |
| `OwnershipManager` | `LeaveWorker(LeaveWorkerRequest) -> LeaveWorkerReply` | Return completed iteration and membership epoch |
| `manager_rpc.py` | `JoinWorker`, `LeaveWorker` handlers | Delegate to `ConsistencyGate`; reject invalid operations |

Original `GetMap` and `CompleteIteration` RPCs remain. Regenerate Python files with:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. manager.proto
```

### 8.5 Evidence and semantics

`python test_e31_dynamic_gate.py` reported **8/8 unittest tests PASS**, including original sequential behavior, joining behind a faster worker, leaving to unblock a barrier, historical-clock retention, rejection of rejoin/reuse, last-worker guard, bounded delay `tau=1`, eventual consistency and invalid inputs.

`python test_e32_dynamic_workers.py` reported **17 named PASS lines**. Two workers completed iteration 1; a third joined at iteration 2, read existing values, and participated in sequential barriers. One original worker left at its completed iteration 1; historical progress remained, remaining workers proceeded to iteration 3, and the manager rejected reuse of the departed ID. The final single-key vector was **`[-0.6,-0.6]`** from **six** successful gradient-1.0 Pushes at learning rate 0.1. Its server clock was **`{1101:3, 1102:1, 1103:3}`**, and a required-clock Pull referencing the departed worker passed. The **legacy worker-ID-less `CanStart` RPC** also passed.

This is demonstrated for the specific sequential integration scenario, with separate gate tests of bounded/eventual join behavior. It does **not** prove that a dynamic worker harness has been integrated into the generic Part B scenario scheduler for all workload shapes.

---

## 9. E4 — Evaluation design and measurement methodology

### 9.1 Scenario and workload

`test_e41_four_server_scaling.py` is a **continuous, timed local scaling driver**. It launches two initial gRPC parameter servers and a manager, creates **four `DistributedParameterClient` workers**, then starts a worker thread for each. Every worker repeatedly **Pulls one key → simulates 10 ms computation → Pushes `[1.0,1.0]`** with an explicit incrementing iteration. Each successful Push is timestamped with its measured Push latency and increments a per-key success counter.

| Factor | Recorded setting |
|---|---|
| Initial/final servers | **2 → 4**; new local gRPC servers started mid-run |
| Workers | **4** concurrent threads / distinct worker IDs |
| Key space | `uint64` partitions; four workload keys in distinct quarter-range regions |
| Keys | `10`, `2^62+10`, `2×2^62+10`, `3×2^62+10` |
| Vector width | **2** float64 values per key |
| Update | SGD, `learning_rate=0.1`, constant gradient `[1.0,1.0]` |
| Worker cycle | One-key Pull, **0.01 s** simulated compute, one-key Push |
| Initial workload | **6 seconds** before scaling begins |
| Scale event | Move one quarter range from each of the two original source servers, adding S3 and S4 sequentially |
| Final workload | **6 seconds** after scaling finishes |
| Summary comparison | Matched **five-second windows** immediately before scaling starts and after scaling finishes |
| Measured Push latency | `perf_counter_ns()` just before calling `push()` until task `.wait()` returns, successful RPC only |
| Throughput | Number of successfully completed logical Pushes in the window divided by window seconds |
| Correctness | Final weight of each key equals `−0.1 × its recorded successful Push count`, both directly on owner and through distributed Pull |
| Execution | Windows PowerShell, local in-process migration manager, real gRPC worker requests |

The last point is important: the measured throughput reflects **this specific four-worker, 10 ms simulated-compute workload**. Because the worker count is fixed, the simulated computation limits the number of possible operations per second, so adding servers is **not expected to double throughput**. This is an X5-style temporal elasticity demonstration, **not** the separate X2 sweep of 1–8 server counts under a fixed scalable workload.

### 9.2 Numerical oracle

For each successful Push to key `k`, the SGD update on each vector coordinate is:

```text
w_new[k] = w_old[k] - 0.1 × 1.0
expected_final[k] = -0.1 × acknowledged_push_count[k]
```

The test counts only completed, accepted logical Push operations. It verifies all final values on the expected physical owners and repeats the check using the **public distributed Pull API**. All five observed E4 runs (two exploratory and three formal) passed this oracle; the formally reported evidence is based on the **three consecutive repeated trials** below.

This is a direct parameter-accounting check, **not** a recorded target-distance/convergence curve of a real ML optimiser. A failed ambiguous RPC after a server crash was not injected.

### 9.3 What the timestamps include and exclude

The latency samples are **successful worker-facing logical Push wall-clock durations**, including routing, RPC completion and any handled stale-map retry, but excluding the preceding Pull and the 10 ms simulated computation. The reported scaling duration uses two `perf_counter_ns` marks around the in-process migration calls, **not** complete wall-clock server provisioning or a remote network service deployment. Results exclude unsuccessful attempts from the latency sample series; no gRPC wire-level packet accounting or CPU profiling was taken.

The experiment does not fully reuse `harness/metrics.py` or Part B's generic scenario executor: E4 provides a purpose-built local driver and exporter. Preserve this distinction in the final coursework presentation.

---

## 10. E4 — Two exploratory runs and three repeated formal trials

### 10.1 Exploratory runs (not pooled into the three-trial headline)

| Run | Pre-scaling successful Pushes | Total successful Pushes | Before throughput | After throughput | Before median Push | After median Push | Transition |
|---|---:|---:|---:|---:|---:|---:|---:|
| Early E4.1 | 1,947 | 4,038 | 328.60/s | 346.00/s | 0.887 ms | 0.545 ms | 0.011 s |
| E4.2 exporter validation | 1,955 | 4,019 | 332.00/s | 343.40/s | 0.747 ms | 0.585 ms | 0.010 s |

Both reported **server-count 2→4, final parameter oracle and distributed Pull PASS**. The second run additionally printed the results directory `results/e4_elasticity/trial_20261121_145950_016845` and successful graph export. These were useful integration/evaluation checks but were **not selected as the formal repeated-trial set**.

### 10.2 Three consecutive formal repeat trials (21 November 2026)

All three runs were fresh script invocations, each with two local initial servers and two newcomers under the same code path. Each printed `Final weights match the Push-count oracle: PASS` and `E4.1 four-server live scaling: PASS`.

| Trial | Initial successful Pushes | **Total successful Pushes** | 2-server throughput (Push/s) | 4-server throughput (Push/s) | Change | Median before (ms) | Median after (ms) | Change in median | Transition |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **1** | 1,908 | **3,849** | 320.80 | 323.60 | **+0.87%** | 0.942 | 0.957 | **+1.59%** (worse) | 0.022 s |
| **2** | 1,877 | **3,906** | 313.80 | 340.40 | **+8.48%** | 1.129 | 0.590 | **−47.74%** | 0.019 s |
| **3** | 1,949 | **3,926** | 325.20 | 326.80 | **+0.49%** | 0.920 | 0.909 | **−1.20%** | 0.007 s |
| **Mean of trial metrics** | — | — | **319.93** | **330.27** | **+3.23%** | **0.997** | **0.819** | **−17.89%** | **0.016 s** |

**Formal total: `3,849 + 3,906 + 3,926 = 11,681` acknowledged successful Pushes**, with the final parameter oracle PASS in **each** trial. Average transition duration: **16 ms**.

The headline percentage changes are calculated as the ratio of the **mean after and mean before values**:

```text
mean throughput before = 319.9333 Push/s
mean throughput after  = 330.2667 Push/s
change = 100 × (330.2667 / 319.9333 − 1) = +3.23%

mean of trial median latencies before = 0.9970 ms
mean of trial median latencies after  = 0.8187 ms
reduction = 100 × (1 − 0.8187 / 0.9970) = 17.89%
```

These are **means of the already-computed trial medians**, **not** pooled medians over all operations. Likewise, **+3.23% is not the mean of the three individual percentage improvements**. Do not conflate these aggregation methods.

### 10.3 Trial-specific evidence directories

The user reported these exporter paths, relative to the project root:

```text
results/e4_elasticity/trial_20261121_145950_016845/  # exploratory exporter check
results/e4_elasticity/trial_20261121_150026_396232/  # formal trial 1
results/e4_elasticity/trial_20261121_150039_600897/  # formal trial 2
results/e4_elasticity/trial_20261121_150052_626785/  # formal trial 3
```

These names are transcribed from local PowerShell output. The original CSV and JSON files **were not uploaded for independent numerical re-aggregation**. The user subsequently provided throughput and latency PNGs from the trial sequence; both images were visually inspected during the conversation. The report should include graphs generated from its own preserved files, not screenshots substituted for the underlying numeric evidence.

### 10.4 Interpretation: correctness is stronger than the speedup claim

- **Correctness:** all formal trials reached the correct final values despite new-server joins and ongoing worker requests, based on the acknowledged-Push-count oracle and distributed final Pull.
- **Throughput:** all three runs had higher post-scaling throughput, but improvements were **+0.49%, +0.87% and +8.48%**. The mean-of-metrics result is **+3.23%**: modest and variable, not near-linear scaling.
- **Median latency:** two runs improved but **Trial 1 worsened slightly** (`0.942 → 0.957 ms`). Claiming that elasticity always lowers latency would contradict the evidence. Trial 2's much larger reduction drives much of the three-run average.
- **Transition duration:** **7–22 ms** for local, in-process range transfer, not a general remote migration time. No large-volume transfer or WAN tests were included.
- **Causality and uncertainty:** only three consecutive local repetitions, no independent CPU load controls, confidence intervals, multi-machine measurement or warm-up study. The observed changes could include OS scheduling and cache effects. A modest post-scaling gain **coincided** with the join; it should not be presented as a causal capacity guarantee.

**Conservative conclusion:** The tested elasticity implementation **maintained parameter correctness and continued work during 2→4-server reconfiguration**. Under the fixed four-worker synthetic workload, mean throughput increased modestly and mean trial-median Push latency decreased, with considerable trial variation.

---

## 11. E4 reporting outputs and graph review

`e4_reporting.py` is imported by `test_e41_four_server_scaling.py` and runs **only after the final oracle has passed**. Each successful invocation creates a fresh, timestamped directory under `results/e4_elasticity/`:

| Artifact | Contents | Interpretation |
|---|---|---|
| `ops.csv` | Each accepted Push's completion time relative to scaling start, its measured latency, and phase | Successful logical Push samples; not all network attempts |
| `events.csv` | `scaling_started` at time 0 and `scaling_finished` at actual measured duration; 2→4 server count | Transition boundaries; does not separately time S3 and S4 join |
| `timeseries.csv` | One-second interval counts; derived P50 and P95 latency | Time-resolved aggregate view |
| `summary.json` | Before/after 5 s comparison, P50/P95, key counts, elapsed transfer time, stated limitations | Experiment-level numeric summary |
| `throughput.png` | Successful Pushes/s over time | Dashed marker at scaling start |
| `latency.png` | One-second median and P95 Push latency over time | Dashed scaling marker, transition shading |

The exporter uses **the scaling-start timestamp as `t = 0`** and marks the interval through scaling finish. At the one-second graph resolution, a **7–22 ms** shaded transition is nearly invisible. The dashed vertical event line remains readable. This is a display-resolution limitation, not evidence that no transition occurred.

**Visual review:** the user uploaded a throughput graph (initially twice) and then the actual latency graph. The figures showed appropriate axes, labels, one-second series and a clearly placed event line. In the inspected latency example, the post-event P50/P95 decreased over the displayed window; it is **one illustrative trial**, not a substitute for reporting the variable outcomes of the other two trials. No final post-edit remake of those image files was required.

**Presentation suggestion:** include the formal Trial 2 throughput/latency PNGs as illustrative time-series figures **and** the all-three-trials summary table in the report. If desired, annotate the marker with the measured **0.019 s** local transition duration. Keep the other trials' figures in the results folder for reproducibility. Do not claim the graph itself demonstrates the exact 19 ms transition width at one-second binning.

---

## 12. Testing and reproduction

### 12.1 Environment and build

Commands assume **Windows PowerShell in the repository root** with the existing Python environment, `grpcio`, `grpcio-tools`, Protocol Buffers bindings and Matplotlib. The user's exact package versions, Git commit and clean-environment installation log were not captured. Existing Part C tests use `Part_C_Tests.test_c2_server_ownership.make_server` to start isolated local test servers.

After changing `manager.proto` for E3:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. manager.proto
python -m py_compile consistency.py coordinated_client.py manager_rpc.py server.py server_manager.py
```

The student already ran the tests; the following commands are **reproduction instructions**, not an assertion that this documentation environment executed them.

### 12.2 E1 focused and integration tests

```powershell
python test_e1_foundations.py
python test_e12_source_transfer.py
python test_e12_destination_import.py
python test_e12_live_cutover.py
python test_e13_concurrent_migration.py
```

Expected verified milestones: `8 tests ... OK`, `E1.2 source transfer foundations: PASS`, `E1.2 destination import: PASS`, `E1.2 live ownership cutover: PASS`, `E1.3 concurrent live migration: PASS`.

### 12.3 E2 and E3 tests

```powershell
python test_e2_graceful_removal.py
python test_e31_dynamic_gate.py
python test_e32_dynamic_workers.py
```

Expected final markers: `E2 graceful server removal: PASS`, `8 tests ... OK`, `E3.2 dynamic worker membership: PASS`. These scripts are designed for the local in-process cluster and will start their own gRPC servers where required.

### 12.4 E4 and repeated results

```powershell
# Install only if missing from your environment.
python -m pip install matplotlib

python test_e41_four_server_scaling.py

# Three distinct, correctness-checked runs:
for ($i = 1; $i -le 3; $i++) {
    Write-Host "`nE4 TRIAL $i" -ForegroundColor Cyan
    python test_e41_four_server_scaling.py
    if ($LASTEXITCODE -ne 0) { throw "E4 trial $i failed" }
}
```

The reporter creates a different timestamped folder for every passing run. Keep `ops.csv`, `events.csv`, `timeseries.csv`, `summary.json`, `throughput.png`, `latency.png`, as well as the corresponding console output. The observed trial figures above are **measured**, not expected constant benchmark results; fresh runs may differ.

### 12.5 Regression test checks

The user reported a **full Part C suite PASS** after E1.1 and again a **full Part C and Part D suite PASS after E3**. The latter was found using recursive discovery because the initial root-only Part D search found zero tests and correctly raised an error. The final successful scope is **the user's discovered suites**, not an independently audited list of exact test files.

```powershell
# Part C: project-root invocation, module names from Part_C_Tests.
$tests = Get-ChildItem '.\Part_C_Tests' -Filter 'test_c*.py' | Sort-Object Name
foreach ($test in $tests) {
    python -m "Part_C_Tests.$($test.BaseName)"
    if ($LASTEXITCODE -ne 0) { throw "FAILED: $($test.Name)" }
}

# Part D: recursively discover tests, excluding typical environment folders.
$root = (Get-Location).Path
$dtests = @(Get-ChildItem -Path . -Recurse -File -Filter 'test_d*.py' |
    Where-Object { $_.FullName -notmatch '\\(venv|\.venv|__pycache__|\.git)\\' } |
    Sort-Object FullName)
if ($dtests.Count -eq 0) { throw 'No Part D test files were found' }
foreach ($test in $dtests) {
    $relative = $test.FullName.Substring($root.Length + 1)
    $module = ($relative -replace '\.py$', '') -replace '[\\/]', '.'
    python -m $module
    if ($LASTEXITCODE -ne 0) { throw "FAILED: $module" }
}
```

**Important:** A prior script printed a success message when **zero** Part D files matched in the root. That empty-discovery run is **not test evidence**. The later recursive suite was reported PASS and supersedes it. Some earlier Part C tests were intentionally updated to match the newer shared Task-ID/automatic-recovery semantics, preserving their actual correctness checks.

### 12.6 File layout and location of results

During development, Part E files were kept **in the repository root**. Their relocation to `Part_E_Tests/` was discussed but **not reported performed**. Avoid writing documentation that claims the move already happened. Move tests only when ready, then adjust any imports/module invocation commands and rerun them.

Suggested eventual organisation (not a claimed current state):

```text
project-root/
  server.py, server_manager.py, manager.proto, manager_rpc.py
  parameter_client.py, distributed_client.py, coordinated_client.py
  range_clocks.py, consistency.py
  e4_reporting.py
  Part_E_README.md
  Part_E_Tests/                # optional future organisation
  results/e4_elasticity/trial_.../
      ops.csv
      events.csv
      timeseries.csv
      summary.json
      throughput.png
      latency.png
```

---

## 13. Problems resolved, trade-offs and remaining limitations

### 13.1 Problems observed and fixes

| Problem | What happened | Resolution / evidence |
|---|---|---|
| Foundation code supplied in a ZIP initially | User wanted in-chat file edits as in Parts A–D | Returned to incremental, directly supplied source/test code; user executed each change |
| `IndentationError` after client edit | `distributed_client.py` had unexpected indentation near `self.table = new_table` | Replaced `refresh_map()` method block with correct indentation; E1.1 passed |
| Old C2 manual-retry test failed | Test expected logical `NOT_OWNER` despite automatic recovery now implemented | Reworked test to check physical `NOT_OWNER`, selective retry, and final values; PASS |
| Old C3 physical sequence assertions failed | Per-server Task IDs were replaced with shared per-worker allocation | Reworked test to assert uniqueness and verify legacy fallback via actual sequence; PASS |
| Ownership-map synchronization for second independent source | A second initial owner may still have an old epoch after S1→S3 migration | Added participating-server map synchronization before the next `move_range()`; E4 2→4 PASS |
| Root-only Part D regression discovery | PowerShell found no `test_d*.py` in project root | Recursive discovery used; full discovered Part D suite later reported PASS |
| First two uploaded plots were identical | Both were throughput graph, latency graph not yet supplied | Actual latency graph uploaded and reviewed |
| Local transition shading hard to see | Migration took only milliseconds but graphs use one-second bins | Keep vertical scaling-event marker; report actual transition duration in summary |
| Inconsistent performance improvements | Trial 1 median Push latency increased slightly | Report all three trials and avoid universal latency-speedup claims |

### 13.2 Implementation boundaries — state and deduplication

1. **In-process migration coordinator:** `ServerManager.move_range()` calls source and destination **Python service objects**. The current evidence does not show running the migration-control path when owners reside in independent OS processes/hosts; worker Push/Pull and ownership discovery do use real gRPC.
2. **Crash during migration is not recovered:** no durable transfer log, restartable migration ID, replica, rollback transaction or consensus protocol. The coordinator can leave partial state if a step fails after destination installation or publication. Part F must address such failures explicitly.
3. **Deduplication metadata is deliberately limited:** matching completed Pushes are imported when the recorded touched-key bounds lie **fully inside** the moving range. A single Push spanning both retained and moved ranges, especially a sparse set whose min/max straddles the split, is not represented as a full migrating logical operation. The source also retains some historical dedup records. Arbitrary ambiguous commits, retry during owner changes and ID collisions after worker restarts are **not proven exactly-once**.
4. **Task-ID scope:** shared allocation is unique among per-owner connections **inside one live `DistributedParameterClient` instance**. Other processes using the same worker identity, a restarted worker, or lost task-counter state can still collide. Full persistent identity design is Part F/hardening work.
5. **Global atomicity:** a logical distributed Push may touch several owners and is still a set of per-owner RPCs; no global atomic commit or globally consistent multi-owner Pull snapshot is provided.
6. **Migration coordination limitations:** one outgoing transfer per source and one incoming staging area per destination; transfers are serialized through the manager's lock. No transfer throttling, back-pressure policy, concurrent multi-source imports, timeouts/retries of manager-side transfer or large-state streaming/batching was measured.
7. **Cleanup and rollback:** the source cancellation test verifies it can resume before a committed cutover. It does **not** prove complete rollback after the destination has installed state or clients have observed the new ownership epoch.
8. **Range-clock volume:** initial range segmentation is efficient for the tested access patterns, but transferring the complete clipped clock-segment snapshot could become expensive with highly fragmented sparse updates; no migration-state byte measurement was recorded.

### 13.3 Worker membership and fault model boundaries

9. **Cooperative leave only:** the client checks that its Push was reported before leaving, but the manager cannot forcibly discover an unreported in-flight operation. A crashed or permanently silent worker is not removed automatically; heartbeat/timeout belongs to F4.
10. **Join baseline semantics:** the newcomer begins at the minimum active progress plus one. The initialized counter is for barrier coordination, **not evidence that it performed earlier iterations**; this matters for algorithm semantics and clock interpretation.
11. **No full dynamic worker experiment matrix:** sequential end-to-end join/leave passed, with bounded/eventual unit tests. The same workload under arbitrary join/leave timing, different τ, multiple worker waves, and crash failures was not reported.

### 13.4 Evaluation and reporting boundaries

12. **Four-worker local workload only:** no large parameter tensors, cross-machine transfer, CPU/wire-level overhead sampling or 1–8 server X2 sweep. Only four active keys were used in E4. Small key/state volume plausibly contributes to the very short measured transition; this is a workload description, not an independently measured causal breakdown.
13. **E4 driver is not generic Part B scenario integration:** it directly orchestrates local scaling and records metrics. X5-style evidence is available; the `add_server` event inside the existing Part B generic scenario framework was not demonstrated.
14. **Short, noisy sample:** three consecutive formal trials, fixed 6+6-second windows and four workers. We have no confidence interval or systematic independent load isolation. Trial 1's latency regressed slightly; performance is not uniformly better.
15. **Metric scope:** only successful logical Push latencies and counts are exported by the E4 reporter. Interrupted/failed RPC attempts, Pull times, migration state-transfer bytes, initial process setup and network packet overhead are not part of those plots.
16. **Provenance:** the final Windows source revision, exact dependency versions and all raw trial CSV/JSON files were not independently supplied for this write-up. Preserve the user's local evidence and record Git SHA before submission. The figures are reviewed images and user-reported console summaries, not reprocessed raw data.
17. **Part F remains open:** failure detection, replication, replica promotion, crash recovery, exactly-once-style retry across uncertain commits, and F5 failure-matrix scenarios were not implemented/tested by E1–E4.

These limitations are deliberately explicit to avoid confusing a **passing student-level local demonstration** with production-grade elasticity.

---

## 14. Consolidated decisions and evidence checklist

| Concern | Chosen design | Rejected/deferred approach | Evidence and caveat |
|---|---|---|---|
| Server join | Manager range split, two-phase snapshot + final delta | Stop-and-copy, dual-write, full remote log | E1.2 + deterministic E1.3 concurrent PASS |
| Moving-key liveness | Serve during bulk copy, short frozen cutover | Freeze all requests for full copy | E1.3 sees during-copy Push and blocked/rerouted Push |
| Clock transfer | Clip and reconstruct range segments | Reset on new partition; per-key clock explosion | E1.1 and E1.2 clock tests PASS |
| Idempotence during transfer | Shared physical Task IDs per live distributed worker + matching completed metadata | Independent per-owner IDs | Identity tests PASS; ambiguous cross-range retries not solved |
| Destination safety | Stage, validate, apply delta, install before routing publication | Advertise empty newcomer | Destination before-owner rejection PASS |
| Map versioning | Increment Part C ownership epoch | Let clients guess owners | E1.2/E2/E4 PASS; no distributed coordinator transaction |
| Server removal | Repeat tested `move_range()` | New deletion protocol or data dropping | E2 two partitions, post-shutdown training PASS |
| Worker join | Minimum active completed progress + 1 | Restart every worker at 1 | E3 gate and sequential integration PASS |
| Worker leave | Remove from active gate; keep history | Erase vector clocks | E3 clock requirements referencing leaver PASS |
| Worker failure | No crash-based auto-exclusion in E | Timeout/heartbeat in F | Graceful path only |
| E4 correctness | Deterministic successful-Push count oracle | Assume success from request completion rate | All 3 formal trials PASS |
| E4 performance | Five-second pre/post windows and marked one-second curves | Only aggregate averages or hand-drawn charts | CSV/JSON/PNG exported in user runs |
| Reproducibility | Per-run timestamped folder and PowerShell runners | Overwrite previous results | Four exported trial directories reported; Git SHA absent |

### 14.1 Verified Part E completion (within tested scope)

- **E1:** confirmed live state transfer of nonzero weights, per-range clock history, matching dedup metadata, partition split, map epoch and automatic `NOT_OWNER` retry; deterministic concurrent Push test PASS.
- **E2:** confirmed handoff of two partitions, preservation of values/clocks/task records, rejection by the former owner and training after its gRPC process stopped.
- **E3:** active/historical worker bookkeeping and membership gRPC, one sequential mid-run join/leave with correct final SGD result and old-clock dependencies.
- **E4:** continuous 2→4 local scale-up with four concurrent workers; two initial successful runs, **three repeated correctly verified runs**, event-marked throughput/latency plots and exported metrics.
- **Regressions:** user reported complete discovered Part C and Part D test suites PASS after E3; no independent full final repo run by the documentation author.

### 14.2 Part F handoff

Preserve all verified Part E source and tests, E4 results and the original Parts A–D functionality. Before F1–F5, inspect the **current repository sources** rather than trusting older uploaded snapshots. The highest-risk areas are **partial cutover recovery, durable/current Task-ID identity, migration of all relevant deduplication/dependency information, heartbeat-based worker exclusion, and promotion of replicated range state**. Do **not** describe Part E's graceful migration as surviving an arbitrary server crash during transfer.

An especially useful F5 test will kill an owner **between snapshot and final delta or during the cutover**, then measure detection, promotion and resumption while checking exact final weights. The E1.3 synchronized test provides a useful starting pattern but does not itself simulate a process crash.

---

## 15. Related documents and evidence provenance

- **`Distributed Systems - Coursework.pdf`** — authoritative E1–E4 specification and Figure 7, p. 9; X5 under experiments §5.8, p. 14; Part F failure obligations.
- **`Part_A_README.md`** — parameter semantics, Task IDs, async API, dependencies and update rules.
- **`Part_B_README.md`** — worker emulator, oracle and original CSV/JSON metrics framework.
- **`Part_C_README.md`** — ownership routing, manager epochs, range clocks, consistency modes and historical C test evidence.
- **`Part_D_README.md`** — key caching, compression, batching, saved communication measurements, and their interaction with new ownership epochs.
- **`agents.md`** — consolidated architectural decisions, file map, verified state and next-part handoff.
- **`observations.md`** — dated E1–E4 test milestones, resolved defects, exact E4 trial outputs, regression status and evidence limitations.
- **User-run local test files** `test_e1_foundations.py`, `test_e12_source_transfer.py`, `test_e12_destination_import.py`, `test_e12_live_cutover.py`, `test_e13_concurrent_migration.py`, `test_e2_graceful_removal.py`, `test_e31_dynamic_gate.py`, `test_e32_dynamic_workers.py`, `test_e41_four_server_scaling.py`, plus `e4_reporting.py` and timestamped local `results/e4_elasticity/` folders.

**Evidence policy:** PASS statuses and quantitative values reflect the user's console logs. The two displayed E4 plot types were visually inspected; raw CSV/JSON directories and a pinned Git revision were not supplied for independent rerun. Reproduction commands are provided so the submitted source and the report can be checked together.

**Summary:** Within a controlled local, no-crash setting, Part E adds and removes parameter servers and workers during training, preserves the tested parameter/clock state during live range migration, and demonstrates 2→4-server elasticity. **All three formal scaling runs preserved the final numerical oracle** (11,681 successful Pushes total). Across those runs, average throughput changed **319.93→330.27 Pushes/s (+3.23%)** and the mean of trial median Push latencies changed **0.997→0.819 ms (−17.89%)**, with material run-to-run variation. The design deliberately stops short of remote, crash-tolerant or globally atomic migration; those limitations are relevant to Part F.
