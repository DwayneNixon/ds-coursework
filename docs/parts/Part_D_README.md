# Distributed Systems Coursework — Part D: Efficient Communication

University of Edinburgh · Distributed Systems (INFR11022) · Building a Parameter Server

This document covers **Part D (D1–D3)** of the coursework. It follows the detailed, evidence-based structure of `Part_C_README.md`: requirements, architecture, **all considered options and why each design was selected or rejected**, implementation history, focused and regression tests, measured experiments, problems and fixes, limitations, and handoff to the next part.

**Status (21 November 2026):** **Three communication optimisations implemented and verified for the tested cases:** **D1 key caching**, **D2 lossless value compression**, and **D3 explicit batching of independent sparse Pulls**. User-run Windows PowerShell output confirms **10 final focused/regression scripts passed**, **three D1 comparison pairs**, **nine D2 comparison runs**, and **six D3 benchmark trials** (three without D1/D2; three with both). These results have **not been independently rerun against a complete, pinned repository revision**. The Part A–C historical notes remain valid within their own reported scopes; the full 17-script Part C suite was not rerun during this final Part D pass.

> **Continuity:** Part A supplies the float64 parameter store, SGD rule, Task IDs, asynchronous RPCs, dependencies and duplicate-Push handling. Part B supplies the worker, oracle and Protobuf-byte/latency instrumentation. Part C supplies per-owner routing, logical distributed Tasks, ownership-epoch refresh, per-range vector clocks and consistency controls. Part D **extends** those components, without replacing the working parameter-server architecture.

---

## 1. Purpose, coursework requirements and selection

Part D in `Distributed Systems - Coursework.pdf` (pp. 8–9) requires **at least three communication optimisations**, measurement of **bytes and latency**, convergence analysis where relevant, and the saving formula:

```text
saving = 1 - (B_optimised / B_baseline)
percentage saving = 100 × saving
```

Section 5.8, **X4** (p. 14), specifically asks for **baseline versus each chosen optimisation** with bytes and latency. The marking guidance (p. 16) additionally values justified trade-offs, including CPU, complexity and potential staleness.

### 1.1 All six options offered by the handout

| Handout option | Benefit and natural workload | Challenges / fit with existing architecture | Decision |
|---|---|---|---|
| **Key caching** | Repeated **sparse** key lists; send a short reference rather than all uint64 IDs | Cache misses, eviction, ordering, stale owners, replaying completed Pushes | **Chosen — D1** |
| **Value compression** | Compress gradients and Pull values when their byte representation is repetitive | Codec CPU cost, incompressible data, invalid payloads, float accuracy, accurate byte instrumentation | **Chosen — D2** |
| **Batching small requests** | Coalesce separate small operations into fewer physical RPCs; reduce per-RPC overhead | Preserve result grouping and task semantics; distinguish from the pre-existing multi-key API | **Chosen — D3, independent sparse Pulls** |
| Aggregation before replication | Combine worker Pushes into fewer replica messages | Meaningful only with Part F replication; replication protocol not yet implemented | **Deferred to Part F** |
| Threshold filtering / reduced precision | Potentially reduce value bytes much more, especially on dense model data | May change numerical answers, convergence and staleness; requires separate error/convergence evaluation | **Not selected** for the initial three; retained as a later research alternative |
| Request pipelining over asynchronous API | Hide network latency by overlapping in-flight work | Existing Tasks are asynchronous; extra pipeline orchestration mainly attacks latency, not always payload bytes; dependency ordering is subtle | **Not selected**; preserve for later performance work |

**Why these three?** They target different sources of communication overhead and can be turned on independently: **D1 removes repeated identifiers**, **D2 shrinks suitable numerical payloads**, and **D3 reduces the number of physical messages**. Each has a measurable before/after test using existing two-server infrastructure. Aggregation-before-replication would depend on unfinished Part F functionality, and lossy filtering would require a new convergence-error policy.

### 1.2 Terminology and scope

A **logical Pull/Push** is a worker-facing operation. In Part C, it is already split into **physical per-owner RPCs**. A sparse request can carry many keys within a single RPC; that existing capability is **not**, by itself, the D3 optimisation. D3 explicitly combines **several independent Pull groups** into one logical distributed call and later separates the replies.

All byte counts below are **serialized Protobuf request and successful response payloads** of the measured physical calls, not TCP/gRPC on-the-wire bytes. The D1/D2 experiment and the D3 batching experiment have **different workloads** and must not have their absolute byte totals mixed.

---

## 2. Architecture after Part D

```text
          Worker / existing Part B or C-facing client
                        |
               CoordinatedClient (optional C4)
                        |
         DistributedParameterClient (Part C + D1 + D2 + D3)
         - ownership-based split and logical task merge
         - D1 signature: per owner / partition / epoch / ordered keys
         - D3 pull_batch(groups): combine independent sparse Pulls
                        |
               per-owner ParameterClient (A + D1 + D2)
               - optional cached-key reference
               - optional lossless gradient compression
               - optional compressed Pull response decoding
               - original serialized Push retained for retry
                        |
                      gRPC / Protobuf
                /                         \
         ParameterServer S1          ParameterServer S2
         - keys / weights           - keys / weights
         - per-range clocks         - per-range clocks
         - D1 bounded LRU cache     - D1 bounded LRU cache
         - D2 zlib encode/decode    - D2 zlib encode/decode
                \                         /
               existing routing/manager ownership map
                  (cached, refreshed on NOT_OWNER)

       harness/metrics.py measures the serialized *physical* attempts
       including cache recovery and original compressed wire sizes.
```

**Key rule:** No cached parameter values or gradients are stored by D1. Only the **ordered sparse key list** is cached. Every Push still computes and commits the intended gradient update; every Pull still reads live weights. D2 changes the **representation**, not the numeric type or rule. D3, as implemented, batches **reads only**, without reordering or aggregating Pushes.

### 2.1 Interfaces and files

| Module / artifact | Part D responsibility |
|---|---|
| `parameter_server.proto` and generated `parameter_server_pb2*.py` | New D1 cache-reference and D2 compression metadata for Push/Pull |
| `server.py` | Bounded ordered-key cache, ownership-epoch invalidation, cache-aware key resolution, lossless gradient decoding, optional Pull response compression |
| `parameter_client.py` | Asynchronous cached requests, miss-and-re-register recovery, exact original Push replay, compression encoding/decoding, original payload-byte tracking |
| `distributed_client.py` | Per-shard deterministic cache IDs; `key_caching` / `value_compression` flags; propagate flags on newly discovered owners; D3 `pull_batch()` |
| `value_compression.py` | Shared lossless float64 → little-endian bytes → zlib encoder and validated inverse |
| `harness/metrics.py` | Count true serialized physical requests, recovery attempts and compressed response **before in-memory decompression** |
| `routing.py`, `range_clocks.py`, consistency components | Reused Part C logic; no new routing or clock structure for D3 |
| `test_d1_*.py`, `test_d2_*.py`, `test_d3_batching.py` | Focused correctness, recovery, compatibility and RPC reduction tests |
| `run_d1_baseline.py`, `run_d3_benchmark.py` | Reproducible local measurement programs, using separate workloads |
| `results/d1_baseline/`, `results/d1_cache_comparison/`, `results/d2_comparison/` | Text results saved by the PowerShell trial commands **if those commands were run as provided** |

During development the Part D test and runner scripts were kept in the **repository root** at the user's request. They have **not yet been reported moved** to a `Part_D_Tests/` package. The exact current local server and D3 code is authoritative; supplied file snapshots span multiple dates and do not constitute a frozen repository version.

### 2.2 Optional flags and backwards compatibility

```python
client = DistributedParameterClient(
    table,
    worker_id=2001,
    value_width=2,
    key_caching=False,        # D1: disabled by default
    value_compression=False,  # D2: disabled by default
)

# Existing calls keep their previous behavior.
values = client.pull([10, 70]).wait(timeout_ms=5000).values

# D3 is an explicit API, not automatic reordering of concurrent calls.
batch = client.pull_batch([[10, 70], [11, 71]])
grouped_values = batch.wait(timeout_ms=5000).values
```

The usual physical `push()` and `pull()` contracts remain available; existing callers need not opt into the new features. Tests verify that the previously working D1 behavior survived D2 and that representative Part C routing/clock APIs remained functional.

---

## 3. D1 — Key caching: design choices and implementation

### 3.1 Problem and alternatives

Repeated sparse requests resend the same ordered uint64 keys. This is avoidable on workloads where key lists recur. Dense `KeyRange` requests already have compact begin/end encoding, so key caching targets **sparse lists** rather than dense ranges.

| Approach | Advantage | Risk / trade-off | Decision |
|---|---|---|---|
| **Cache the ordered sparse list on each physical owner** | Preserves existing routing and shard boundaries; removes repeated key serialization | Must handle miss, eviction, ownership epoch and exact replay | **Chosen** |
| Cache the whole logical key set before splitting | Only one logical ID | Routing/owner changes can invalidate hidden per-server membership; physical servers do not own entire list | Rejected |
| Cache parameter values as well as keys | Might avoid additional reads | Pull results can become stale; destroys basic parameter-server semantics | Rejected |
| Send full keys every time | No cache state and simplest validation | Repeated transmission cost remains | Retained as **cache-OFF baseline** |
| Unbounded dictionary | Simple implementation | Potential memory growth under changing sparse workloads | Rejected in favor of bounded LRU |

### 3.2 Chosen cache identity and encoding

`DistributedParameterClient._cache_id(route)` constructs a SHA-256 digest with a version-specific prefix and length-delimited components: **ownership-map epoch, partition ID, physical owner address, count of keys, and each key in its exact order**. The first 16 bytes are used as a **128-bit cache ID**. The signature is computed **after routing**, so each physical owner registers only the keys it owns.

Ordered identity matters because flat gradient/value vectors are position-sensitive. Reordering `[10,70]` to `[70,10]` without changing the corresponding gradient order would be incorrect. The shortened digest is a practical fingerprint, not a collision-proof identity or an authentication mechanism; the server also rejects an ID that conflicts with a different list **while that ID is still registered**.

The extended Protobuf fields are:

| Message | New field | Number | Purpose |
|---|---|---:|---|
| `PushRequest` | `bytes key_cache_id` | 9 | Identify repeated sparse list |
| `PushRequest` | `bool use_cached_keys` | 10 | Interpret ID as lookup, not registration |
| `PullRequest` | `bytes key_cache_id` | 6 | Same cache ID for Pull |
| `PullRequest` | `bool use_cached_keys` | 7 | Cached Pull lookup |

These fields are optional-in-practice proto3 values, leaving ordinary old-client requests unaffected. Updated Python stubs are generated using `grpc_tools.protoc`.

### 3.3 Registration, hit and miss flow

```text
First request (cold):
  client → server: [explicit sparse keys] + 16-byte cache ID
  server: validate target and ownership; store ordered list in LRU
  server → client: normal successful response

Repeated request (hot):
  client → server: 16-byte ID + use_cached_keys=true
  server: retrieve same ordered list, read/update CURRENT weights
  server → client: normal response

Evicted/unknown ID:
  client → server: cached reference
  server → client: FAILED_PRECONDITION / KEY_CACHE_MISS
  client → server: full-key Pull registration (safe read)
  client → server: replay EXACT original request and Task ID
  server: execute or deduplicate as appropriate
```

The server uses an `OrderedDict` with a default **128 cached key lists per server**, least-recently-used eviction, and the pre-existing server lock. New ownership-map epochs clear the cache. `NOT_OWNER` remains separate from `KEY_CACHE_MISS`: stale ownership causes map refresh and **selective rejected-shard retry**, while cache misses cause local re-registration and exact request replay.

A confirmed cache miss must not advance weights, clocks or completed-task state. The D1 code avoids marking such a miss as a permanently failed Task ID while recovery is attempted.

### 3.4 Push fingerprints, clocks and exact retries

Part A identifies completed Pushes by Task ID and a SHA-256 fingerprint over the **serialized Push request**. A cached reference and a full-list request differ at the byte level. Rewriting a failed cached Push into a different Push payload under the same Task ID would therefore conflict with the existing deduplication contract.

**Chosen recovery:** perform an auxiliary **full-key Pull** to restore the cache, then resend the **unchanged serialized Push** with the **same Task ID**. This preserves the server's fingerprint comparison and prevents a duplicate update for a previously completed request. If an already-applied Push is replayed after cache eviction, re-registration must happen before server key resolution and deduplication can succeed.

`_prepare_push_clocks()` was updated to accept **resolved keys** instead of reading only `request.keys`, which is empty for a cached Push. This ensures the same per-key/partition iteration clocks are committed with the corresponding gradient updates. No new clock data structure was introduced.

**Boundary:** Focused tests cover an exact replay after eviction and automatic recovery from a deliberately cleared cache. They do **not** reproduce every possible timeout/partition/restart sequence or prove durable exactly-once behavior across server restarts; those belong to the failure model of Part F.

---

## 4. D1.1 — Establishing the no-cache sparse baseline

### 4.1 Why a new workload, not the C5 benchmark?

Part C's C5 experiment used **eight dense keys**, an excellent consistency/convergence exercise but a poor case for key caching because a dense `KeyRange` already encodes the target compactly. We therefore created a repeated sparse workload that exercises the same Part C distributed client and two owner servers, with **caching disabled**.

The development progressed through **D1.1A** (two-server sparse routing), **D1.1B** (five repeated sparse Pull/Push iterations with exact SGD expectation), **D1.1C** (reuse Part B byte/latency metrics), **D1.1D** (64 sparse keys, 20 iterations) and **D1.1E** (large sparse keys, 200 iterations, three independent fresh-server runs). We retained the passing small test as `test_d1_setup.py`, rather than permanently copying its setup into every benchmark.

### 4.2 Baseline configuration and correctness

| Setting | Value |
|---|---|
| Parameter servers | **2** fresh local gRPC servers per script invocation |
| Worker | **1** distributed client, worker ID **2001** |
| Owned regions | `[0, 2^63)` and `[2^63, 2^64)` |
| Key set | **64 ordered sparse keys**, 32 in each owner partition |
| Actual construction | `[(2^60)+2i for i in 0..31]` plus `[(2^63)+(2^60)+2i for i in 0..31]` |
| Vector width / update rule | **2** / default SGD with learning rate **0.1** |
| Gradients | Repeated `[1.0, 2.0]` per key (fixed, deterministic) |
| Workload | Per iteration: distributed Pull then distributed Push; **200 iterations** |
| Physical calls | **400 logical operations**, **800 physical shard RPCs** per trial, excluding final verification Pull |
| Metrics | Physical serialized request bytes, successful response bytes, logical Push/Pull P50/P99 latency |
| Correctness | Final parameters checked against `-0.1 × iterations × gradient` with float tolerance |
| Trials | **3** independent program invocations, fresh servers |

The **larger key IDs** were intentional: Protobuf varints encode tiny integer keys compactly, which would understate the potential benefit of replacing realistic large sparse IDs with a short signature. This is a deliberately favorable repeated-key benchmark for D1, not a representative measurement of all access patterns.

### 4.3 Baseline evolution and observations

| Stage | Request bytes | Response bytes | Total bytes | Evidence |
|---|---:|---:|---:|---|
| D1.1C: two keys × five Pull/Push rounds | 490 | 916 | 1,406 | `test_d1_setup.py` passed; basic instrumentation validated |
| D1.1D: 64 small keys × 20 rounds | 24,320 | 89,452 | 113,772 | 40 logical operations / 80 physical RPCs; final SGD correct |
| **D1.1E: 64 large keys × 200 rounds** | **461,746** | **1,911,776** | **2,373,522** | **Same bytes across three trials; final SGD correct** |

For the final no-cache baseline, response payloads were **80.5%** of measured traffic. A key-cache optimisation that primarily affects requests cannot, by itself, remove most of those response bytes. This became an important reason to evaluate D2 separately.

---

## 5. D1.2–D1.5 — Client integration, recovery and evaluation

### 5.1 Verified incremental behavior

| Milestone / test | What it established | Final reported result |
|---|---|---|
| `test_d1_key_cache.py` | Full-key registration, cached Pull, fresh values after Push, unknown ID rejection | **PASS** |
| `test_d1_cached_push.py` | Cached Push SGD, correct clock iteration, same-ID duplicate suppression, fingerprint conflict and safe miss | **PASS** |
| `test_d1_distributed_cache.py` | Per-shard registration, reference-size reduction, cached Push and automatic cache-miss repair, clocks | **PASS** |
| `test_d1_cache_edges.py` | Completed Push replay after eviction; ownership epoch change, selective retry and correct new owner | **PASS** |
| `test_d1_setup.py` | Previously working uncached sparse RPCs and SGD remain valid | **PASS** |
| `Part_C_Tests.test_c2_auto_recovery` | Existing `NOT_OWNER` refresh/retry behavior survives D1 | **PASS** |

A client attribute was initially referenced as `self.key_caching` although its constructor stored `self._key_caching`. The integration test raised `AttributeError`. Changing the `_cache_id()` check to use the existing `_key_caching` attribute corrected it; the following full D1.3 test passed.

During the ownership test, `service3.weights[50] == [-0.3, -0.4]` failed because Python produced `[-0.30000000000000004, -0.4]`. The printed value and a tolerance-based comparison confirmed a floating-point representation difference, **not a lost or duplicated update**. The subsequent D1.5 run passed.

### 5.2 Three-trial D1 comparison (same 64-large-key workload)

| Metric, per trial | Cache OFF | Cache ON | Reduction / change |
|---|---:|---:|---:|
| Request bytes | **461,746** | **232,756** | **228,990 B; 49.59%** |
| Response bytes | 1,911,776 | 1,911,776 | 0 B |
| **Total bytes** | **2,373,522** | **2,144,532** | **228,990 B; 9.65%** |
| Logical operations | 400 | 400 | Identical |
| Physical shard RPCs (no misses in measured loop) | 800 | 800 | Identical |
| Final parameter oracle | PASS | PASS | Preserved |

All three comparison trials had the **same exact byte counts** per mode. The latency figures varied and were compared using **the arithmetic mean of each trial's already-computed percentile**:

| Latency metric (ms) | Cache OFF, mean of 3 | Cache ON, mean of 3 | Interpretation |
|---|---:|---:|---|
| Push P50 | **1.774** | **1.946** | Higher median with caching |
| Push P99 | **2.271** | **3.146** | Higher reported Push tail latency |
| Pull P50 | **1.103** | **1.227** | Higher median with caching |
| Pull P99 | **1.932** | **1.714** | Lower but variable tail latency |

Caching reduced bytes but **did not consistently reduce latency**. Potential overhead includes cache-ID hashing, lookup, locks and client callback bookkeeping; these CPU components were not independently profiled. The three trials on one local machine cannot support a broad performance ranking.

---

## 6. D2 — Lossless value compression: considered alternatives and chosen codec

### 6.1 Why lossless float64 compression?

D1 left response bytes unchanged, yet responses dominated the baseline. D2 therefore targets **numerical payloads**: gradients in Push requests and current parameter values in Pull replies. The original parameter type is **float64**, and no change to learning rate, update order or numerical precision was wanted.

| Encoding option | Potential advantage | Drawback for this project | Decision |
|---|---|---|---|
| **Python `zlib`, on packed IEEE 754 doubles** | Standard library, no extra installation, lossless, straightforward cross-platform fixed-endian serialization | Compression/decompression CPU; some data do not compress | **Chosen** |
| Zstandard (`zstd`) | Fast, adjustable codec with good real-world throughput | Additional Python dependency; more packaging/testing overhead | Not chosen initially |
| Snappy | Designed for fast low-latency compression | Additional dependency; ratio may be lower on some patterns | Not chosen initially |
| Run-length encoding of deltas | Excellent for long runs of identical changes | Requires a delta representation; arbitrary floats often do not form useful runs | Not chosen |
| Threshold filtering / float32 | Can reduce payload dramatically | **Lossy** or omission semantics; changes convergence/accuracy assumptions | Rejected for D2's correctness-preserving goal |
| Compress all messages regardless of size | Simple predictable format | May **increase** bytes for short/incompressible payloads | Rejected; choose compressed form only if smaller |

### 6.2 D2.1 codec and validation

`value_compression.py` packs doubles using explicit little-endian `struct.pack('<Nd', ...)`, compresses the raw bytes with `zlib.compress(..., level=3)`, and reverses the process with a decompressor bounded to the **exact expected uncompressed count × eight bytes**. The decoder rejects invalid counts, malformed streams, incomplete/oversized output and extra data; the float64 bit pattern is preserved for valid finite inputs.

The codec test reported:

```text
D2 Protobuf fields: PASS
Lossless float64 round-trip: PASS
Repeated-value compression: PASS
Raw: 1024 bytes | Compressed: 30 bytes
Invalid payload length rejected: PASS
D2.1 codec: PASS
```

**Important:** 1,024 → 30 bytes was a **highly repetitive raw-value codec demonstration**, not a measured whole-RPC saving or proof of comparable compression on random model data.

### 6.3 Protocol and execution paths

| Message | New field | Number | Use |
|---|---|---:|---|
| `PushRequest` | `bytes compressed_gradients` | 11 | Transmit encoded float64 gradient vector |
| `PushRequest` | `bool gradients_compressed` | 12 | Discriminate compressed from raw gradient field |
| `PullRequest` | `bool accept_compressed_values` | 8 | Client opt-in to compressed reply |
| `PullResponse` | `bytes compressed_values` | 5 | Encoded current parameter values |
| `PullResponse` | `bool values_compressed` | 6 | Mark reply representation |

The client constructs the normal full request, then compares a compressed variant's **actual Protobuf `ByteSize()`**. For Push it sends the compressed form only when smaller; for Pull the server compresses on request and sends the compressed response only if the complete serialized reply is smaller. This is a per-message fallback, not an unconditional claim that compression saves bytes.

The server checks that compressed and raw gradient forms are not mixed, decompresses the exact number of elements and runs the **existing finite-value validation, staged SGD update and clock commit** on the decoded gradients. Pull reply construction preserves keys, width and clocks, replacing only the values field when beneficial. The client decompresses before returning values to ordinary API users, so the Part C distributed merge does not need to understand a new numeric representation.

### 6.4 Preservation of actual transmitted bytes

A decompressed `PullResponse` occupies more Protobuf bytes in memory than the compressed reply actually sent. It would be misleading to benchmark `response.ByteSize()` **after** decompression.

`ParameterClient._schedule()` therefore saves `_response_wire_bytes` **before** decoding. The client also keeps `_pull_request_bytes`, and D1's cache wrapper preserves response sizes and any recovery request/response bytes. `harness/metrics.py` uses these preserved physical-attempt measurements when available and retains the previous Part B fallback for uncached, uncompressed clients.

The reported integration test checked: initial compressed Pull decoding, compressed distributed Push, correct recovered values, correct vector clocks and identical compressed-Push retry idempotence. Both D1 integration/recovery regressions passed after D2 was added.

---

## 7. D2.3 — Compression evaluation and interaction with D1

### 7.1 Same 64-large-key Push/Pull workload as D1

For X4-style comparison, we held constant: two servers, one worker, 64 repeated sparse keys (32 per owner), width two, fixed `[1.0,2.0]` gradients, 200 Pull/Push iterations, 400 logical operations, 800 physical RPCs and the final SGD oracle. Each mode had three user-run trials; the initial three-mode smoke comparison also passed.

| Configuration | Request bytes | Response bytes | Total bytes | Saving vs neither |
|---|---:|---:|---:|---:|
| **Neither D1 nor D2** | 461,746 | 1,911,776 | **2,373,522** | Baseline |
| **D1 only: caching** | 232,756 | 1,911,776 | **2,144,532** | **9.65%** |
| **D2 only: compression** | 268,146 | 1,717,012 | **1,985,158** | **16.36%** |
| **D1 + D2** | 39,156 | 1,717,012 | **1,756,168** | **26.01%** |

**Independent D2 saving:** 2,373,522 − 1,985,158 = **388,364 B per trial**, of which **193,600 B** was on requests and **194,764 B** on responses. Response bytes fell by **10.19%**; unchanged clocks and other metadata remain part of the serialized response.

**D1 + D2 saving:** 2,373,522 − 1,756,168 = **617,354 B per trial**. The remaining request bytes are only 39,156 (a **91.52% request-byte reduction**), but total traffic still includes large response metadata. Effects are not to be interpreted as simply adding independent percentage savings; the optimisations work on overlapping message contents.

### 7.2 Three-trial latency comparison

The following figures are the **arithmetic mean of the three trial-level P50/P99 values**, not pooled-percentile statistics over individual operations:

| Metric (ms) | Neither | D2 only | D1 + D2 |
|---|---:|---:|---:|
| Push P50 | **1.771** | **1.888** | **1.877** |
| Push P99 | **2.172** | **2.366** | **2.553** |
| Pull P50 | **1.083** | **1.229** | **1.194** |
| Pull P99 | **1.843** | **2.078** | **1.791** |

Compression reduced serialized bytes but **did not yield a consistently faster latency distribution**. On local RPCs, encoding/decoding overhead may outweigh the modest transport reduction. No CPU profiler, cross-machine test or confidence intervals were recorded, so CPU cost remains a reasoned limitation rather than a directly measured quantity.

**Convergence boundary:** Because D2 is **lossless**, we expect the same numeric updates; the fixed-gradient final-oracle tests passed. We did **not** generate separate multiworker target-distance convergence curves for compressed versus uncompressed training. Do not claim they were measured.

---

## 8. D3 — Request batching: what was actually implemented

### 8.1 Why not just send multiple keys?

Part A/Part C already support `pull([key1, key2, ...])` and the distributed client already splits that request by owner. That is **one multi-key operation**, not necessarily batching **several independent application-level operations**. D3 must reduce the number of actual physical requests compared with submitting those operations separately.

| Batching design | Benefits | Risks / reason not selected | Decision |
|---|---|---|---|
| **Explicit `pull_batch([group1, ...])` for disjoint sparse Pull groups** | Reuses tested `pull()`, owner routing, compression, key caching, clocks and results merge; returns original group layout | Only independent reads with shared dependency/clock settings; not automatic coalescing | **Chosen** |
| Automatically queue all concurrent Pulls for a time window | Transparent to callers; might find larger batches | Requires scheduler/timer policy, fairness, latency cap, task cancellation/failure mapping | Deferred as unnecessary complexity |
| Batch Pushes with different Task IDs | Potentially fewer RPCs for updates | Must preserve per-Push identity, ordering, partial failures, dependencies and update-rule semantics | Not implemented in D3 |
| Merge keys inside an existing single Pull | Already implemented in Parts A–C | Would not demonstrate a distinct D3 feature | Not counted as new optimisation |
| Batch mixed Pull and Push operations | Flexible and potentially higher savings | Read-after-write and dependency ordering require an explicit transaction or execution sequence | Rejected for this stage |

### 8.2 Chosen API and semantics

```python
# Eight independent caller-visible groups can become one routed Pull.
groups = [[i, 64 + i] for i in range(8)]
result = distributed_client.pull_batch(groups).wait(timeout_ms=5000)

# result.values is a list with one vector-list per original group.
# Values inside each group retain the caller's original key order.
```

`pull_batch()` validates a non-empty list of non-empty list/tuple targets, concatenates keys, records group lengths, **rejects duplicate/overlapping keys across groups**, performs a **single existing distributed `pull(combined_keys, ...)`**, and slices the merged flat value vector back into the original group order. It returns a `Task`-compatible completion object and retains physical subtasks for measurement. An optional shared `deps` / `required_clock` is applied to the whole batch; distinct per-group prerequisites were not implemented.

Because it calls the existing `pull()`, the owner fan-out, key-cache IDs, compressed responses, clock metadata and stale-map retry remain in the tested path. It does **not** batch Pushes, remove per-group dependencies automatically, create a scheduler, or make a multi-server Pull globally atomic.

### 8.3 Focused D3 correctness and size result

`test_d3_batching.py` created two servers split at key 64, performed a Push to initialise 16 keys, then compared eight ordinary distributed Pulls against one batch of the same eight disjoint two-key groups.

```text
Unbatched Pulls: PASS
Batched Pull correctness: PASS
Batched parameter values: PASS
Original physical RPCs: 16
Batched physical RPCs: 2
Original payload bytes: 1088
Batched payload bytes: 390
Payload reduction: 64.15%
D3 request batching: PASS
```

This verified identical returned values, **16 → 2 physical RPCs**, and **1,088 → 390 serialized payload bytes** for one example. The benchmark below repeats the same design at higher sample count.

---

## 9. D3.2 — Repeated batching performance, with and without D1/D2

### 9.1 Experiment design

`run_d3_benchmark.py` starts fresh two-server instances with partition boundary **64**, uses one worker (worker ID derived from trial number), and applies a known Push to make all 16 tested keys nonzero. It then warms both access patterns **outside the measurement**.

Each measured round retrieves the same eight disjoint groups `[[i,64+i] for i in range(8)]`, either by eight **sequential** ordinary Pull calls or a single explicit `pull_batch()` call. The order of the two modes alternates by round to reduce systematic ordering effects. There are **100 rounds × 3 trials** per feature-switch configuration. This is **800 caller-level Pull groups per trial per mode**; those groups are not eight independently allocated Tasks in the batched case.

Two feature-switch configurations were tested: **neither D1 nor D2** and **both D1 and D2 enabled**. The first is the independent D3 effect; the second is the interaction with existing optimisations. Exact counts exclude warm-up and the initial setup Push.

### 9.2 Communication and RPC counts — identical across each mode's three trials

| Feature switches | Access mode | Physical RPCs per trial | Requests | Responses | **Total bytes** |
|---|---|---:|---:|---:|---:|
| D1 OFF, D2 OFF | Eight separate Pulls per round | **1,600** | 30,400 | 78,400 | **108,800** |
| D1 OFF, D2 OFF | **One batched Pull per round** | **200** | 5,200 | 33,800 | **39,000** |
| D1 ON, D2 ON | Eight separate Pulls per round | **1,600** | 60,800 | 78,400 | **139,200** |
| D1 ON, D2 ON | **One batched Pull per round** | **200** | 7,600 | 12,800 | **20,400** |

**RPC saving (both configurations):** `1 - 200/1600 = 87.5%`.

**D3 independently:** `1 - 39,000/108,800 = 64.15%` fewer serialized bytes.

**Incremental D3 when D1 and D2 are enabled:** `1 - 20,400/139,200 = 85.34%` fewer serialized bytes **relative to the unbatched D1+D2 case**.

**All three compared with the original no-optimisation D3 workload:** `1 - 20,400/108,800 = 81.25%`. This is a **cross-configuration comparison on the same D3 benchmark**, not a claim that the earlier 64-key D1/D2 training loop achieved 81.25% savings.

**Counterexample worth reporting:** Caching **increased** unbatched request traffic here (30,400 → 60,800 B). With only two small keys per group, the 16-byte reference and its flags can cost more than simply sending those tiny varints. The combined, batched path instead lets compression and caching operate on larger groups and reduced RPC overhead. D1 should therefore be enabled selectively for a suitable workload, not assumed universally beneficial.

### 9.3 Latency per complete eight-group round

P50/P99 are calculated on **100 complete-round latencies per trial**; below are means of the three trial-level percentiles (milliseconds):

| Feature switches | Mode | P50 (mean of 3) | P99 (mean of 3) | Observed P50 range |
|---|---|---:|---:|---|
| Neither | Eight sequential Pulls | **7.080** | **9.475** | 6.897–7.188 |
| Neither | **Batched Pull** | **0.901** | **1.368** | 0.879–0.918 |
| D1 + D2 | Eight sequential Pulls | **7.111** | **9.779** | 6.850–7.323 |
| D1 + D2 | **Batched Pull** | **0.922** | **1.297** | 0.891–0.954 |

The repeated experiment showed a substantial reduction in time to retrieve all eight groups: approximately **7 ms → 0.9 ms**, together with 87.5% fewer physical RPCs. This is a wall-clock time for a **sequential eight-call workload**; it is **not evidence of an equivalent gain versus eight concurrent independent Pulls**, a different batching window or real inter-machine network conditions.

All six D3 trials reported **`Correctness and RPC reduction: PASS`** and each three-trial command ended with **`D3 benchmark: PASS`**.

### 9.4 D3 limitations / sensitivity

- Batch API is explicit, not transparent timing-based aggregation. Callers choose the groups and pay a combined completion latency; a slow key/owner can hold up the whole batch.
- Only **disjoint non-empty sparse Pull groups** are supported. Overlapping groups could be served without duplicate physical reads in a more advanced API, but duplicate-key handling was intentionally left strict.
- A single shared dependency set and required clock apply to the combined request; no per-group independent failure or Task-ID result is provided.
- Two owners and no failures were used for the repeated D3 latency tests. Existing Part C `NOT_OWNER` recovery was separately tested, but no in-flight ownership migration with D3 grouping was reported.
- The benchmark repeatedly reads unchanged nonzero parameters; it validates response correctness and costs, **not neural-network convergence**.

---

## 10. Testing, regression evidence and reproduction

### 10.1 Environment and protocol generation

Run scripts from the repository root in **Windows PowerShell**, with the same Python environment and working `grpcio` / `grpcio-tools` dependencies used for Parts A–C. For shared imports from the existing Part C test package:

```powershell
$env:PYTHONPATH = "$(Get-Location);$(Get-Location)\Part_C_Tests"
```

After editing `parameter_server.proto`, regenerate and keep the updated stubs alongside the source:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parameter_server.proto
```

`zlib` and `struct` are Python standard-library modules; the D2 codec adds **no third-party compression package**.

### 10.2 Final 10-script focused/regression run (reported 21 November 2026)

| Script / command | Coverage | User-reported status |
|---|---|---|
| `python test_d1_setup.py` | Repeated sparse routing, SGD oracle, Part B byte accounting | **PASS** |
| `python test_d1_key_cache.py` | Registration, cached Pull, fresh weights, unknown ID | **PASS** |
| `python test_d1_cached_push.py` | Update, vector clock, duplicate Task ID, fingerprint and safe miss | **PASS** |
| `python test_d1_distributed_cache.py` | Distributed cache hits, smaller references, automatic miss recovery | **PASS** |
| `python test_d1_cache_edges.py` | Exact Push replay after eviction and owner-epoch transition | **PASS** |
| `python test_d2_codec.py` | Protobuf fields, exact float64 round-trip and malformed-length detection | **PASS** |
| `python test_d2_integration.py` | Compressed Pull/Push, clocks, idempotent replay, D1 compatibility | **PASS** |
| `python test_d3_batching.py` | Group-value equality, physical RPC count and byte saving | **PASS** |
| `python -m Part_C_Tests.test_c2_auto_recovery` | Existing stale-manager-map selective retry | **PASS** |
| `python -m Part_C_Tests.test_c4_client_clock_dependencies` | Existing cross-owner required-clock, per-key wait, legacy API | **PASS** |

These are **10 executed scripts**, not 10 individual assertions. Other relevant Part C integration coverage was reported earlier; the **full 17-script Part C loop was not rerun in this final D session**. No tests should be represented as executed in the documentation-authoring environment.

To repeat the same focused checks:

```powershell
python test_d1_setup.py
python test_d1_key_cache.py
python test_d1_cached_push.py
python test_d1_distributed_cache.py
python test_d1_cache_edges.py
python test_d2_codec.py
python test_d2_integration.py
python test_d3_batching.py
python -m Part_C_Tests.test_c2_auto_recovery
python -m Part_C_Tests.test_c4_client_clock_dependencies
```

### 10.3 Reproduce the three optimisation comparisons

The two benchmark files are currently in the repository root. They start fresh servers for each invocation/trial.

**D1 / D2** (same repeated 64-large-key, 200-iteration Push/Pull workload):

```powershell
python run_d1_baseline.py                   # D1 OFF, D2 OFF
python run_d1_baseline.py --cache           # D1 ON,  D2 OFF
python run_d1_baseline.py --compress        # D1 OFF, D2 ON
python run_d1_baseline.py --cache --compress # D1 ON, D2 ON
```

For three independent runs, invoke each configuration three times and keep its complete console output. During development the outputs were saved using `Tee-Object` under `results/d1_cache_comparison/` and `results/d2_comparison/`; **those local files were not individually inspected here**. The test runner still prints its historical `D1.1D sparse baseline: PASS` label for all feature-switch modes; read the **Caching/Compression: ENABLED/DISABLED** lines to determine the actual configuration.

**D3** (100 rounds × eight groups × three fresh-server trials per invocation):

```powershell
python run_d3_benchmark.py
python run_d3_benchmark.py --cache --compress
```

The D3 runner prints three trial sections per command. It does not produce CSV by itself in the code shared in conversation, so preserve terminal output if submitting the measurements as X4 evidence.

### 10.4 Optional broader Part C regression

`Part_C_README.md` documents the earlier green **17-script suite** and its PowerShell loop. Repeat that suite before a final release, especially after further server/protocol changes. Its prior successful run **preceded** the full D1/D2/D3 integration, so the targeted two-script Part C regression in this session is useful but not an equivalent replacement.

---

## 11. Problems encountered and how they were resolved

| Issue | Observed symptom | Explanation / resolution | Evidence limit |
|---|---|---|---|
| Unhelpful initial D1 benchmark packaging | First baseline supplied as a ZIP rather than incrementally explained changes | Returned to the existing Parts A–C approach; source placed in project root, explained and user-run step by step | Workflow correction, not a code defect |
| Tiny sparse keys obscure caching gains | Protobuf varints for keys `<128` are already compact | Kept small-key test as control, moved final D1 benchmark to large sparse uint64 IDs across two partitions | Favors repeat-key workloads, not universal savings |
| Cached Push lacks explicit `request.keys` | Original clock preparation would route an empty key list | Pass **resolved keys** into `_prepare_push_clocks()` | Cached Push clock test passed |
| Cache miss with Task-ID deduplication | Rebuilding a different Push request under same Task ID changes its serialized fingerprint | Register keys with an auxiliary Pull, then replay the **original** request bytes/ID | Exact replay and deliberate eviction tests passed |
| Cache miss could mark recoverable task failed | A recoverable reference lookup should not permanently fail the Task ID before retry | Skip failed-task finalisation for `KEY_CACHE_MISS`; ordinary errors unchanged | Automatic recovery test passed |
| Wrong client caching attribute | `AttributeError: ... key_caching ... Did you mean: '_key_caching'?` | `_cache_id()` now uses `self._key_caching`, matching constructor | Subsequent D1.3 PASS |
| Exact float equality in ownership test | `[-0.30000000000000004, -0.4]` failed strict comparison with `[-0.3, -0.4]` | Print actual value; compare components with `abs_tol` | D1.5 rerun PASS |
| Decompressed Pull size distorted metrics | In-memory reply becomes larger than transmitted compressed reply | Capture `_response_wire_bytes` before decoding; count physical request attempts and recovery | D2 comparison bytes repeated across trials |
| Small unbatched Pulls worsen with D1 | Unbatched D1+D2 request bytes **60,800**, versus **30,400** without | For two tiny keys, signature/flags exceed raw key varints; choose optimization based on key size/reuse | Observed in D3 workload |
| Comparing D1/D2 and D3 byte totals | D1/D2 used 64 large keys plus Push/Pull; D3 used eight tiny Pull groups | Report separate workloads; compare the all-three result **only against the same D3 baseline** | Correct X4 interpretation |
| End-to-end latency not guaranteed to improve with byte savings | D1 and D2 median latencies mixed/increased | Present both byte and P50/P99 results, avoid declaring a universal speedup | No CPU profiler/cross-machine network experiment |

**Evidence rule:** Intermediate errors are retained for diagnosis. A milestone is marked successful only after the user reported its corrected output.

---

## 12. Consolidated design decisions and trade-offs

| Concern | Chosen implementation | Alternatives and why not chosen | Cost / remaining caveat |
|---|---|---|---|
| At least three D optimisations | D1 key IDs, D2 values, D3 message count | Replication aggregation needs F; lossy filters need accuracy analysis; pipelining adds orchestration | Three different feature mechanisms, different workloads |
| Key-cache placement | Physical owner after Part C routing | One pre-routing logical cache | Ownership and epoch coupling |
| Cache signature | Truncated SHA-256 of epoch/owner/partition/ordered keys | Index-only/global ID without scoping | Extremely unlikely collision remains theoretically possible |
| Cache storage | Per-server 128-list LRU, cleared on new epoch | Unbounded map; parameter-value cache | Evictions require re-registration; no persistence |
| Cache-miss recovery | Auxiliary full-key Pull + exact Push replay | Rewrite failed Push with full keys | Extra round trips; deterministic fingerprint preserved |
| D1 default | OFF | Always-on | May increase bytes for small key lists |
| D2 codec | zlib level 3 over little-endian float64 | zstd/snappy require dependency; RLE/delta need special workload | CPU cost; per-message `ByteSize()` fallback |
| D2 correctness | Lossless float64 and strict decoded-size validation | Reduced precision / filtering | Same numeric representation, no independent convergence curve |
| D2 default | OFF | Universal compression | Compression may not help every message |
| D3 API | Explicit `pull_batch()` of disjoint sparse reads | Background automatic coalescer; batched Pushes | Common dependency/clock requirement, one combined Task |
| D3 routing | Reuse existing distributed `pull()` | New server-side batch RPC type | No server/proto changes for D3 |
| Payload measurement | Actual serialized physical attempts and pre-decode response sizes | Reconstructed or only logical `ByteSize()` | Excludes gRPC headers/TCP and setup/observer traffic |
| Correctness evidence | Fixed-gradient final SGD, clock/identity tests, group-value equality | Timing alone | No live migration/replication/large-scale proof |
| Latency reporting | P50/P99 across three trials, explicit per-trial aggregation | Single trial interpreted causally | Local scheduling noise; only three trials |

---

## 13. What is complete, limitations and next-part handoff

### 13.1 Verified within the tested Part D scope

- Three handout-listed optimisations implemented: **D1 per-owner sparse key-cache references**, **D2 lossless numeric compression**, and **D3 explicit batching of independent sparse Pull groups**.
- Cache registration, hot lookup, unknown ID rejection, LRU eviction/re-registration, exact serialized Push retry, duplicate protection, owner-epoch change and vector-clock correctness verified by focused user-run tests.
- Compressed gradients and Pull values verified as accurate, with codec validation, unchanged SGD, vector clocks and duplicate-Push behavior.
- Batched Pulls return the same groups and values as separately issued Pulls, with **87.5% fewer shard RPCs** in the tested eight-group workload.
- Three-trial D1, D2 and D3 evaluations measure Protobuf bytes and P50/P99 latency; comparison formulas and workload definitions are documented.
- The **10-script final targeted test set** passed, including Part C `NOT_OWNER` recovery and server-enforced client clock-dependency integration.

### 13.2 Open limitations and obligations

1. **X4 workload coverage:** D1's final key list repeats exactly and uses large uint64 IDs; D2 uses repeated float values; D3 uses eight sequential two-key Pulls. These are explanatory favorable microbenchmarks, **not a broad Zipf/dense/incompressible workload matrix**.
2. **Convergence:** No separate compressed-vs-uncompressed multiworker convergence curves were reported. Lossless D2 and read-only D3 preserve tested numerical correctness, but this is not a substitute for a broader convergence experiment.
3. **Overhead:** No CPU time, memory-residency, bandwidth, compressed ratio by entropy or network packet bytes were profiled; statements about CPU cost are trade-off analysis, not direct CPU measurements.
4. **Partial batching semantics:** D3 batches only **explicitly supplied, disjoint sparse Pull groups** with one shared dependency/clock set. It does not transparently merge arbitrary concurrent calls or batch independent Push Task IDs.
5. **Cache coordination:** No load test for concurrent registrations/evictions, cache collisions, server restart, persistence, authentication or recovery during a true network partition. D1 cache does not itself provide durable exactly-once delivery.
6. **Ownership and clocks:** Part C's epoch/`NOT_OWNER` route recovery was tested, but a **live state-preserving transfer of weights and clocks** is Part E. Clearing the cache on epoch change is necessary but not a migration algorithm.
7. **Atomicity:** A distributed logical operation still consists of independent per-owner operations. Neither caching, compression nor batching creates a globally atomic multi-server transaction or snapshot.
8. **Regression completeness:** Final 10 tests passed; the full Parts A/B/C suite and the 17-script Part C loop have not been user-reported rerun after every D2/D3 change. Do so before final submission.
9. **Reproducibility:** No pinned Git commit, Python/gRPC version lock, collected raw per-operation CSV for D3, cross-machine benchmark or independent container execution was supplied with these logs.
10. **Other coursework requirements:** Parts E and F, safe migration/failover/replication and the complete X1–X7 experiment set remain separate obligations. An 81.25% D3 microbenchmark saving is **not** the overall coursework's universal savings figure.

### 13.3 Handoff to Part E / subsequent conversation

**Part D checkpoint: VERIFIED COMPLETE (tested scope)** as of **21 November 2026**. Preserve the Part D README and all current server/client/Protobuf/codec/test/benchmark files. Preserve Part C's manager, routing, range-clock and consistency logic. Keep D1/D2 **off by default**; D3 is an explicit method.

For **Part E**, the next challenge is runtime addition/removal of servers and workers with correct state/clock transfer. In particular, a new ownership epoch must invalidate old D1 cache references, and newly discovered `ParameterClient` connections must retain the D2 compression flag. The existing C2/D1 `NOT_OWNER` tests demonstrate selective rerouting, **not copying the old owner's values and clocks**. Avoid treating the passing D1.5 ownership test as a complete E1 migration test.

Recommended evidence to carry into the next chat: `agents.md`, `observations.md`, `Part_C_README.md`, this `Part_D_README.md`, the official PDF and the **actual latest** `server.py`, `parameter_server.proto`, `parameter_client.py`, `distributed_client.py`, `value_compression.py` plus tests. Earlier uploaded server/proto snapshots predate Part D; inspect current files before making changes.

---

## 14. Related documents and evidence provenance

- **`Distributed Systems - Coursework.pdf`** — Part D choices and saving formula pp. 8–9; X4 experiment p. 14; communication marking guidance p. 16.
- **`Part_A_README.md`** — asynchronous RPCs, numeric values, Task IDs, update rules and original safety constraints.
- **`Part_B_README.md`** — worker/oracle and the original payload/latency measurement framework.
- **`Part_C_README.md`** — contiguous routing, ownership map and epoch, clocks, consistency and the pre-D regression checkpoint; this README matches its explanatory style.
- **`agents.md`** — project-wide current state, decisions, source map and next-part handoff.
- **`observations.md`** — dated test PASS results, byte/latency figures and resolved failures, preserving the Parts A–C history.
- **Part D test scripts, `run_d1_baseline.py`, `run_d3_benchmark.py` and locally preserved `results/`** — source-side experiment artifacts and user-produced output.

**Evidence policy:** The quantitative results are copied from the user's Windows PowerShell executions in this conversation. The documentation was assembled from those results, supplied source excerpts and the coursework/Part C README. It is **not** an independently executed benchmark, a verified Git release or a guarantee of correctness for untested production failure modes.

**Summary:** Part D delivers three deliberately distinct, opt-in communication optimisations. On the repeated 64-large-key workload, **D1 saved 49.59% of request bytes and 9.65% overall**, **D2 saved 16.36% overall**, and **D1+D2 saved 26.01% overall**. On the separate eight-group Pull workload, **D3 saved 64.15% of total bytes and 87.5% of physical RPCs**, while **all three together saved 81.25% against that workload's unoptimised baseline**. Numerical and targeted regression tests passed. These are bounded microbenchmark results with explicit trade-offs, not universal performance claims.
