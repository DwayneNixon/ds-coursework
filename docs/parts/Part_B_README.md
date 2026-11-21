# Distributed Systems Coursework — Part B: Reusable Test Harness

University of Edinburgh · Distributed Systems (INFR11022) · Building a Parameter Server

This document covers **Part B (B1–B5)** of the coursework: a reproducible worker emulator and experiment harness for the parameter server implemented in Part A. It explains the **architecture, key implementation decisions, alternatives considered, test coverage, experimental outputs, and limitations**. The intention is to reuse the same harness when developing and evaluating Parts C–F, rather than replacing it with a separate test system each time.

**Status (16 November 2026):** The **single-server Part B harness baseline** passed its user-run focused tests and its final JSON-configured, five-output-file integration regression. The final run recorded **39 operations, 19 completed iterations, and 12,036 serialized Protobuf payload bytes**. These are *reported local Windows/PowerShell results*, not an independently rerun or pinned Git commit. **Part B is not claimed to satisfy every future distributed scenario yet**: multi-server actions, a complete one-million-key end-to-end experiment, measured recovery times, and staleness tracking remain outstanding.

> **Relationship to Part A:** Part B builds on the existing `ParameterClient` and gRPC server. It does **not** replace `server.py`, `parameter_client.py`, or `parameter_server.proto`. For the single-node API, update rules, dependencies, task IDs and Part A tests, see the separate Part A README.

---

## 1. Purpose and requirements

Part B is **not a machine-learning implementation**. It generates the communication and compute pattern of distributed stochastic gradient descent (SGD), with controllable workloads and faults, so that later distributed-system choices can be tested against repeatable inputs.

| Requirement | What the harness provides | Evidence / boundary |
|---|---|---|
| **B1 — Worker loop** | `pull → simulated compute → push`, for configurable iterations; push depends on the preceding pull | Single-worker and integrated tests passed |
| **B2 — Realistic workloads** | Seeded Zipf sparse access, contiguous dense ranges, scattered hot keys, lognormal compute delays, stragglers | Sampling/compute/scatter tests, four-worker run, direct dense RPC test passed; million-key *generator* test only |
| **B3 — Correctness** | Seeded hidden target, target-difference gradients, deterministic fixed-gradient mode, read-only convergence observer | Single-worker analytic oracle, multiworker fixed-gradient and observer tests passed |
| **B4 — Faults and elasticity** | Timed/iteration-triggered scenarios, local subprocess workers, worker add/kill, injected client-to-server delay/drop | Scenario/process/fault tests and combined worker scenario passed; not a real network partition or server failover |
| **B5 — Metrics** | Operation/iteration/convergence/event CSVs and JSON summary; latency percentiles; 1-second-window push throughput; Protobuf payload sizes | Focused exporter/math/byte tests and five-file integrated run passed |

The harness design follows Sections **5.1–5.7** of `Distributed Systems - Coursework.pdf`. The larger experiments **X1–X7** in Section 5.8 are still to be completed alongside Parts C–F.

## 2. Architecture and execution flow

```text
                         configs/b5_integrated.json
                                    |
                                    v
                           Driver (run_b4.py)
                          /        |         \
                         /         |          \
                        v          v           v
                 LocalCluster  ScenarioEngine   Metrics/export
                      |         (time/iteration)      ^
          worker subprocesses      |                  |
               |  |  |             v                  |
               |  |  +------> action callback         |
               |  |             |                     |
               |  |     add/kill processes           |
               |  |     delay/drop link state        |
               v  v                                   |
        run_worker / WorkloadGenerator                 |
               |                                      |
        FaultInjectingClient                           |
               |                                      |
         ParameterClient -----------------------------  |
               |       PROGRESS / METRIC_OP /           |
               |       METRIC_ITER via child stdout     |
               v                                      |
         Part A gRPC server <---- read-only Observer --+
               |                      |
        in-memory key -> vector   target distance

           Results: ops.csv, iterations.csv, convergence.csv,
                    events.csv, summary.json
```

The **driver** is the coordinator of an experiment, not a new parameter server. Worker processes access parameters through the **Part A client API**. A dedicated observer uses a separate client to inspect model convergence. The driver collects operation and iteration reports from worker stdout, executes scenario events, then writes the standard results files.

### Separation of responsibilities

- **Worker** knows how to perform an iteration, not how the cluster is deployed.
- **Workload generator** selects keys and compute delays from seeded random streams, not from server state.
- **Correctness oracle** computes expected behaviour independently of actual server updates.
- **Scenario engine** decides *when* an action fires; the provided callback decides *how* to perform it.
- **Cluster adapter** starts/stops OS processes and stores their local node handles.
- **Fault wrapper** injects selected client-side link effects without modifying the Part A transport.
- **Metrics/observer** measure results without taking over training or ownership logic.

This modular boundary is deliberate: later Parts C–F can supply new routing, server actions, consistency rules, and failure mechanisms while retaining the same experiment structure.

## 3. Repository files

The following names are the Part B scripts/modules **reported implemented and tested**. Your local repository is the authoritative source for their current contents; a full, independently audited working tree was not supplied.

| Path | Responsibility |
|---|---|
| `harness/worker.py` | B1 worker loop, gradient modes, progress and metrics callbacks |
| `harness/workload.py` | B2 sparse Zipf/dense/scatter key selection; compute and straggler sampling |
| `harness/oracle.py` | B3 seeded hidden target, expected target-difference result and fixed-gradient oracle |
| `harness/observer.py` | B3 read-only periodic and final convergence measurements |
| `harness/scenarios.py` | B4 `ScenarioEngine` event validation, triggers, dispatch and event records |
| `harness/cluster.py` | B4 `LocalCluster` subprocess lifecycle and supported process actions |
| `harness/faults.py` | B4 `LinkFaultController` and `FaultInjectingClient` delay/drop injection |
| `harness/metrics.py` | B5 measurement helper, CSV writers, percentiles, throughput, byte totals and JSON summary |
| `configs/b5_integrated.json` | Seeded, declarative single-server integration scenario |
| `run_b1.py`, `run_b2.py` | Earlier focused worker/workload demonstrations |
| `run_b4.py` | Combined B4/B5 experiment driver and worker subprocess entry point |
| `test_b2_*.py`, `test_b3_*.py`, `test_b4_*.py`, `test_b5_*.py` | Focused tests described in Section 10 |
| `results/` | Experiment output directories; retain relevant runs for reporting |

There is **one reusable harness**, even though development and smoke-test scripts carry milestone names. The final driver is still called `run_b4.py`; the name does not mean it only supports B4.

---

## 4. B1 — Worker emulation

### The iteration loop

Each emulated worker repeatedly performs:

```text
for iteration = 1 .. T:
    keys = sample_keys_for_this_iteration()
    pull_task = client.pull(keys)
    weights = pull_task.wait()
    sleep(simulated_compute_time)
    gradients = calculate_gradients(weights, keys)
    push_task = client.push(keys, gradients, width, deps=[pull_task])
    push_task.wait()
    record_iteration_and_operation_metrics()
```

`deps=[pull_task]` preserves the intended data dependency. Both pull and push use asynchronous **Part A `Task` handles**, even though the emulated SGD loop waits for results at the natural points in an iteration.

A small deterministic B1 example used one worker, three iterations, width two and four keys. Its final weights matched the expected vector `[0.271, -0.542, 0.813, 1.084]` within floating-point tolerance.

### Decision: emulated computation, not actual neural-network training

| Option | Why it was considered | Decision |
|---|---|---|
| Train a real ML model | Could generate realistic gradients | **Not chosen**: introduces ML, GPU and dataset variance unrelated to parameter-server behaviour |
| **Pull / sleep / synthetic gradient / push** | Reproducible timing and correctness; directly exercises communication | **Chosen**: matches the required distributed-SGD *access pattern* |

### Decision: preserve the existing client API

We reused `ParameterClient` rather than adding another RPC implementation in the harness. This keeps future routing, dependencies and compression behind a consistent worker-facing interface. The harness never needs to know the actual server-side dictionary representation.

---

## 5. B2 — Workloads, sparsity and heterogeneous workers

### Sparse: Zipf-distributed popularity

In sparse mode, keys are selected according to ranked popularity: rank `r` is sampled with probability proportional to `r^(-s)`, where `s` is the Zipf exponent. Draws are made **with replacement**, then deduplicated and sorted into the requested key set. Therefore the number of *unique* keys in an iteration can be smaller than the number of draws.

- Larger `zipf_s` creates stronger hot-key concentration.
- `keys_per_iter` controls how many ranks to draw.
- Sorting gives a stable key order for matching returned weights to gradients.
- A dedicated, seeded key-selection stream makes the generated selections repeatable.

### Scatter: separating popularity from key location

When `scatter=true`, ranked key popularity is mapped to actual key IDs through a **fixed seeded permutation** of the keyspace. Hot ranks are no longer automatically concentrated in the lowest-numbered IDs. With `scatter=false`, ranks map more directly to key locations, which is useful when deliberately studying hot spots later.

**Choice:** precompute a seeded permutation using `array('Q')` and shuffle it. This avoids the much higher per-element memory overhead of a Python list of Python integers, though a complete million-key permutation still consumes memory and time. The million-key **sampling/permutation smoke test passed**; this does *not* establish million-key server/RPC throughput.

### Dense: contiguous key ranges

Dense mode produces a contiguous `range(start, end)` (half-open, with unit step), which the Part A API can send as `KeyRange` rather than listing every key. A direct native-range Push/Pull test passed.

**Important limitation:** some earlier worker integration paths converted generated ranges to Python lists. Thus the *direct dense API* is verified, but fully native range handling throughout every harness workload path has **not** been established.

### Compute times and stragglers

Compute time is sampled from a lognormal distribution (tested baseline: median **20 ms**, sigma **0.4**). Selected stragglers multiply that duration by a configurable slowdown factor. Key selection and compute timing have distinct seeded random streams, so changing timing parameters need not change the access pattern.

**Choice:** sleep to model heterogeneous computation rather than run variable CPU/GPU workloads. This makes communication comparisons easier to interpret and avoids hardware-dependent compute noise. One live B2 demonstration used **four workers, five iterations each, with one straggler**; all 20 iterations completed and the resulting weights remained finite.

---

## 6. B3 — Independent correctness checks

Two complementary gradient modes were implemented because they answer different correctness questions.

### Mode 1: `target_diff` — convergence towards a hidden vector

`TargetVector(num_keys, width, seed)` generates a reproducible hidden target `w*`, with Gaussian components. The worker computes the gradient using the pulled weight:

```text
g = w_pulled - w_target
w_new = w_old - η × g
```

For one worker updating every key under this simple schedule, with initially zero weights:

```text
w(T) = w* × (1 - (1 - η)^T)
```

With the default Part A learning rate `η = 0.1`, the five-iteration B3 test reported an expected Euclidean distance of **0.8286654752553987** and an actual distance of **0.8286654752553986**. The difference is floating-point roundoff.

The coursework also specifies a **multiworker sequential aggregation** formula involving the number `m` of workers touching each key: the error contracts by `|1 - ηm|` per synchronized iteration. That stronger synchronized multiworker case is **not yet verified** by the single-worker B3 test; it belongs with the Part C consistency evaluation.

### Mode 2: `fixed` — exact offline final weights

The fixed-gradient generator computes each gradient as a deterministic function of worker ID, iteration, key and vector component. It does **not** depend on the weights read from the server. Consequently, for a fresh zero-initialized server with SGD:

```text
expected_weight[key, component]
    = -η × sum(all intended fixed gradients for that key/component)
```

This result is independent of worker interleaving (up to floating-point rounding) and is therefore useful for checking **lost or duplicated logical updates** under future faults. A focused test with **three workers, four iterations each** matched the offline expected weights.

### Observer: measure without updating

`ConvergenceObserver` has its **own read-only `ParameterClient`**. It periodically pulls all model keys, computes Euclidean distance to `w*`, and records:

```text
ts_ns, iteration, dist_to_target
```

The driver records an initial sample **before starting workers** and a final sample **after they exit**, plus periodic samples on a separate thread. This was tested both alone and in the integrated experiment.

**Observer trade-offs:** pulling the whole model provides a direct correctness signal without changing weights, but it produces extra RPC traffic. Batched pulls are **not an atomic global snapshot**, and a reported iteration label can lag visible updates due to concurrent execution and delayed worker progress messages. Observer bytes are intentionally **not** included in worker operation totals.

---

## 7. B4 — Scripted events, process control and link faults

### Scenario design

A `ScenarioEngine(events, execute)` handles events with exactly one trigger:

- `at: {"time_s": ...}` — time since experiment start.
- `at: {"iteration": ...}` — minimum completed iteration across **currently active** workers (the slowest active worker).

When a trigger becomes due, the engine invokes an `execute(action, args)` callback once and records timestamp, action, arguments and outcome. Actions are allowed to execute **in trigger order**, which may differ from their order in the JSON file. Failures can be recorded as error outcomes.

This separation avoids hardcoding process and network operations inside the schedule engine. It was chosen over a single monolithic fault loop because Parts C–F will add server actions without replacing scheduling logic.

### Local processes rather than threads

`LocalCluster` starts and tracks **real child processes** (`subprocess.Popen`), allowing a worker to be terminated abruptly. This is closer to a process failure than simply setting a stop flag in a thread. The driver retains the node handles; logical worker IDs (for example `702`) are translated to process-node IDs where required.

**Current scope:** the tested live scenario starts, adds and kills *workers*. A full distributed server manager, graceful server transfer and actual server crash/recovery are not yet verified.

### Delay/drop injection without rewriting Part A

`LinkFaultController` maintains shared link settings in a JSON state file that is written with an atomic replace. `FaultInjectingClient` wraps an existing `ParameterClient`; before an outgoing `pull` or `push`, it can:

- add a configured delay and optional jitter; or
- probabilistically drop the call before it is submitted.

A focused test observed a configured **80 ms** delay (approximately **86.73 ms** elapsed) and confirmed that a **100% drop probability** prevented a push from updating the server. The final scenario showed multiple `FAULT_DELAY worker=701 ... ms=80.0` messages.

**Why this choice?** It provides deterministic, testable fault hooks without altering Part A networking or relying on OS-specific traffic-control privileges. **Limit:** this is worker-side **pre-RPC outgoing interception**, not a true bidirectional packet-level link emulator. It cannot independently simulate lost responses, mid-flight loss, long-lived TCP partitions, or traffic between multiple server processes.

### Integrated scenario that passed

The final JSON-configured smoke test used the following parameters:

| Setting | Value |
|---|---|
| Server | One existing Part A server, `127.0.0.1:50051` |
| Seed | `42` |
| Parameters | `16` keys, width `2` |
| Initial workers | `701`, `702` |
| Worker iterations | `8` each; simulated compute `200 ms` |
| At `time_s = 0.3` | Add worker `703` |
| At slowest active iteration `1` | Delay worker `701` → `server1` by `80 ms` (no jitter) |
| At slowest active iteration `2` | Kill worker `702` |

The **observed event order** was `add_worker`, `delay_link`, then `kill_worker`: a timed event can become due before an iteration-triggered event even when it appears later in configuration. Worker **702 visibly completed three iterations** before termination; workers **701 and 703 each completed eight**. An event trigger is not an instruction to freeze every process exactly at that iteration.

**Design boundary:** although event records are generic, some assertions in `run_b4.py` still refer specifically to `701`, `702` and `703`. They are appropriate for this smoke test but must be generalized for future configurations and experiments.

---

## 8. B5 — Instrumentation and output schema

### Metrics collection design

The worker loop calls optional `on_operation` and `on_iteration_record` callbacks. When the worker is a subprocess, it immediately prints JSON records prefixed with `METRIC_OP` and `METRIC_ITER`, along with `PROGRESS` lines. A driver-side reader thread forwards each worker's stdout to a thread-safe queue, and the driver drains that queue into its in-memory records.

**Chosen:** structured, flushed stdout over separate per-worker metrics files or a new RPC metrics service. It reuses the existing subprocess output channel and lets the driver retain **already-delivered** metrics when a child is killed. A killed worker can still lose records for calls that have not finished or flushed.

### Five result files

All are written to the configured `output.dir` (`results/b5_integrated/` in the demonstrated scenario).

| File | Fields / meaning |
|---|---|
| `ops.csv` | `ts_ns, worker, iteration, op, n_keys, bytes_out, bytes_in, latency_ms, status, retries` — per pull/push operation |
| `iterations.csv` | `worker, iteration, t_start, t_end, staleness` — completed iterations, start/end timestamps in Unix nanoseconds |
| `convergence.csv` | `ts_ns, iteration, dist_to_target` — observer samples |
| `events.csv` | `ts_ns, action, args, outcome` — executed scenario actions; `args` is JSON embedded in the CSV cell |
| `summary.json` | Completed pushes, windowed peak throughput, P50/P99 push/pull latency, total recorded bytes, final distance and a recovery-times placeholder |

#### Latency

`measure_operation` uses a monotonic high-resolution timer from invoking the client call through `task.wait()` completion. It therefore measures **client-observed operation completion latency**, including any injected pre-call delay and dependency waiting. It is **not** an isolated network round-trip-time measurement. Successful operations contribute to reported P50/P99 percentiles. The helper uses linear interpolation between ordered samples.

#### Throughput

Throughput is defined as successful completed **pushes per second** in a sliding one-second interval. The current helper counts pushes in the preceding one-second window at **push-completion timestamps** and reports an event-sampled peak. It does not yet write a regularly sampled throughput series for time plots or recovery calculations. A synthetic test with push completions at `0.2`, `0.8` and `1.7` seconds verified window counts `[1, 2, 2]`.

#### Serialized communication bytes

The metric is the size of **serialized Protobuf request and response messages**:

- **Push outgoing:** the length of the *exact* deterministic serialized PushRequest already retained by Part A's `Task` (`_push_request_bytes`).
- **Pull outgoing:** reconstructed `PullRequest` using the requested keys or `KeyRange`, actual pull Task ID, and dependency Task IDs, then measured with `.ByteSize()`.
- **Incoming:** the completed Protobuf response's `.ByteSize()`.
- **Total:** sum of `bytes_out + bytes_in` across **recorded worker operations**; missing measurements are not quietly interpreted as zero.

This includes Protobuf metadata, keys, values/gradients and dependencies where present. It **excludes** gRPC/HTTP/2 headers and framing, TCP/IP overhead, observer pulls and RPCs not recorded by worker callbacks. For a failed operation, bytes may be `null` because actual delivery is unknown. The pull request is reconstructed to match the tested client code, rather than measured by a transport-level interceptor.

#### Incomplete metrics (intentionally explicit)

- **Staleness:** blank/null in `iterations.csv`. A pull does not currently return the version/iteration of its freshest data, so reporting `0` would be incorrect. Version/clock support belongs with Part C.
- **Recovery time:** `recovery_times` is currently empty, *not zero*. The required definition uses the median throughput over **five seconds before** a failure/join and the first time throughput returns to at least **90%** of that baseline; the short single-server smoke scenario cannot establish this.
- **Retries:** normal worker operations report `retries=0`; scenario-driven explicit retry attempt accounting has not yet been integrated.
- **Traffic coverage:** only operations reported to the driver are counted, not every possible RPC or byte on the network.

---

## 9. Configuration and reproducibility

The demonstrated configuration file is `configs/b5_integrated.json`. Its main fields are:

```json
{
  "experiment": "b5_integrated",
  "seed": 42,
  "server_address": "127.0.0.1:50051",
  "model": { "num_keys": 16, "value_width": 2 },
  "workers": { "ids": [701, 702], "iterations": 8, "compute_ms": 200 },
  "scenario": [
    { "at": { "iteration": 1 }, "action": "delay_link", "a": 701, "b": "server1", "ms": 80, "jitter_ms": 0 },
    { "at": { "time_s": 0.3 }, "action": "add_worker", "count": 1 },
    { "at": { "iteration": 2 }, "action": "kill_worker", "target": 702 }
  ],
  "output": { "dir": "results/b5_integrated", "observer_interval_s": 0.1 }
}
```

The user reported that this JSON passed `python -m json.tool` and that the **JSON-configured local driver** then passed the five-file experiment. In the configuration, `kill_worker.target` is a **logical worker ID**; the driver translates it to the local cluster's process-node handle.

**Determinism contract:** the same seed/configuration should reproduce **key sets, target values, compute-time samples and fixed gradients**. It should **not** be expected to reproduce OS scheduling, which worker finishes first, the precise instant a wall-time event takes effect, latency, or the exact number of worker operations completed just before an abrupt kill. A fresh parameter-server state is also needed for repeatable correctness experiments.

**Important:** The integrated smoke test uses a small fixed scenario; the richer coursework Section 5.3 schema (`cluster`, `consistency`, `access`, `stragglers`, etc.) is a **future experiment target**, not an assertion that every example field is already accepted by this driver. The harness has a general workload generator, but the integrated smoke scenario exercises only a small subset of its modes.

---

## 10. How to install, run and test

### Prerequisites

Use Python 3, `grpcio`, `grpcio-tools`, the existing Part A server/client, and generated Protobuf stubs. The Part B harness's standard-library modules include `csv`, `json`, `random`, `time`, `threading`, `queue`, `subprocess`, `tempfile`, `array`, `pathlib` and `concurrent.futures`. No separate ML framework is needed for the demonstrated harness.

From the repository root in **PowerShell**:

```powershell
python -m pip install grpcio grpcio-tools
```

If the `.proto` changed and generated stubs are missing or stale:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parameter_server.proto
```

### Run the integrated experiment

**Terminal 1 — start a genuinely fresh single-node Part A server:**

```powershell
python server.py
```

**Terminal 2 — validate configuration and launch the harness:**

```powershell
python -m json.tool configs/b5_integrated.json
python run_b4.py
```

The important output checks include:

```text
Convergence monitoring: PASS
Original and added workers completed: PASS
Worker termination: PASS
Scenario event execution: PASS
Live link delay injection: PASS
B4 final integration: PASS
Convergence/event exports: PASS
Summary JSON export: PASS
Completed-worker metrics: PASS
Killed-worker metrics preserved: PASS
Operation/iteration consistency: PASS
B5 live metrics integration: PASS
Five-file integration: PASS
```

Expected output location:

```text
results/b5_integrated/
    ops.csv
    iterations.csv
    convergence.csv
    events.csv
    summary.json
```

**Why restart the server?** The Part A server caches completed Task IDs `(worker_id, sequence)` in memory. Recreating clients with IDs `701`, `702`, etc., while reusing that server can produce `INVALID_ARGUMENT: Task ID reused with different request`. Stopping the **actual server process** and starting a fresh one clears the old in-memory task history and initial weights. Merely rerunning `run_b4.py` does not reset server state. This collision was encountered and resolved during integration.

### Focused test reference

Tests were run **incrementally**, often with a fresh server for tests that use default port `50051`. The following are the named scripts and behaviours supported by the recorded development history; results below are user-reported local executions.

| Stage | Script / command | What was verified |
|---|---|---|
| B1 | `python run_b1.py` | One-worker SGD loop and exact expected weights |
| B2 | `python test_b2_sampling.py` | Seeded sparse selection, sorting/uniqueness and million-key generator |
| B2 | `python test_b2_scatter.py` | Seeded scatter mapping |
| B2 | `python test_b2_compute.py` | Compute distribution and straggler slowdown |
| B2 | `python test_b2_dense_api.py` | Direct native `range` Pull/Push |
| B2 | `python run_b2.py` | Four-worker, five-iteration live workload |
| B3 | `python test_b3_oracle.py` | Single-worker target-distance analytic result |
| B3 | `python test_b3_fixed.py` | Offline multiworker fixed-gradient sum |
| B3 | `python test_b3_observer.py` | Independent periodic convergence samples |
| B4 | `python test_b4_scenarios.py` | Time/iteration triggers and one-time dispatch |
| B4 | `python test_b4_links.py` | Configured delay, dropped push, fault event |
| B4/B5 | `python run_b4.py` | Live add/kill/delay, metrics queue, observer, five files |
| B5 | `python test_b5_ops.py` | Operation latency and CSV export |
| B5 | `python test_b5_iterations.py` | Iteration timestamps, missing staleness, CSV export |
| B5 | `python test_b5_records.py` | Convergence/event CSV exports and JSON event arguments |
| B5 | `python test_b5_summary.py` | P50/P99, sliding window, byte aggregation and JSON export |
| B5 | `python test_b5_bytes.py` | Actual Protobuf payload sizes for sparse/dense/dependency calls |
| Config | `python -m json.tool configs/b5_integrated.json` | JSON syntax validity |

An additional B4 dummy-process test was reported passing, but its exact filename was not retained in the supplied notes. Do not invent one or assume a repository-wide `pytest` suite exists.

**Run server-dependent tests individually** with the correct initial state. A large alphabetical test loop against a single live server can reuse worker IDs and mutable parameters, producing misleading failures.

---

## 11. Measured results and evidence

### Selected focused results

| Check | Actual reported observation |
|---|---|
| B1 | Expected `[0.271, -0.542, 0.813, 1.084]`; actual matched up to `0.8130000000000001` roundoff |
| B3 target oracle | Expected distance `0.8286654752553987`; observed `0.8286654752553986` |
| B3 fixed gradients | 3 workers × 4 iterations; measured weights matched offline gradient sum |
| B4 link fault | Configured 80 ms delay measured about 86.73 ms in focused test; 100% dropped outgoing push did not update parameters |
| B5.2 iteration timing | Worker 902: `8.274`, `6.378`, `6.180` ms for three iterations (individual machine measurements; not benchmarks) |
| B5.4 math fixture | Three synthetic pushes produced `[1, 2, 2]` one-second-window counts; the 900-byte synthetic total was *test data*, not server traffic |

### B5.7 focused real Protobuf payloads

| Operation | Request bytes | Response bytes | Total bytes |
|---|---:|---:|---:|
| Sparse pull | 20 | 40 | 60 |
| Push with pull dependency | 63 | 2 | 65 |
| Dense pull with push dependency | 27 | 40 | 67 |
| **Total** | **110** | **82** | **192** |

All six checks in `test_b5_bytes.py` passed, including the equality between the operation CSV's byte sum and `summary.json`.

### Final JSON-configured integrated regression

| Result | Actual reported value |
|---|---:|
| Initial workers | 701 and 702 |
| Worker dynamically added | 703 |
| Worker terminated | 702 (after three visible completed iterations) |
| Workers that completed all eight iterations | 701 and 703 |
| Executed scenario events | 3 (`add_worker`, `delay_link`, `kill_worker`) |
| `ops.csv` rows | **39** |
| `iterations.csv` rows | **19** |
| Generated required output files | **5 of 5** |
| Serialized worker payload bytes | **12,036** |
| Summary byte-total equality | **PASS** |

There were **39 recorded operations** but **19 completed iterations**. With one successful pull and one push per completed iteration, 19 iterations explain 38 operations; the remaining operation **may** belong to a worker killed during its next unfinished iteration. The specific row was not inspected, so this is an interpretation, **not a verified classification**.

The user separately validated all 39 CSV records had nonempty outgoing/incoming byte fields and computed a total of **12,036 B**, exactly equal to `summary.json["total_bytes"]`. This is the strongest final check on the new accounting. It does not claim 12,036 bytes of *total network traffic*.

### Issues encountered and resolved

| Issue | Cause / correction |
|---|---|
| `UnboundLocalError` accessing `message` | The metrics-dispatch `if` block was outside the message-draining `while` loop; indentation was corrected |
| `INVALID_ARGUMENT: Task ID reused with different request` | Replayed worker IDs against an old live server; stop/restart the real Part A server to reset its in-memory task history |
| Event ordering differed from JSON order | Correct expected behaviour: events fire when their separate wall-time or iteration triggers become due |

---

## 12. Choices made, alternatives rejected, and why

These are the **Part B** decisions, not commitments about Part C's partitioning or consistency architecture.

| Design question | Chosen approach | Alternative(s) and reason not chosen |
|---|---|---|
| How to simulate learning? | Pull/compute-sleep/push with synthetic gradients | Real ML training adds dataset/GPU noise and is unnecessary for testing the server |
| How to couple harness to Part A? | Existing asynchronous `ParameterClient` | Direct `grpc` or a second client implementation duplicates API and dependency handling |
| How to sample sparse access? | Seeded Zipf ranks, draw-with-replacement, unique sorted keys | Uniform-only access does not expose popular-key hot spots |
| How to distribute hot ranks? | Optional fixed seeded permutation (`scatter`) | Always-hot low IDs confounds key popularity with partition placement |
| How to represent dense access? | Native half-open Python `range` / `KeyRange` where supported | Expanded lists cost memory/serialization; some worker paths still need integration work |
| How to model stragglers? | Lognormal delays with slowdown multiplier | Running actual heavyweight computation reduces timing control |
| How to verify correctness? | Both target-difference and deterministic fixed gradients | Only convergence can mask duplicates; only fixed gradients do not show convergence behaviour |
| How to observe convergence? | Separate read-only client and monitoring thread | Reusing training clients would entangle measurement and worker state |
| How to specify events? | Declarative trigger/action records and callback-based engine | Hardcoded sleeps and process actions throughout the driver are less reusable |
| How to emulate worker failure? | Actual child OS processes | Threads cannot be abruptly killed as isolated OS processes |
| How to inject basic link faults? | Worker-side wrapper plus shared JSON link state | Reworking Part A networking or requiring OS packet filters would increase scope and platform dependence |
| How to collect child metrics? | Flushed JSON lines on stdout, central queue | Per-worker local files or a new metrics RPC service are more moving parts for this baseline |
| What is a communication byte? | Serialized Protobuf request/response payload | Number-of-floats × 8 omits metadata; packet-level wire measurement is out of current instrumentation scope |
| What to do with unavailable metrics? | Explicit empty/null staleness, bytes or recovery fields | Fabricated zeroes would incorrectly imply measurements were taken |

**Design philosophy:** choose small, testable components with clear interfaces, validate each independently, then combine them in one reproducible experiment. Preserve flexibility for distributed ownership, real faults and new consistency/compression options in later parts.

---

## 13. Current limitations and work for Parts C–F

The final B4/B5 run is **an integration smoke test on one Part A server**, not the complete Section 5.8 evaluation. Important limitations:

1. **Scale:** no end-to-end benchmark with ≥ 1,000,000 server parameters or eight-server scaling curve. The million-key *workload generator* was smoke-tested.
2. **Distributed operations:** no verified ownership manager, transparent cross-server routing, `NOT_OWNER` refresh, range-clock splitting, server additions/removals, replication or failover yet.
3. **Fault realism:** worker-side pre-RPC delays/drops are not bidirectional network partitions; `kill_server`, `add_server`, `remove_server` and partition healing still require distributed integration.
4. **Consistency/staleness:** no implemented sequential/eventual/bounded-`τ` comparison or server-reported data age. Do not interpret blank staleness as fresh data.
5. **Recovery:** no valid five-second pre-event throughput baseline, regularly sampled throughput time series, or server recovery-time result.
6. **Exactly-once after crashes:** fixed-gradient correctness works in normal concurrency tests, but persistence and failure-safe duplicate suppression remain future work.
7. **Incomplete worker records:** a force-killed worker cannot report in-flight operations that never reach its callback; collected records are a faithful log of delivered events, not an omniscient RPC trace.
8. **Measurement overhead:** the observer reads the model periodically, but its traffic is not included in worker-byte totals; repeated model-wide scans can influence server performance.
9. **Configuration generality:** the current config runs the demonstrated smoke scenario, and several assertions still assume workers 701/702/703. Generalize them before running X1–X7.
10. **Versioning:** no validated Git revision/package-version lock was included in the recorded console outputs; future report-quality experiments should record both.

### Experiments remaining (coursework Section 5.8)

| ID | Question | When it becomes meaningful |
|---|---|---|
| **X1** | Does sequential multiworker aggregation match the analytic formula? | Part C sequential consistency and synchronization |
| **X2** | How does throughput/P99 scale from one to eight servers? | Part C partitioning/manager |
| **X3** | What do eventual and bounded-delay consistency change? | Part C selectable models, clocks and stragglers |
| **X4** | How many bytes and milliseconds do communication optimizations save? | Part D codecs/batching and baseline comparison |
| **X5** | Is server addition smooth under load? | Part E live server membership/range transfers |
| **X6** | Does server failure preserve fixed-gradient final weights exactly once? | Parts E/F recovery, replay and deduplication |
| **X7** | What happens in the harsh failure matrix? | Part F combined faults |

## 14. Recommended next steps and repository care

- **Keep** the Part A server/client/proto, the reusable `harness/` modules, scenario config, focused tests, and the five output schemas. Avoid rewriting working components just to adopt the next distributed design.
- **Maintain separate documentation:** Part A README for the single-node server; **this Part B README** for the harness; `agents.md` for architectural continuity; `observations.md` for dated, evidence-based test results.
- **Before Part C coding:** compare manager-held range partitioning with consistent hashing; then discuss vector-clock representation and consistency enforcement. Record the chosen approach and why it was selected. No Part C design choice is finalized in this README.
- **For future measurements:** use fresh server state, fixed input seeds, several repetitions, preserved experiment configs and source revision, and document the gap between logical payload bytes and physical wire bytes.
- **Do not delete tests merely because their features passed.** They are regression checks when Part C–F modifies the client or server.

**Summary:** Part B has a **working, verified single-server harness baseline** with seeded workers, workload generation, analytic and fixed-gradient oracles, subprocess scenario actions, simulated delay/drop faults, read-only convergence monitoring, and five-file CSV/JSON reporting. The integrated experiment reported **39 operations, 19 completed iterations, and 12,036 recorded worker Protobuf payload bytes**. Subsequent parts will extend this same harness for genuine distributed consistency, elasticity, communication optimization and fault recovery.
