# Distributed Systems coursework — Parameter Server

A Python/gRPC parameter server developed for the University of Edinburgh Distributed Systems coursework. Implementation and tests are organised into Parts A–F. The accompanying part READMEs explain the design, test cases, measurements and stated limitations.

> **Submission status:** This repository is still undergoing final audit. The existing X3–X7 evidence does not replace unverified X1 or missing X2 evidence or a full requirement-by-requirement review.

## Requirements

- Python 3.13 (used by the automated audit smoke tests).
- Dependencies listed in `requirements.txt`.
- Local TCP ports available for the gRPC servers and manager; process-spawning tests use localhost.
- Run commands below from the **repository root**.

### Install

```bash
python -m pip install -r requirements.txt
```

The repository already contains generated `*_pb2.py` and `*_pb2_grpc.py` bindings, so compiling Protobufs is not required for an ordinary checkout. If you edit a `.proto` file, install a compatible `grpcio-tools` and regenerate both corresponding bindings.

## Run a standalone parameter server

In terminal 1:

```bash
python server.py
```

By default it listens on `127.0.0.1:50051` with width-2 vectors and SGD learning rate 0.1.

A server also limits each Pull/Push to **200,000 parameter values** by default (for width 2: at most 100,000 keys per request). The optional `ParameterServer(max_values_per_request=...)` constructor setting changes this budget; it does **not** limit the total number of parameter keys in the model. Oversized requests fail with `RESOURCE_EXHAUSTED`.

In terminal 2, from the repository root:

```bash
python -m Part_A_Tests.test_ranges
python -m Part_A_Tests.test_public_api
```

Some Part A scripts, including the two above, connect to an existing standalone server. Other tests launch their own temporary in-process gRPC server and need no manually started server.

**Important:** Restart the standalone server when re-running stateful demonstrations that rely on fresh weights and worker Task IDs. A reused worker ID and sequence can intentionally be treated as an already-completed logical Push.

## Reproducible automated tests

A clean-checkout regression workflow is provided in `.github/workflows/audit-smoke.yml`. It compiles and imports the code and exercises selected Part A/C/F regressions, B4/B5 integration, X6 and X7 on a GitHub Actions runner.

Run selected isolated checks from the root:

```bash
python -m Part_A_Tests.test_server_dependencies
python -m Part_A_Tests.test_rule_registration
python -m Part_A_Tests.test_concurrent_pushes
python -m Part_C_Tests.test_c1_worker_harness
python -m unittest -v Part_F_Tests.test_f12_automatic_failure_detection
python -m unittest -v Part_F_Tests.test_f24_process_replication
```

### X1 — analytical multiworker aggregation

The X1 experiment reuses the Part B worker loop, independent hidden-target oracle, convergence observer and B5 CSV/JSON exporters. All workers Pull the same round snapshot before submitting their target-difference gradients, and the next round waits for every acknowledged Push. This realizes the synchronous sequential round assumed by the handout's formula.

```bash
python -m Part_B_Tests.run_x1_aggregation
```

Configuration: `configs/x1_aggregation.json`. The driver compares every measured round distance with `|1 - learning_rate * number_of_workers|^t × initial_distance` to numerical tolerance, and writes `ops.csv`, `iterations.csv`, `convergence.csv`, `events.csv`, `oracle.csv` and `summary.json` under `results/x1_aggregation/`. No separately started server is required.

### B4/B5 integrated worker and fault scenario

Start a **fresh** standalone server in another terminal:

```bash
python server.py
```

Then run:

```bash
python -m Part_B_Tests.run_b4
```

The driver reads `configs/b5_integrated.json` and emits the B5 observation files under `results/b5_integrated/`. It is a configured smoke scenario, not yet a general-purpose X1/X2 benchmarking framework. The scenario currently contains specific expectations for workers 701, 702 and 703.

### X6 — live server crash with offline oracle

No manually started server is required. The runner launches its own manager and server processes.

```bash
python -m Part_F_Tests.run_f51_x6 --output results/f51_x6
```

Default experiment settings are loaded from `Part_F_Tests/f51_x6_config.json`. The output includes acknowledged Push evidence, offline-oracle comparison, and recovery-time measurements.

### X7 — failure matrix

Run X6 first, then:

```bash
python -m Part_F_Tests.run_f52_x7 --x6-summary results/f51_x6/summary.json --output results/f52_x7
```

The runner reads `Part_F_Tests/f52_x7_config.json`, verifies X6 evidence, and launches isolated process/fault tests. A deliberately documented `FAIL_CLOSED` migration case is a safety limit, not a recovered/available case.

## Layout

- `server.py`, `parameter_client.py`, `distributed_client.py`, `server_manager.py`, `coordinated_client.py`: core implementation.
- `*.proto`, `*_pb2.py`, `*_pb2_grpc.py`: gRPC service contracts and generated bindings.
- `harness/`: workloads, worker loop, independent oracle, scenarios, fault injection and metrics.
- `Part_A_Tests/` through `Part_F_Tests/`: focused regressions and experiment runners.
- `configs/`: B5 integration configuration.
- `results/`: committed experiment artifacts and summaries.
- `Part_A_README.md` through `Part_F_README.md`: historical implementation notes, instructions and constraints. Some older root-level command examples predate the current test-folder organisation; use the module commands above.

## Known limits and audit status

- The X1 analytic-oracle runner has been implemented and is part of CI, but its evidence and integration review are ongoing. X2's 1–8 server scaling experiment still requires reproducible evidence.
- Fault experiments establish guarantees only for their recorded configurations and injected boundaries, not arbitrary network partitions or multiple simultaneous failures.
- X7 records one intentional fail-closed postpublication migration case.
- The final project-wide source, configuration and performance audit remains in progress. Do not infer full submission readiness solely from the smoke workflow.
