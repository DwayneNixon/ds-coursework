# Building a Parameter Server · Distributed Systems Coursework

Python/gRPC parameter server implementing sparse and dense vector updates, asynchronous tasks, routing and range clocks, consistency modes, communication optimisations, live elasticity, replication and failover.

**Status:** Final audit in progress. Development has included all Parts A–F and X1–X7; verified guarantees are **limited to tested configurations**. A partial postpublication migration failure remains deliberately fail-closed. Do not mistake a selected smoke test for a complete submission audit.

## Quick start

Use **Python 3.13** (the CI version). From the repository root:

```bash
python -m pip install -r requirements.txt
python server.py
```

This launches a standalone parameter server at `127.0.0.1:50051`, default vector width 2, SGD learning rate 0.1. In another terminal:

```bash
python -m Part_A_Tests.test_ranges
python -m Part_A_Tests.test_public_api
```

Restart the standalone server before rerunning stateful test scripts: completed Task IDs are deliberately deduplicated. Client workers must have **distinct stable worker IDs**, and retries must preserve the original Task ID and payload. A single request may contain at most 200,000 parameter values by default (configurable via `ParameterServer(max_values_per_request=...)`); the overall model can contain many more keys.

The generated `*_pb2*.py` files are committed, so ordinary checkouts do **not** require running protoc. When changing a `.proto`, install a compatible `grpcio-tools` to regenerate the paired bindings.

## Regressions

Run all Part A–F test scripts, each in its own Python process with fresh single-server state:

```bash
python -m scripts.run_all_regressions
```

Results and individual failure logs are written to `results/audit_regressions/`. This is a more thorough check than the selected GitHub Actions smoke suite. The two CI workflows are:

- `.github/workflows/audit-smoke.yml` — end-to-end smoke, X1–X7 selected experiments and fault scenarios
- `.github/workflows/audit-full.yml` — all discovered Part A–F test scripts

## Experiments

One dispatcher makes the evaluation commands discoverable:

```bash
python -m experiments x1
python -m experiments x2
python -m experiments x3
python -m experiments d3
python -m experiments x4
python -m experiments x5
python -m experiments x6
python -m experiments x7
```

X4 expects the D3 benchmarking CSV to have been generated first; X7 expects the X6 evidence. The X2 default configuration runs **1–8 real server processes** with three seeds and exports `scaling.csv` and `scaling.png`. See [experiment commands and input/output contracts](docs/EXPERIMENTS.md). The previous `python -m Part_*_Tests.run_*` entrypoints are retained for compatibility.

## Repository map

| Location | Responsibility |
|---|---|
| `ds/` | Core implementation: server, client, ownership manager, routing/clocks, consistency, migration/replication and heartbeat/failover components |
| `harness/` | Worker generator, independent oracle, deterministic workload, scenarios, fault injection and B5 metrics |
| `experiments/` | Central X1–X7 experiments, C3 clock growth and D3 communication benchmark |
| `configs/` | Seeded experiment configurations |
| `Part_A_Tests/` … `Part_F_Tests/` | Requirement-specific regression tests and compatibility command entrypoints |
| `docs/` | Architecture, experiment recipes, testing, report draft, demo plan and historical implementation notes |
| `results/` | Recorded trial results and plots |
| `scripts/` | Complete test-suite runner |
| `*.proto`, `*_pb2*.py` | Worker/manager wire contracts and generated bindings |
| Root `*.py` | Backward-compatible imports and `server.py` CLI; the main implementation lives in `ds/` |

## Read this before submission

[Architecture and design](docs/ARCHITECTURE.md) · [Experiments and evidence](docs/EXPERIMENTS.md) · [Regression instructions](docs/TESTING.md) · [Report draft](docs/REPORT_DRAFT.md) · [10-minute demo plan](docs/DEMO.md)

Original milestone histories: [Part A](docs/parts/Part_A_README.md), [Part B](docs/parts/Part_B_README.md), [Part C](docs/parts/Part_C_README.md), [Part D](docs/parts/Part_D_README.md), [Part E](docs/parts/Part_E_README.md), [Part F](docs/parts/Part_F_README.md).

### Explicit boundaries

- Each partition's Push is locally atomic, but an operation spanning multiple servers is **not a distributed transaction**.
- Replicated ACK fencing and confirmed crash-stop recovery were demonstrated for the tested single-failure, manager-alive configurations, **not** arbitrary partitions, byzantine failures or simultaneous multiple server losses.
- The F3.4 partial postpublication migration source-crash case remains **fail-closed**; it protects committed state by refusing writes instead of guaranteeing availability.
- E4's migration and plotted 2→4-server experiment run with in-process server objects; X2 uses separate server processes. Treat them as **different deployment scopes**.
- The experimental byte counts are serialized Protobuf application payloads, excluding gRPC/TCP headers and backup traffic.
- Some archived Part READMEs contain commands from the former flat directory structure; this README and the central experiment/test guides are authoritative.
