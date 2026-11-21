# Distributed Parameter Server

A Python/gRPC parameter server for the University of Edinburgh Distributed Systems coursework, covering asynchronous updates, multi-server ownership, consistency, communication optimisations, elasticity and crash recovery.

**Status:** Implementation and evidence audited. The recorded live demonstration is a separate submission requirement. All evaluated fault guarantees are limited to the tested crash-stop model.

## Quick start

Python **3.13** was used for reproducible automated checks.

```bash
python -m pip install -r requirements.txt
python server.py
```

In a second terminal:

```bash
python -m tests.part_a.test_ranges
python -m tests.part_a.test_public_api
```

The standalone server listens on `127.0.0.1:50051`. Restart it before running another stateful demonstration. Stable, **unique worker IDs** are required because Task IDs are deduplicated on retries. Maximum Pull/Push request size is 200,000 parameter values by default (adjustable in the server constructor).

## Where everything lives

| Directory | Purpose |
|---|---|
| `ds/` | Parameter-server implementation: client, primary servers, routing, range clocks, manager, consistency and fault tolerance |
| `harness/` | Reusable worker, deterministic workloads, numerical oracle, metrics, fault/cluster helpers |
| `tests/part_a` – `tests/part_f` | All 98 tests grouped by coursework requirement |
| `experiments/` | Canonical evaluation drivers, X1–X7, B/D benchmarks and X3 analysis plots |
| `configs/` | Seeded JSON configurations (including X6 and X7 fault trials) |
| `results/` | Final reproducible experiment evidence and the required X4 raw benchmark inputs |
| `docs/` | Architecture, experiment methods, requirement mapping, testing and demo plan |
| `scripts/` | Test runner, repository checker, report builder and evidence manifest |
| `submission/` | Four-page report, editable rendered source and hand-in checklist |

Root-level Python modules are small **compatibility aliases** for the existing public imports; the substantive implementation is maintained in `ds/`. `server.py` remains a runnable entrypoint, and `*.proto` plus the generated `*_pb2*.py` bindings remain at the root.

## Test the project

Run every Part A–F test script with fresh process state:

```bash
python -m scripts.check_repository
python -m scripts.run_all_regressions
```

For a specific part, use `--part A` through `--part F`. See [testing and reproducibility](docs/TESTING.md). GitHub Actions contains the [full regression](.github/workflows/audit-full.yml) and [X1–X7 integration/evidence](.github/workflows/audit-smoke.yml) workflows.

## Run experiments

```bash
python -m experiments b1
python -m experiments b2
python -m experiments b4           # requires fresh standalone server
python -m experiments x1
python -m experiments x2           # 1–8 real server processes, 3 seeds
python -m experiments x3           # includes consistency plots
python -m experiments d3
python -m experiments x4           # uses d3 and D1/D2 reference logs
python -m experiments x5           # in-process 2 → 4 server migration
python -m experiments x6           # process-separated live crash + oracle
python -m experiments x7           # after x6; nine-case fault matrix
```

The recorded outputs are in `results/`. X6 uses `results/x6_failover/`; X7 uses `results/x7_fault_matrix/`. See [experiment configurations and scope](docs/EXPERIMENTS.md).

## Report and hand-in

```bash
python -m pip install -r requirements-report.txt
python -m scripts.build_report_pdf
```

The builder verifies **at most eight A4 pages** and writes `submission/parameter_server_report.pdf`. The [submission checklist](submission/README.md) explains preparing the source archive and recording the required **≤10-minute live demonstration**.

**Important limitations:** No cross-shard atomic transaction; manager is a coordination dependency; worker identities must not be reused; E4 migration uses in-process server objects whereas X2 and X6 use independent server processes. X7 includes one documented **fail-closed** partial-migration case that preserves safety without restoring availability.

[Architecture](docs/ARCHITECTURE.md) · [Requirements audit](docs/REQUIREMENTS_AUDIT.md) · [Experiments](docs/EXPERIMENTS.md) · [Testing](docs/TESTING.md) · [Demo](docs/DEMO.md)
