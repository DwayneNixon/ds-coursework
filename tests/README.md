# Tests (98 scripts)

Tests are grouped **by coursework requirement**, not scattered across the repository root. Each test keeps its existing assertions; shared local gRPC fixtures live in `harness/local_servers.py`.

| Package | Count | Focus |
|---|---:|---|
| `tests.part_a` | 23 | API, async Task, validation, dependencies, update rules and concurrency |
| `tests.part_b` | 17 | Workload generator, million-key logical model, oracle, scenarios and B5 metrics |
| `tests.part_c` | 25 | Routing, ownership manager, range clocks and consistency |
| `tests.part_d` | 8 | Key-list caching, compression and Pull batching |
| `tests.part_e` | 9 | Range migration, dynamic membership and elasticity |
| `tests.part_f` | 16 | Heartbeats, replica ACK, promotion, repair and failure matrix |

Run everything in a clean, isolated process per test:

```bash
python -m scripts.run_all_regressions
```

Or run only a part or one focused regression:

```bash
python -m scripts.run_all_regressions --part C
python -m tests.part_b.test_b2_million_key_distributed
python -m unittest -v tests.part_f.test_f31_replica_promotion
```

The runner starts a fresh standalone server for legacy port-50051 tests and writes any failures to ignored `results/audit_regressions/`. A separate GitHub Actions workflow exercises the entire suite. For experiment instructions, see [docs/EXPERIMENTS.md](../docs/EXPERIMENTS.md).
