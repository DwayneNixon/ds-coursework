# Testing and reproducibility

## Environment

Use Python 3.13 and run all commands from the repository root. Install versions compatible with the committed `*_pb2.py` and `*_pb2_grpc.py` files:

```bash
python -m pip install -r requirements.txt
python -m compileall -q -x '/__pycache__/' .
```

Expected runtime uses localhost gRPC and spawns multiple Python processes in F/X2; permit ephemeral local ports. The code requires no CUDA/GPU, and a clean checkout already contains generated Protobuf bindings.

## Tests

`python -m scripts.run_all_regressions` discovers all `tests/part_a/test_*.py` through `tests/part_f/test_*.py`, launches **each test script in an isolated Python subprocess** and supplies a freshly started standalone `server.py` on port 50051 where a legacy test needs it. Results are written to `results/audit_regressions/summary.json` with a raw log per test. A test's nonzero exit/timeout is a failure; do not count skipped tests as pass.

For part-level isolation:

```bash
python -m scripts.run_all_regressions --part A
python -m scripts.run_all_regressions --part B
python -m scripts.run_all_regressions --part C
python -m scripts.run_all_regressions --part D
python -m scripts.run_all_regressions --part E
python -m scripts.run_all_regressions --part F
```

Do not run the full regression runner concurrently with other tests requiring a *fresh* server on port 50051; launch after the smoke experiment has finished.

The audited implementation previously passed 98/98 test scripts. After this organisation change, require a **fresh 98/98 pass against the final branch revision**; do not rely on the earlier run.

## Automated verification

- `.github/workflows/audit-smoke.yml` runs imports/syntax, targeted A/B/C/E/F tests, X1/X2, D3/X4, live X6 and X7. It uploads raw evidence even if an experiment fails. Successful CI for a prior commit does not prove current head.
- `.github/workflows/audit-full.yml` runs all discovered A–F test scripts (98). It uploads the individual script logs and `summary.json`. The suite is slower because E4 and F tests spawn processes and exercise timeouts.
- Use the GitHub Actions run for **the exact PR head SHA** when reporting PASS or reproducing performance values.

## Focused commands

```bash
python -m tests.part_a.test_request_limits
python -m tests.part_a.test_server_dependencies
python -m tests.part_a.test_rule_registration
python -m tests.part_b.test_b2_worker_dense_range
python -m tests.part_b.test_b2_million_key_distributed
python -m tests.part_c.test_c1_worker_harness
python -m tests.part_e.test_e12_source_transfer
python -m unittest -v tests.part_f.test_f31_replica_promotion
python -m unittest -v tests.part_f.test_f32_replication_repair
```

The legacy B4/B5 integration smoke needs a *fresh* server started in another terminal:

```bash
python server.py
# separate terminal:
python -m experiments b4
```

Individual server-backed tests may assume virgin zero-valued keys and unused Task IDs; restart the standalone process between runs. The F3/F4 tests start their own manager and server processes.

## What to record for a reproducible submission

Keep: SHA, Python version, dependency versions, configs and RNG seeds; number/size of servers and workers; workload mode and key count; oracle pass/tolerance; throughput window definitions; p50/p99 latencies; serialized request/response byte totals; recovery detection/promotion/resume phases; raw events, ops, iteration and convergence data; failed or fail-closed cases and why.

Avoid: treating a wrapper import as a unit test; counting a fail-closed case as recovered; quoting one-run timings as universal; claiming the manager and E4 transfer are fully process-independent; reproducing results with a dirty standalone server and reused worker IDs.

## Final audit checklist

- [ ] Both CI workflows pass for the final source revision
- [ ] All X1–X7 experiment configs, plots and raw evidence are preserved and labelled
- [ ] Per-part tests and README paths match the organised repository
- [ ] No stale bytecode, local paths, personal data, temporary logs or duplicated sources
- [ ] Report ≤8 pages and recorded demonstration ≤10 minutes
- [ ] Test a clean checkout with the user-facing installation and commands in under 10 minutes for the basic quick start
