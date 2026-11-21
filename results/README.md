# Reproducible experiment evidence

The directories below are the **canonical retained outputs**. We removed duplicate historical runs and generated scratch files; these files remain because the experiments or final report require their measured values.

| Result | Contents | What it proves |
|---|---|---|
| `x1_aggregation/` | Independent analytic oracle, per-round convergence, operations and summary | Correct synchronous multiworker aggregation |
| `x2_scaling/` | `trials.csv` (24 seeded trials), `scaling.csv`, scaling plot and per-trial raw metrics | Measured 1–8-server throughput, latency, bytes and convergence |
| `c5_evaluation/` | 30 consistency/straggler scenarios and convergence plots | X3 sequential vs bounded τ vs eventual modes |
| `d1_cache_comparison/`, `d2_comparison/` | Three-trial reference logs for cached keys, compression and combined optimisation | Raw inputs needed to regenerate the X4 comparison |
| `d3_benchmark/`, `x4_comparison/` | Batched Pull measurements, median comparison CSV and graph | X4 bytes, RPC reduction and p99 trade-offs |
| `e4_elasticity/` | One selected 2→4-server trial with operations, events, timeseries and plots | X5 in-process elasticity and final oracle |
| `x6_failover/` | Primary-kill event, successful Pushes, offline oracle and recovery timings | X6 confirmed single primary crash |
| `x7_fault_matrix/` | Nine case logs, failure matrix and one explicitly fail-closed outcome | X7 injected-fault safety/recovery scope |
| `c3_clock_growth.csv` | Range-clock fragmentation data | C3 overhead characteristics |
| `ci_evidence_manifest.json` | Final workflow source SHA, run ID, verification and SHA256 file digests | Provenance for the retained evidence |

**File policy:** these are the measured results used for the report, not test fixtures to delete. Each X2 trial has its own substantive metrics; repeated empty `events.csv` files were removed because X2 injects no faults. Scratch output from tests (`b5_*`, `audit_regressions/`) is ignored and regenerated on demand.

The byte metric measures serialized Protobuf application payloads, not full network bytes. X5's range migration runs with in-process servers, whereas X2 and X6 use separate Python server processes. X7 includes an intentionally **fail-closed** case, not a successful automatic recovery.

Run and regenerate results with [the experiment guide](../docs/EXPERIMENTS.md). The final report identifies its experiment CI revision.
