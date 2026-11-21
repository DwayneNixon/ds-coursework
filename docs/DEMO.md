# Ten-minute recorded demonstration — evidence-led script

**Handout requirement:** show the cluster running, a server being added and a server being killed **while work continues**. The narration should show actual command output/events rather than presenting a simulated prewritten result as a live run.

Record at 1080p with readable terminal zoom. Use a **clean checkout** with `python -m pip install -r requirements.txt`, a tested Python 3.13 environment, no stale standalone server on 50051, and the final passing Git SHA visible. Capture X5 and X6 in separate terminal sessions; X5 and X6 are not the same deployment topology.

| Timeline | Screen/action | Suggested concise narration and what to show |
|---|---|---|
| 00:00–00:40 | GitHub tree / `README.md` | “We built a Python/gRPC parameter server with asynchronous clients, range routing, consistency, elasticity and crash recovery.” Show the final revision |
| 00:40–01:30 | `ds/`, `harness/`, `experiments/`, architecture diagram | Describe worker→shard→manager, note each primary owns one range and operations across shards are not transactions |
| 01:30–02:20 | `python -m Part_A_Tests.test_server_dependencies` and a short client example | Demonstrate a waiting dependency, then success and exactly-once retry behaviour |
| 02:20–03:00 | X1 `oracle.csv` and X2 scaling plot | Point out analytic correctness and the non-monotonic measured 1–8-server performance |
| 03:00–05:15 | **Live X5:** `python -m experiments x5` | Start workers on two servers, **add servers under load**, show ownership redistribution and continued successful Pushes; show per-key oracle and recorded throughput plot |
| 05:15–07:55 | **Live X6:** `python -m experiments x6` | Show live manager/server processes and worker requests, kill the primary after Pull/before Push, observe heartbeat detection, backup promotion, write-fence/repair and resumed Pushes. Show final oracle error and recovery decomposition |
| 07:55–08:50 | `python -m experiments x7` or existing matrix artifact | Show matrix with eight handled/recovered cases and the **EXPECTED_FAIL_CLOSED** source-crash row; explicitly distinguish safety from availability |
| 08:50–09:35 | X3/X4 comparison plots and CSV | Brief consistency vs straggler throughput and byte/latency optimisations; show D3 batching's 87.5% RPC reduction |
| 09:35–10:00 | GitHub Actions checks, limitations | Summarise scoped guarantees, local-process/in-process differences and known failure boundary |

## Commands (repository root)

```bash
python -m pip install -r requirements.txt
python -m Part_A_Tests.test_server_dependencies
python -m experiments x1
python -m experiments x2
python -m experiments x5
python -m experiments x6
python -m experiments x7
```

**Recording practicality:** X5's default E4 test takes 6 seconds before and after the scaling transition; X6's default setup has a 6.2-second pre-fault and post-fault window. The full X7 matrix launches multiple processes and may exceed the allocated video slot: it is acceptable to show its *previously generated, clearly labelled evidence* instead of attempting the entire suite inside the recording. Do not pretend saved output is currently running.

## Required visual proof

1. At least two active parameter servers before scale-up; **newly added servers** appear in the manager ownership state during real Push traffic.
2. Successful Pushes both before and after adding servers, plus correctness check.
3. Clearly observable primary-server kill (process termination) during active work and the automatic manager detection/promotion/retry sequence.
4. Continued acknowledged updates after failover and comparison to the **independent fixed-gradient oracle**.
5. Explicit mention of the partial postpublication migration case that remains **fail-closed**, and that E4 movement is **in-process** whereas X6 uses real server processes.

## Before uploading

- Watch the complete recording for readable terminal text, correct attribution of each experiment, and ≤10-minute runtime.
- Check the X5 and X6 experiments correspond to the submitted revision and that the actual outputs are visible and accurate.
- Avoid personal folder paths in screenshots; use root-relative paths and short server labels.
- Export an MP4 and verify audio/video synchronization. The video must be recorded by the submitter; this file is a plan, **not** a recorded demo.
