# Distributed Systems Coursework — Part A: Single-Node Parameter Server

University of Edinburgh · Distributed Systems (INFR11022)

This repository implements **Part A (A1–A4)** of the parameter-server coursework: a single Python gRPC server storing vector-valued parameters, with an asynchronous worker client, update-rule configuration, task dependencies, and correctness tests.

**Status:** The standalone Part A functionality and final regression tests were reported passing locally on 15 November 2026. The coursework's checkpoint using the **actual Part B harness** is still to be completed during Part B. This is a single-node implementation; it does not yet provide distributed ownership, persistence, replication, or failover.

> This README describes the existing project built and tested incrementally. It does not require replacing `server.py`, `parameter_client.py`, or `parameter_server.proto` with a different implementation.

## 1. How the system works

```text
Worker A ── ParameterClient ── gRPC ─┐
                                     ├── ParameterServer ── in-memory key → vector
Worker B ── ParameterClient ── gRPC ─┘         │
                                              ├── SGD / registered update rules
                                              ├── task completion tracking
                                              └── lock-protected updates and reads
```

- **Keys:** unsigned 64-bit integers (`uint64`).
- **Values:** vectors of 64-bit floating-point numbers (`double`), with a server-wide width of at least 1; the default is **2**.
- **Missing keys:** read as zero vectors of the configured width.
- **Sparse targets:** explicit keys, such as `[10, 15, 20]`.
- **Dense targets:** half-open ranges, such as `range(10, 13)` representing keys `10, 11, 12`.
- **Push:** supplies a flat gradient vector whose length is `number_of_keys × width`.
- **Pull:** retrieves the current values as a flat vector, together with the requested keys and width.
- **Concurrency:** the server protects its parameter dictionary, deduplication records, and update-rule selection with a shared lock.

### Default update rule

For a key's current weights `w`, incoming gradient `g`, and learning rate `η`:

```text
w_new = w_old - η × g
```

The default learning rate is **0.1**. Each accepted Push applies its update under the server lock, so distinct concurrent updates are not lost. For constant-rate SGD, repeated pushes are equivalent to applying the sum of their gradients (apart from floating-point rounding). **There is no separate gradient batching or accumulation window** in this Part A implementation. Other custom rules may not have the same sum-equivalence property.

### Atomic multi-key updates

A Push targeting multiple keys first computes and validates every resulting vector in temporary storage. The server commits them together only if all calculations succeed. If a registered rule raises an exception, returns the wrong width, or produces invalid numerical values, no partial update is committed.

### Asynchronous tasks and dependencies

`ParameterClient.push(...)` and `.pull(...)` return a `Task` immediately. Use `task.wait(timeout_ms=...)` to obtain the result, and `task.done()` to inspect completion. A timeout **while waiting** does not itself cancel the underlying operation; you may call `wait()` again.

A request can declare `deps=[task_a, task_b]`. The client waits for successful prerequisites before submitting the dependent RPC. The updated Protobuf protocol also carries Task IDs and dependency IDs, and the server enforces those dependencies for identified requests. A failed prerequisite prevents dependent work from executing. Server-side dependency waits are bounded (currently five seconds in the tested design).

**Task IDs and idempotence:** Task IDs have the form `(worker_id, sequence)`. The client allocates Push IDs in the lower sequence range and Pull IDs in the upper range, keeping previously tested Push numbering intact. Successful Push Task IDs are tracked in memory. Repeating the **same** successful Push returns an acknowledgement without applying the gradient twice; reusing its Task ID for a different Push is rejected. Explicit retries of eligible failed Push RPC attempts preserve the original serialized request and ID.

**Worker identity requirement:** assign distinct `worker_id` values to independent logical workers. Two clients constructed with the same worker ID may generate colliding sequences. The server's task history is **not persistent across restarts**, and worker restarts need a stable identity/sequence strategy for stronger fault-tolerance guarantees (a later coursework part).

### Update-rule configuration

- SGD is the default and remains available under the name `"sgd"`.
- `ParameterServer.register_update_rule(name, function)` registers a trusted Python callable directly in the server process.
- `ParameterClient.set_update_rule(name)` selects a previously registered server rule through gRPC.
- `ParameterClient.register_update_rule(name, gradient_scale)` allows clients to register a supported **linear rule** defined as `w_new = w_old + gradient_scale × gradient`. For example, `gradient_scale=0.5` adds half the gradient. Clients do **not** transmit arbitrary executable Python code.
- Rule selection is **global to the single server**, not specific to a worker. A rule change and a Push are ordered by the server's lock.

## 2. Files in the Part A folder

| File | Purpose | Keep? |
|---|---|---|
| `server.py` | `ParameterServer`, gRPC RPC handlers, in-memory storage, validation, locking, update rules, deduplication, dependency tracking, and server entry point. | **Yes** |
| `parameter_client.py` | Public worker API (`ParameterClient`), asynchronous `Task`, dependency scheduling, Task IDs, retries, and remote rule configuration. | **Yes** |
| `parameter_server.proto` | Protocol Buffers contract: Push/Pull, Task IDs, dependencies, ranges, and rule configuration. | **Yes** |
| `parameter_server_pb2.py` | Generated Python Protobuf message classes. | **Needed at runtime**; regenerate after editing `.proto` |
| `parameter_server_pb2_grpc.py` | Generated Python gRPC client stub and server registration helpers. | **Needed at runtime**; regenerate after editing `.proto` |
| `README.md` | Setup, architecture, examples, and test guide (this document). | **Yes** |
| `client.py` | **Unreviewed** in this documentation pass; may be an early example or legacy client. | **Inspect before deleting** |
| `__pycache__/` | Generated Python bytecode cache. | **Safe to delete**; Python recreates it |

The `_pb2.py` and `_pb2_grpc.py` files are **generated**, not manually edited. You may choose either to commit them for convenience or to regenerate them during setup; do not delete them and then expect the code to run without regeneration.

## 3. Installation and running

### Requirements

- Python 3 (the version supported by your installed gRPC packages).
- `grpcio` for runtime gRPC communication.
- `grpcio-tools` to generate Python bindings from the `.proto` file.

From the repository folder in **PowerShell**:

```powershell
python -m pip install grpcio grpcio-tools
```

Generate the client/server bindings after every change to `parameter_server.proto`:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. parameter_server.proto
```

Start the standard single-node server in terminal 1:

```powershell
python server.py
```

The default server listens on **`127.0.0.1:50051`**, with vector width **2** and learning rate **0.1**. Keep this terminal running. Start worker scripts from a second terminal. The standard `server.py` does **not** currently provide a documented command-line `--width` or `--learning-rate` option; width can be configured when constructing `ParameterServer(value_width=...)` in Python.

For tests that use a temporary server bound to `127.0.0.1:0`, the test manages its own port and you **do not** need to launch `server.py` separately.

## 4. Quick-start worker example

Use only `ParameterClient`; worker code does not need to import gRPC or Protobuf directly:

```python
from parameter_client import ParameterClient

client = ParameterClient("127.0.0.1:50051", worker_id=101)
try:
    keys = [10, 11]
    initial = client.pull(keys).wait(timeout_ms=5000)
    print("Before:", list(initial.values))

    update = client.push(keys, [10.0, 20.0, 30.0, 40.0], width=2)
    after = client.pull(keys, deps=[update]).wait(timeout_ms=5000)
    print("After:", list(after.values))
finally:
    client.close()
```

On a fresh server, `Before` is `[0.0, 0.0, 0.0, 0.0]`, and `After` is `[-1.0, -2.0, -3.0, -4.0]`.

### Sparse and dense access

```python
# Explicit (sparse) keys
client.pull([10, 12, 15])

# Contiguous keys 10, 11, 12 (half-open dense range)
client.pull(range(10, 13))

# Two gradient values per key because the default vector width is 2
client.push(range(10, 13), [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], width=2)
```

### Register and activate a supported rule

```python
client.register_update_rule("half_add", gradient_scale=0.5)
client.set_update_rule("half_add")
# Subsequent pushes use w_new = w_old + 0.5 * gradient.
client.set_update_rule("sgd")  # Restore the default rule
```

Choose a new rule name for each registration. Registering a rule does not automatically activate it. Rule changes affect all workers connected to the server.

### Explicit retry after an eligible RPC failure

```python
# If a push fails with UNAVAILABLE or DEADLINE_EXCEEDED:
# retry = client.retry_push(failed_push_task)
# retry.wait(timeout_ms=5000)
```

A retry preserves the original Task ID and request bytes. Do **not** use a brand-new Push as a substitute for a retry when the original outcome is uncertain: doing so would apply an additional logical update if the first Push actually succeeded.

## 5. Test files — complete reference

The repository contains **22 test scripts**. They are grouped below by the behaviour they cover. Most are quick, isolated checks; some are diagnostics for intentionally unsupported or hazardous inputs.

### Basic operations and asynchronous execution

| Test file | What it checks |
|---|---|
| `test_ranges.py` | Sparse key lists versus half-open dense ranges, returned values, and invalid range/list combinations. |
| `test_async.py` | Push/Pull return Task handles, SGD results, `done()`, repeatable `wait()`, and invalid Push errors. |
| `test_timeout.py` | A short `wait()` timeout does not cancel a still-running RPC; a later wait obtains the result. |
| `test_public_api.py` | Two worker clients share one server, use independent worker IDs, and observe each other's updates through the public API. |

### Dependencies and task lifecycle

| Test file | What it checks |
|---|---|
| `test_dependencies.py` | A dependent Pull does not execute before its Push prerequisite; failed prerequisites prevent dependent RPCs. |
| `test_multi_dependencies.py` | Waiting for multiple prerequisites, dependency chains, and failure propagation. |
| `test_dependency_proto.py` | Protobuf round-trips for Task IDs and dependency fields on Push/Pull; old-style messages remain valid. |
| `test_server_dependencies.py` | **Server-enforced** ordering when a dependent RPC arrives before its prerequisite; failed prerequisite handling; new Push/Pull Task IDs. |
| `test_client_shutdown.py` | Client close while an RPC is pending; cancellation and terminal states of dependent tasks. |
| `test_failure_lifecycle.py` | `UNAVAILABLE`/`INVALID_ARGUMENT` prerequisites, propagated dependency failures, repeatable failed waits, and unaffected independent work. |

### Task IDs, retries, and concurrency

| Test file | What it checks |
|---|---|
| `test_task_ids.py` | Same ID + same Push is deduplicated; same ID + different Push is rejected; different IDs execute. |
| `test_auto_task_ids.py` | Automatic sequence allocation, worker-specific IDs, and unique IDs under concurrent pushes. |
| `test_retries.py` | Simulated lost acknowledgement after an applied Push; explicit retry does not apply the gradient twice. |
| `test_concurrent_pushes.py` | 100 distinct concurrent pushes to the same key are all reflected in the final values. |
| `test_concurrent_duplicates.py` | Simultaneous duplicate Push requests apply once; conflicting same-ID requests are rejected. |
| `test_worker_identity.py` | **Diagnostic:** demonstrates that independently created clients with the **same worker ID** can collide. Passing this test documents a limitation, not collision prevention. |

### Rules, validation, and atomicity

| Test file | What it checks |
|---|---|
| `test_update_rules.py` | Local registration/selection, default SGD, alternative additive rule, switching rules, and deduplication. |
| `test_remote_update_rules.py` | One client selects a rule over gRPC; a second client uses it; invalid names and rule restoration. |
| `test_rule_registration.py` | Client-initiated registration of a supported scaled rule; global effect, duplicates, invalid coefficients, and restoration of SGD. |
| `test_validation_edges.py` | Invalid widths/gradient lengths/keys/ranges, uint64 boundaries, empty targets, and rejection of NaN/infinity gradients. |
| `test_atomic_push.py` | A custom rule failing on the second key cannot leave the first key partially updated. |
| `test_rule_safety.py` | Wrong-width/non-finite rule outputs, input-list mutation, all-or-nothing updates, and retry of a previously failed Task ID. |

### Running tests

With a **fresh default server** running on port 50051, you can run the tests that depend on it:

```powershell
python test_ranges.py
python test_async.py
python test_public_api.py
```

Each of these may assume that certain keys start at zero. **Restart the server before each test if needed.** The exact keys and initial-state assumptions are encoded in the scripts.

Most remaining tests create their own isolated temporary gRPC server and can be run directly, for example:

```powershell
python test_server_dependencies.py
python test_rule_registration.py
python test_atomic_push.py
python test_concurrent_pushes.py
python test_concurrent_duplicates.py
```

To run **all** test scripts one at a time in PowerShell:

```powershell
Get-ChildItem -Filter 'test_*.py' | Sort-Object Name | ForEach-Object {
    Write-Host "Running $($_.Name)"
    python $_.FullName
    if ($LASTEXITCODE -ne 0) { throw "Test failed: $($_.Name)" }
}
```

**Caution:** The bulk command does **not** automatically restart the regular server or reset its parameters; tests that assume fresh state may fail if run against a previously used server. Running them individually with their required state is safer. The separate `test_worker_identity.py` intentionally demonstrates a known collision behaviour.

## 6. Errors and limitations

- Invalid key/gradient shapes, unsupported combinations, duplicate keys, incorrect widths, and non-finite input gradients are rejected with gRPC `INVALID_ARGUMENT`.
- Unexpected errors in trusted update-rule execution are mapped to gRPC `INTERNAL`; the atomic commit logic prevents a partial Push on those failures.
- The original failed RPC retains its gRPC error (for example `UNAVAILABLE`); dependent tasks report a dependency failure. The client-side `DependencyFailedError` is a Python exception, while direct server dependency failures use a gRPC status.
- `Task.wait(timeout_ms=...)` may raise a client-side `TaskWaitTimeout`. This does not by itself cancel the RPC.
- Server state and deduplication records are **in memory only**, so data is lost on server restart.
- Worker IDs must be distinct and their sequences unique throughout a live server's deduplication history. The server does not automatically allocate worker identities.
- A bounded wait is used for unresolved server-side dependencies; raw gRPC requests with missing prerequisites can time out.
- The current implementation is a **single server**. Ownership routing (`NOT_OWNER`), replicated persistence, elastic membership, consistency modes and communication codecs belong to later parts.
- Dense ranges are half-open; extremely large ranges are not recommended. A dedicated range-size guard was not confirmed in the original reviewed source.
- Client registration supports a **linear gradient-scaling rule family**, not arbitrary remotely submitted executable Python functions.
- The real Part B harness demonstration required by the coursework is a separate pending integration checkpoint.

## 7. Suggested repository cleanup

**Do not delete the working implementation or the tests just to shorten the file list.** Tests are useful regression evidence for future Parts B–F.

- Safe to remove locally: `__pycache__/` and `.pyc` files (Python recreates them).
- **Inspect first:** `client.py`. It may be a legacy demonstration from the original stages, but its contents have not been reviewed; do not delete it until you've checked it isn't imported or needed.
- Keep: `server.py`, `parameter_client.py`, `parameter_server.proto`, `README.md`, and all tests.
- Generated files: keep `parameter_server_pb2.py` and `parameter_server_pb2_grpc.py` available for runtime or explicitly regenerate them during installation.

Recommended `.gitignore` entries:

```gitignore
__pycache__/
*.py[cod]
.venv/
venv/
.pytest_cache/
```

In VS Code's Source Control view, **`U` means untracked by Git**, not unused or unnecessary.

## 8. Next step: Part B

Part A's standalone implementation and targeted tests have passed in the user's local environment. The next step is to build the **Part B worker/workload and correctness harness**, then demonstrate A1–A4 through that harness. Reuse `ParameterClient` and `ParameterServer`; do not create a parallel parameter-server implementation.
