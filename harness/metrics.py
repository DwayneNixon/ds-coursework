
import csv
import time
from pathlib import Path
import json
import parameter_server_pb2 as pb

OP_FIELDS = [
    "ts_ns",
    "worker",
    "iteration",
    "op",
    "n_keys",
    "bytes_out",
    "bytes_in",
    "latency_ms",
    "status",
    "retries",
]


def serialized_pull_request_size(target, task, deps=None):
    """Calculate the serialized size of a PullRequest."""

    # Match ParameterClient's dense and sparse target formats.
    if isinstance(target, range):
        request = pb.PullRequest(
            key_range=pb.KeyRange(
                begin=target.start,
                end=target.stop,
            )
        )
    else:
        request = pb.PullRequest(keys=list(target))

    # Include the pull's actual Task ID.
    if task.task_id is not None:
        worker_id, sequence = task.task_id

        request.task_id.CopyFrom(
            pb.TaskId(
                worker_id=worker_id,
                sequence=sequence,
            )
        )

    # Include dependencies, just as ParameterClient does.
    for dependency in deps or []:
        if dependency.task_id is not None:
            worker_id, sequence = dependency.task_id

            request.dependencies.add(
                worker_id=worker_id,
                sequence=sequence,
            )

    return request.ByteSize()




def distributed_payload_sizes(task, op):
    """Measure physical Protobuf payloads, including recovery."""

    if op not in ("push", "pull"):
        raise ValueError(f"Unknown operation: {op}")

    bytes_out = 0
    bytes_in = 0

    for route, subtask in task.tasks:
        attempts = getattr(subtask, "_request_attempts", None)

        if attempts is not None:
            # Includes cache registration and recovery requests.
            bytes_out += sum(len(data) for data in attempts)
            bytes_in += getattr(
                subtask, "_extra_response_bytes", 0
            )

        elif op == "push":
            request_data = subtask._push_request_bytes

            if request_data is None:
                raise RuntimeError(
                    "Serialized Push request is unavailable"
                )

            bytes_out += len(request_data)

        else:
            request_data = getattr(
                subtask, "_pull_request_bytes", None
            )

            if request_data is not None:
                bytes_out += len(request_data)
            else:
                # Compatibility with existing Part B clients.
                if hasattr(route, "keys"):
                    subtarget = list(route.keys)
                else:
                    subtarget = range(route.begin, route.end)

                bytes_out += serialized_pull_request_size(
                    subtarget, subtask
                )

        # Failed RPCs have no successful Protobuf response.
        if subtask._future.exception() is None:
            response = subtask.wait(timeout_ms=0)

            bytes_in += getattr(
                subtask,
                "_response_wire_bytes",
                response.ByteSize(),
            )

    return bytes_out, bytes_in


def measure_operation(
    create_task,
    worker_id,
    iteration,
    op,
    n_keys,
    record_callback=None,
    target=None,
    deps=None,

):
    """Measure RPC latency and serialized Protobuf bytes."""

    start = time.perf_counter_ns()

    status = "OK"
    bytes_out = None
    bytes_in = None

    try:
        task = create_task()
        result = task.wait(timeout_ms=5000)

        # Distributed Tasks contain several gRPC subrequests.
        if hasattr(task, "tasks"):
            bytes_out, bytes_in = distributed_payload_sizes(task, op)
            return task, result
        
        # Calculate serialized outgoing request bytes.
        if op == "push":
            request_data = task._push_request_bytes

            if request_data is None:
                raise RuntimeError(
                    "Serialized push request is unavailable"
                )

            bytes_out = len(request_data)

        elif op == "pull":
            if target is None:
                raise ValueError(
                    "Pull measurement requires target"
                )

            bytes_out = serialized_pull_request_size(
                target,
                task,
                deps,
            )

        else:
            raise ValueError(f"Unknown operation: {op}")

        # The completed RPC returns a Protobuf response.
        bytes_in = getattr(
            task, "_response_wire_bytes", result.ByteSize()
        )

        return task, result

    except Exception as error:
        status = type(error).__name__
        raise

    finally:
        end = time.perf_counter_ns()

        if record_callback is not None:
            record_callback({
                "ts_ns": time.time_ns(),
                "worker": worker_id,
                "iteration": iteration,
                "op": op,
                "n_keys": n_keys,
                "bytes_out": bytes_out,
                "bytes_in": bytes_in,
                "latency_ms": (end - start) / 1_000_000,
                "status": status,
                "retries": 0,
            })

def write_ops_csv(records, output_dir):
    """Write operation records to a CSV file."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / "ops.csv"

    with filename.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=OP_FIELDS,
        )

        writer.writeheader()
        writer.writerows(records)

    return filename


ITERATION_FIELDS = [
    "worker",
    "iteration",
    "t_start",
    "t_end",
    "staleness",
]


def write_iterations_csv(records, output_dir):
    """Write worker iteration records to CSV."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / "iterations.csv"

    with filename.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=ITERATION_FIELDS,
        )

        writer.writeheader()
        writer.writerows(records)

    return filename


# --------------------------------------------------
# Convergence metrics
# --------------------------------------------------

CONVERGENCE_FIELDS = [
    "ts_ns",
    "iteration",
    "dist_to_target",
]


def write_convergence_csv(records, output_dir):
    """Export observer convergence records to CSV."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / "convergence.csv"

    with filename.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=CONVERGENCE_FIELDS,
        )

        writer.writeheader()
        writer.writerows(records)

    return filename


# --------------------------------------------------
# Scenario event metrics
# --------------------------------------------------

EVENT_FIELDS = [
    "ts_ns",
    "action",
    "args",
    "outcome",
]


def write_events_csv(records, output_dir):
    """Export scenario execution records to CSV."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / "events.csv"

    with filename.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=EVENT_FIELDS,
        )

        writer.writeheader()

        for record in records:
            row = record.copy()

            # Store event arguments as JSON inside CSV.
            row["args"] = json.dumps(
                row["args"],
                sort_keys=True,
            )

            writer.writerow(row)

    return filename


# --------------------------------------------------
# Summary statistics
# --------------------------------------------------

def percentile(values, p):
    """Calculate a percentile using linear interpolation."""

    if not values:
        return None

    values = sorted(values)

    position = (len(values) - 1) * p / 100

    lower = int(position)
    upper = min(lower + 1, len(values) - 1)

    fraction = position - lower

    return (
        values[lower] * (1 - fraction)
        + values[upper] * fraction
    )


def rolling_throughput(operations):
    """Calculate completed pushes per second in a 1-second window."""

    push_times = sorted(
        record["ts_ns"]
        for record in operations
        if record["op"] == "push"
        and record["status"] == "OK"
    )

    window_ns = 1_000_000_000
    rates = []
    left = 0

    for right, timestamp in enumerate(push_times):

        # Remove pushes outside the previous second.
        while (
            left <= right
            and push_times[left] <= timestamp - window_ns
        ):
            left += 1

        rates.append(right - left + 1)

    return rates


def total_transmitted_bytes(operations):
    """Sum measured bytes, or return None if data is missing."""

    if not operations:
        return None

    for record in operations:
        if (
            record["bytes_out"] is None
            or record["bytes_in"] is None
        ):
            return None

    return sum(
        record["bytes_out"] + record["bytes_in"]
        for record in operations
    )


def make_summary(operations, convergence_records):
    """Create a summary from collected experiment records."""

    push_latencies = [
        record["latency_ms"]
        for record in operations
        if record["op"] == "push"
        and record["status"] == "OK"
    ]

    pull_latencies = [
        record["latency_ms"]
        for record in operations
        if record["op"] == "pull"
        and record["status"] == "OK"
    ]

    rates = rolling_throughput(operations)

    completed_pushes = len(push_latencies)

    final_distance = None

    if convergence_records:
        final_record = max(
            convergence_records,
            key=lambda record: record["ts_ns"],
        )

        final_distance = final_record["dist_to_target"]

    return {
        "throughput": {
            "window_s": 1,
            "completed_pushes": completed_pushes,
            "peak_pushes_per_s": max(rates, default=0),
        },
        "p50_push_latency_ms": percentile(
            push_latencies, 50
        ),
        "p99_push_latency_ms": percentile(
            push_latencies, 99
        ),
        "p50_pull_latency_ms": percentile(
            pull_latencies, 50
        ),
        "p99_pull_latency_ms": percentile(
            pull_latencies, 99
        ),
        "total_bytes": total_transmitted_bytes(
            operations
        ),
        "final_distance": final_distance,
        "recovery_times": [],
    }


def write_summary_json(summary, output_dir):
    """Export summary statistics as JSON."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / "summary.json"

    with filename.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(summary, file, indent=4)

    return filename
