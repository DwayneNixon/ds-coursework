"""Part F, F5.1: reproducible evidence and fixed-gradient correctness helpers.

All times used for throughput are monotonic seconds from one driver process.
This module intentionally does not modify the parameter server or gRPC API.
"""

from bisect import bisect_right
import csv
import json
import math
from pathlib import Path
from statistics import median
import threading

OPS_FIELDS = ("ts_ns", "worker", "iteration", "op", "n_keys",
              "bytes_out", "bytes_in", "latency_ms", "status", "retries")
ITERATION_FIELDS = ("worker", "iteration", "t_start", "t_end", "staleness")
CONVERGENCE_FIELDS = ("ts_ns", "iteration", "dist_to_target")
EVENT_FIELDS = ("ts_ns", "action", "args", "outcome")


def fixed_gradient_oracle(completed_iterations, gradient_function, *,
                          width=2, learning_rate=0.1):
    """Independent offline SGD oracle.

    completed_iterations: iterable of (worker_id, iteration, tuple_of_keys).
    The supplied gradient_function is the existing harness's B3 generator.
    All completed logical pushes are included ONCE; never pass retry attempts.
    Returns {key: [expected floating-point vector]}.
    """
    if not callable(gradient_function):
        raise TypeError("A deterministic gradient function is required")
    if type(width) is not int or width < 1:
        raise ValueError("Invalid vector width")
    if not isinstance(learning_rate, (float, int)) or not math.isfinite(learning_rate):
        raise ValueError("Invalid learning rate")
    contributions = {}
    for worker, iteration, keys in completed_iterations:
        if type(worker) is not int or type(iteration) is not int or iteration < 1:
            raise ValueError("Invalid worker or iteration in oracle inputs")
        if len(set(keys)) != len(keys):
            raise ValueError("One logical Push cannot repeat a key")
        for key in keys:
            per_component = contributions.setdefault(key, [[] for _ in range(width)])
            for component in range(width):
                value = gradient_function(worker, iteration, key, component)
                if not math.isfinite(value):
                    raise ValueError("Non-finite fixed gradient")
                per_component[component].append(float(value))
    return {key: [-learning_rate * math.fsum(values) for values in per_component]
            for key, per_component in contributions.items()}


def compare_weights(expected, actual, *, abs_tol=1e-9, rel_tol=1e-10):
    """Return evidence of equality, with maximum error and mismatches."""
    if set(expected) != set(actual):
        raise ValueError("Oracle and actual key sets differ")
    errors = []
    max_error = 0.0
    for key, reference in sorted(expected.items()):
        values = actual[key]
        if len(values) != len(reference):
            raise ValueError(f"Width mismatch at key {key}")
        for index, (truth, observed) in enumerate(zip(reference, values)):
            error = abs(observed - truth)
            max_error = max(error, max_error)
            if not math.isclose(observed, truth, abs_tol=abs_tol, rel_tol=rel_tol):
                errors.append({"key": key, "component": index,
                               "expected": truth, "actual": observed,
                               "absolute_error": error})
    return {"matches": not errors, "max_absolute_error": max_error,
            "mismatches": errors}


def throughput_recovery(completion_times_s, fault_time_s, *,
                        sample_step_s=0.1, last_time_s=None):
    """Calculate the coursework's 1-second sliding-window throughput metric.

    The five baseline samples count [t-1,t) for each of five disjoint
    seconds ending at fault injection. Postfault samples are on a fixed
    time grid; resolution is reported, so the measured time is quantized.

    IMPORTANT: the literal 1-second sliding window contains pre-fault Pushes
    for the first second after a fault. Both the literal metric and a more
    conservative postfault-only measure are reported to expose this bias.
    """
    if type(fault_time_s) not in (int, float) or not math.isfinite(fault_time_s):
        raise ValueError("Invalid fault timestamp")
    if type(sample_step_s) not in (int, float) or not math.isfinite(sample_step_s) or sample_step_s <= 0:
        raise ValueError("Invalid sampling interval")
    times = sorted(float(t) for t in completion_times_s)
    if any(not math.isfinite(t) for t in times):
        raise ValueError("Invalid Push completion timestamp")
    if last_time_s is None:
        last_time_s = times[-1] if times else fault_time_s
    if last_time_s <= fault_time_s:
        raise ValueError("Observation must extend beyond the fault")

    def count(left, right):
        # Half-open interval: [left, right).
        return bisect_right(times, math.nextafter(right, -math.inf)) - bisect_right(
            times, math.nextafter(left, -math.inf))

    baseline = [count(fault_time_s - 5 + i, fault_time_s - 4 + i)
                for i in range(5)]
    pre = float(median(baseline))
    threshold = 0.9 * pre
    samples = []
    recovered = None
    postfault = None
    n_steps = int((last_time_s - fault_time_s) / sample_step_s)
    for i in range(1, n_steps + 1):
        now = fault_time_s + i * sample_step_s
        rate = count(now - 1, now)
        # Strictly post-fault Push completions in the 1-second interval.
        post_rate = count(max(now - 1, fault_time_s), now)
        samples.append({"relative_s": round(now - fault_time_s, 6),
                        "completed_pushes_per_s": rate,
                        "post_fault_pushes_per_s": post_rate})
        if pre > 0 and recovered is None and rate >= threshold:
            recovered = now - fault_time_s
        if pre > 0 and postfault is None and post_rate >= threshold:
            postfault = now - fault_time_s
    return {"baseline_pushes_per_second": baseline,
            "theta_pre": pre, "threshold_90pct": threshold,
            "sample_step_s": sample_step_s,
            "throughput_recovery_s": recovered,
            "postfault_only_recovery_s": postfault,
            "measurement_note": (
                "1-s sliding windows before t_fault+1 include pre-fault pushes; "
                "postfault_only_recovery_s is an additional conservative diagnostic."
            ), "samples": samples}


def recovery_decomposition(fault_time_s, recovery, first_resumed_push_s=None):
    """F3 detection/promotion/resumption durations, never invent missing phases.

    T_resume is measured from promotion to the first new completed Push;
    this is distinct from the official throughput recovery metric.
    """
    detected = recovery.get("detected_at_s")
    promoted = recovery.get("promoted_at_s")
    ready = recovery.get("ready_at_s")
    if any(x is None for x in (detected, promoted, ready)):
        return None
    if not (fault_time_s <= detected <= promoted <= ready):
        raise ValueError("Recovery timestamps are not ordered")
    if first_resumed_push_s is not None and first_resumed_push_s < promoted:
        first_resumed_push_s = None
    return {
        "T_detect_s": detected - fault_time_s,
        "T_promote_s": promoted - detected,
        "T_resume_to_first_push_s": (
            first_resumed_push_s - promoted
            if first_resumed_push_s is not None else None
        ),
        "controller_ready_s": ready - fault_time_s,
        "T_rec_first_push_s": (
            first_resumed_push_s - fault_time_s
            if first_resumed_push_s is not None else None
        ),
    }


class ExperimentEvidence:
    """Thread-safe event and operation collector; CSV output follows §5.7."""

    def __init__(self):
        self._lock = threading.Lock()
        self.operations = []
        self.iterations = []
        self.events = []
        self.convergence = []
        self.completed = []
        self.push_times_s = []

    def add_op(self, *, monotonic_end_s=None, **record):
        row = {field: record.get(field, "") for field in OPS_FIELDS}
        with self._lock:
            self.operations.append(row)
            if row["op"] == "push" and row["status"] == "ok":
                if monotonic_end_s is None:
                    raise ValueError("Successful Push requires monotonic completion time")
                self.push_times_s.append(monotonic_end_s)

    def add_iteration(self, worker, iteration, t_start, t_end, keys):
        with self._lock:
            self.iterations.append({"worker": worker, "iteration": iteration,
                                    "t_start": t_start, "t_end": t_end,
                                    "staleness": ""})
            self.completed.append((worker, iteration, tuple(keys)))

    def add_event(self, ts_ns, action, args, outcome):
        with self._lock:
            self.events.append({"ts_ns": ts_ns, "action": action,
                                "args": json.dumps(args, sort_keys=True),
                                "outcome": outcome})

    @staticmethod
    def _write_csv(path, fields, rows):
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    def write(self, output_dir, summary):
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        with self._lock:
            ops, iterations, events, convergence = (
                list(self.operations), list(self.iterations), list(self.events),
                list(self.convergence))
        self._write_csv(output / "ops.csv", OPS_FIELDS, ops)
        self._write_csv(output / "iterations.csv", ITERATION_FIELDS, iterations)
        self._write_csv(output / "convergence.csv", CONVERGENCE_FIELDS, convergence)
        self._write_csv(output / "events.csv", EVENT_FIELDS, events)
        with (output / "summary.json").open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True, allow_nan=False)
        return output
