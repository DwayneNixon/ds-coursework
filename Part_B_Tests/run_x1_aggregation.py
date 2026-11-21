"""X1: synchronous multiworker target-difference aggregation versus §5.5 oracle.

Run from the repository root:
    python -m Part_B_Tests.run_x1_aggregation

Uses the existing Part B worker, target oracle, convergence observer and
B5 metrics. All workers Pull before any Push in each round; the next
round begins only after all Push acknowledgements.
"""

import argparse
import csv
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import TargetVector
from harness.observer import ConvergenceObserver
from harness.metrics import (
    make_summary, write_ops_csv, write_iterations_csv,
    write_convergence_csv, write_events_csv, write_summary_json,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "x1_aggregation.json"


def run(config, output_dir):
    workers = list(config["workers"]["ids"])
    iterations = config["workers"]["iterations"]
    num_keys = config["model"]["num_keys"]
    width = config["model"]["value_width"]
    eta = config["learning_rate"]
    seed = config["seed"]
    compute_ms = config["workers"]["compute_ms"]
    m = len(workers)

    if (config.get("mode") != "sequential"
            or config.get("experiment") != "X1_analytical_aggregation"):
        raise ValueError("X1 requires the sequential analytic-oracle configuration")
    if (m < 2 or len(set(workers)) != m or iterations < 1
            or num_keys < 1 or width < 1):
        raise ValueError("Invalid worker, iteration or model configuration")
    if not (0 < eta * m < 1):
        raise ValueError("Stable X1 requires 0 < learning_rate * workers < 1")
    if compute_ms < 0:
        raise ValueError("Negative simulated compute delay")

    service = ParameterServer(value_width=width)
    service.learning_rate = eta
    server = grpc.server(ThreadPoolExecutor(max_workers=max(8, m * 2 + 2)))
    pb_grpc.add_ParameterServerServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    if port <= 0:
        raise RuntimeError("Cannot allocate X1 gRPC port")
    server.start()
    address = f"127.0.0.1:{port}"

    clients = [ParameterClient(address, worker_id=wid) for wid in workers]
    observer_client = ParameterClient(address, worker_id=100000)
    targets = TargetVector(num_keys, width, seed)
    observer = ConvergenceObserver(
        observer_client, targets, num_keys, width
    )
    operations = []
    iteration_records = []
    lock = threading.Lock()
    before_push = threading.Barrier(m)
    current_round = [0]

    def sample_committed_round():
        # Executed once, *before* releasing workers into the next round.
        current_round[0] += 1
        observer.sample(current_round[0])

    after_push = threading.Barrier(m, action=sample_committed_round)

    def record_operation(record):
        with lock:
            operations.append(record)

    def record_iteration(record):
        with lock:
            iteration_records.append(record)

    try:
        initial = observer.sample(0)["dist_to_target"]

        def worker(wid, client):
            run_worker(
                client=client, worker_id=wid, iterations=iterations,
                num_keys=num_keys, keys_per_iter=num_keys, width=width,
                targets=targets, seed=seed, compute_ms=compute_ms,
                gradient_mode="target_diff",
                before_push_barrier=before_push,
                after_push_barrier=after_push,
                on_operation=record_operation,
                on_iteration_record=record_iteration,
            )

        with ThreadPoolExecutor(max_workers=m) as pool:
            futures = [pool.submit(worker, wid, client)
                       for wid, client in zip(workers, clients)]
            for future in futures:
                future.result()

        records = sorted(observer.records, key=lambda r: r["iteration"])
        if len(records) != iterations + 1 or current_round[0] != iterations:
            raise AssertionError("Missing synchronized round samples")
        if len(operations) != 2 * m * iterations:
            raise AssertionError("Missing measured Pull/Push operations")
        if any(record["status"] != "OK" for record in operations):
            raise AssertionError("A measured RPC failed during X1")

        factor = abs(1.0 - eta * m)
        oracle = []
        for record in records:
            t = record["iteration"]
            expected = initial * factor ** t
            measured = record["dist_to_target"]
            absolute_error = abs(expected - measured)
            tolerance = 1e-10 * max(1.0, expected)
            if absolute_error > tolerance:
                raise AssertionError(
                    f"X1 oracle mismatch at round {t}: "
                    f"measured={measured}, formula={expected}"
                )
            oracle.append({
                "iteration": t,
                "expected_distance": expected,
                "measured_distance": measured,
                "absolute_error": absolute_error,
            })

        output_path = Path(output_dir)
        if not output_path.is_absolute():
            output_path = ROOT / output_path
        output_path.mkdir(parents=True, exist_ok=True)
        write_ops_csv(operations, output_path)
        write_iterations_csv(iteration_records, output_path)
        write_convergence_csv(records, output_path)
        write_events_csv([], output_path)
        with (output_path / "oracle.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            fields = list(oracle[0])
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(oracle)
        summary = make_summary(operations, records)
        summary.update({
            "experiment": "X1_analytical_aggregation",
            "seed": seed, "mode": "sequential",
            "num_workers": m, "worker_ids": workers,
            "iterations_per_worker": iterations,
            "num_keys": num_keys, "value_width": width,
            "gradient_mode": "target_diff", "learning_rate": eta,
            "theoretical_contraction_factor": factor,
            "initial_distance": initial,
            "expected_final_distance": oracle[-1]["expected_distance"],
            "observed_final_distance": oracle[-1]["measured_distance"],
            "maximum_absolute_oracle_error":
                max(row["absolute_error"] for row in oracle),
            "oracle_matches": True,
        })
        write_summary_json(summary, output_path)
        return summary
    finally:
        for client in clients:
            client.close()
        observer_client.close()
        server.stop(0).wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    summary = run(config, args.output or config["output_dir"])
    print("X1 sequential multiworker analytical oracle: PASS")
    print("Expected distance:", summary["expected_final_distance"])
    print("Observed distance:", summary["observed_final_distance"])
    print("Max absolute error:", summary["maximum_absolute_oracle_error"])


if __name__ == "__main__":
    main()
