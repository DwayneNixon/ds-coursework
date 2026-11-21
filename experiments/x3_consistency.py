
import csv
import math
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc
import manager_pb2_grpc as manager_grpc
import random
from harness.local_servers import make_server
from harness.local_servers import clock_counter

from server_manager import ServerManager
from manager_rpc import ManagerService
from routing import Partition, KEY_SPACE_END
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient
from harness.worker import run_worker
from harness.metrics import make_summary


WORKERS = [1601, 1602]
ITERATIONS = 24
NUM_KEYS = 8
WIDTH = 2

SAMPLE_EVERY = 4
STRAGGLER_MS = 60

# First run one seed to verify the experiment.
# After it passes, we can use (42, 43, 44).
SEEDS = (42, 43, 44)

MODES = [
    ("sequential", "sequential", 0),
    ("bounded_1", "bounded", 1),
    ("bounded_2", "bounded", 2),
    ("bounded_4", "bounded", 4),
    ("eventual", "eventual", 0),
]


def distance_to_target(values, targets):
    expected = [
        component
        for key in range(NUM_KEYS)
        for component in targets[key]
    ]

    return math.sqrt(sum(
        (actual - wanted) ** 2
        for actual, wanted in zip(values, expected)
    ))


def run_case(label, mode, tau, slow_ms, seed):
    # Start two fresh parameter servers.
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    manager = ServerManager([
        Partition("p1", 0, 4, address1),
        Partition("p2", 4, KEY_SPACE_END, address2),
    ])

    manager.configure_consistency(
        workers=WORKERS,
        mode=mode,
        tau=tau,
    )

    table = manager.get_map()

    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    server1.start()
    server2.start()

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager),
        grpc_manager,
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0

    grpc_manager.start()
    manager_address = f"127.0.0.1:{port}"

    clients = []
    observer = None

    operations = []
    iterations = []
    staleness = []
    convergence = []
    errors = []
    finish_times = {}

    lock = threading.Lock()

    try:
        # Unlike C4.4, use meaningful target-difference
        # gradients to study convergence.
        targets = {
            key: [float(key + 1), -float(key + 1)]
            for key in range(NUM_KEYS)
        }

        observer = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=1699,
            value_width=WIDTH,
        )

        for worker_id in WORKERS:
            distributed = DistributedParameterClient.from_manager(
                manager_address,
                worker_id=worker_id,
                value_width=WIDTH,
            )

            client = CoordinatedClient(
                distributed,
                manager_address,
                worker_id=worker_id,
                total_iterations=ITERATIONS,
            )

            clients.append(client)

            # Observe the vector clocks of each worker's Pull.
            original_pull = client.pull

            def monitored_pull(
                target, deps=None,
                c=client, original=original_pull
            ):
                iteration = c.iteration
                task = original(target, deps=deps)

                def completed(future):
                    try:
                        response = future.result()

                        other = next(
                            worker for worker in WORKERS
                            if worker != c.worker_id
                        )

                        seen = min(
                            clock_counter(response, 0, other),
                            clock_counter(
                                response, NUM_KEYS - 1, other
                            ),
                        )

                        lag = max(0, iteration - 1 - seen)

                        with lock:
                            staleness.append(lag)

                    except Exception as error:
                        with lock:
                            errors.append(error)

                task._future.add_done_callback(completed)
                return task

            client.pull = monitored_pull

        initial_values = observer.pull(
            range(NUM_KEYS)
        ).wait(timeout_ms=5000).values

        initial_distance = distance_to_target(
            initial_values, targets
        )

        start = time.perf_counter()

        convergence.append({
            "mode": label,
            "straggler_ms": slow_ms,
            "seed": seed,
            "completed_pushes": 0,
            "elapsed_s": 0.0,
            "distance": initial_distance,
        })

        def record_operation(record):
            with lock:
                operations.append(record)

        def record_iteration(record):
            # The worker calls this after its Push succeeds.
            with lock:
                iterations.append(record)
                count = len(iterations)

                if record["iteration"] == ITERATIONS:
                    finish_times[record["worker"]] = (
                        time.perf_counter() - start
                    )

            # Sample convergence periodically.
            if count % SAMPLE_EVERY == 0:
                values = observer.pull(
                    range(NUM_KEYS)
                ).wait(timeout_ms=5000).values

                distance = distance_to_target(
                    values, targets
                )

                with lock:
                    convergence.append({
                        "mode": label,
                        "straggler_ms": slow_ms,
                        "seed": seed,
                        "completed_pushes": count,
                        "elapsed_s": time.perf_counter() - start,
                        "distance": distance,
                    })

        with futures.ThreadPoolExecutor(
            max_workers=len(WORKERS)
        ) as executor:

            jobs = []

            for client in clients:
                worker_id = client.worker_id

                compute_ms = (
                    slow_ms if worker_id == WORKERS[1] else 0
                )

                jobs.append(executor.submit(
                    run_worker,
                    client=client,
                    worker_id=worker_id,
                    iterations=ITERATIONS,
                    num_keys=NUM_KEYS,
                    keys_per_iter=NUM_KEYS,
                    width=WIDTH,
                    targets=targets,
                    seed=seed,
                    compute_ms=compute_ms,
                    gradient_mode="target_diff",
                    on_iteration=client.iteration_finished,
                    on_operation=record_operation,
                    on_iteration_record=record_iteration,
                ))

            for job in jobs:
                job.result(timeout=45)

        elapsed = time.perf_counter() - start

        assert not errors, errors

        expected_pushes = len(WORKERS) * ITERATIONS

        assert len(iterations) == expected_pushes
        assert len(staleness) == expected_pushes
        assert len(operations) == expected_pushes * 2

        assert all(
            record["status"] == "OK"
            for record in operations
        )

        assert manager.consistency_gate.snapshot() == {
            worker_id: ITERATIONS for worker_id in WORKERS
        }

        maximum_lag = max(staleness)

        if mode == "sequential":
            assert maximum_lag == 0

        if mode == "bounded":
            assert maximum_lag <= tau

        final_values = observer.pull(
            range(NUM_KEYS)
        ).wait(timeout_ms=5000).values

        final_distance = distance_to_target(
            final_values, targets
        )

        assert math.isfinite(final_distance)

        convergence.append({
            "mode": label,
            "straggler_ms": slow_ms,
            "seed": seed,
            "completed_pushes": expected_pushes,
            "elapsed_s": elapsed,
            "distance": final_distance,
        })

        # Verify that all logical updates reached both partitions.
        final_clock = {
            worker_id: ITERATIONS for worker_id in WORKERS
        }

        assert (
            service1.range_clocks["p1"].clock_for_key(0)
            == final_clock
        )

        assert (
            service2.range_clocks["p2"].clock_for_key(7)
            == final_clock
        )

        metrics = make_summary(operations, [])

        assert metrics["total_bytes"] is not None

        summary = {
            "mode": label,
            "straggler_ms": slow_ms,
            "seed": seed,
            "completed_pushes": expected_pushes,
            "elapsed_s": elapsed,
            "pushes_per_s": expected_pushes / elapsed,
            "max_lag": maximum_lag,
            "initial_distance": initial_distance,
            "final_distance": final_distance,
            "p50_push_ms": metrics["p50_push_latency_ms"],
            "p99_push_ms": metrics["p99_push_latency_ms"],
            "total_bytes": metrics["total_bytes"],
            "fast_worker_finish_s": finish_times[WORKERS[0]],
            "slow_worker_finish_s": finish_times[WORKERS[1]],
        }

        print(
            f"{label:12} | slow={slow_ms:2d}ms | "
            f"rate={summary['pushes_per_s']:7.2f}/s | "
            f"lag={maximum_lag:2d} | "
            f"distance={final_distance:.6f}"
        )

        return summary, convergence

    finally:
        for client in clients:
            client.close()

        if observer is not None:
            observer.close()

        grpc_manager.stop(0).wait()
        server1.stop(0).wait()
        server2.stop(0).wait()


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=list(rows[0])
        )
        writer.writeheader()
        writer.writerows(rows)


def main():
    summaries = []
    all_convergence = []

    for seed in SEEDS:
        cases = [
            (slow_ms, label, mode, tau)
            for slow_ms in (0, STRAGGLER_MS)
            for label, mode, tau in MODES
        ]

        # Change experiment order between trials.
        random.Random(seed).shuffle(cases)

        for slow_ms, label, mode, tau in cases:
            summary, trace = run_case(
                label, mode, tau, slow_ms, seed
            )

            summaries.append(summary)
            all_convergence.extend(trace)

    write_csv(
        "results/c5_evaluation/summary.csv",
        summaries,
    )

    write_csv(
        "results/c5_evaluation/convergence.csv",
        all_convergence,
    )

    print()
    print("C5 benchmark scenarios completed: PASS")
    print("Consistency and clock checks: PASS")
    print("Performance and convergence CSV exports: PASS")
    print("Output: results/c5_evaluation/summary.csv")
    print("Output: results/c5_evaluation/convergence.csv")


if __name__ == "__main__":
    main()
