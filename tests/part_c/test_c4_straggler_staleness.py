from harness.local_servers import clock_counter

import threading
import time
import grpc

from concurrent import futures
from math import isclose

import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService

from routing import Partition, KEY_SPACE_END
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient

from harness.worker import run_worker
from harness.oracle import fixed_gradient


WORKERS = [1501, 1502]
ITERATIONS = 8
NUM_KEYS = 8
WIDTH = 2
SLOW_WORKER_MS = 200



def run_mode(mode, tau=0):
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
    samples = []
    errors = []
    sample_lock = threading.Lock()

    try:
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

            # Observe Pull results without changing the worker.
            original_pull = client.pull

            def monitored_pull(
                target, deps=None,
                c=client, original=original_pull
            ):
                iteration = c.iteration
                task = original(target, deps=deps)

                def completed(future):
                    try:
                        result = future.result()

                        other_worker = next(
                            w for w in WORKERS
                            if w != c.worker_id
                        )

                        # Our dense workload accesses both
                        # server partitions on every iteration.
                        seen = min(
                            clock_counter(
                                result, 0, other_worker
                            ),
                            clock_counter(
                                result, 7, other_worker
                            ),
                        )

                        lag = max(0, iteration - 1 - seen)

                        with sample_lock:
                            samples.append((
                                c.worker_id,
                                iteration,
                                seen,
                                lag,
                            ))

                    except Exception as error:
                        with sample_lock:
                            errors.append(error)

                task._future.add_done_callback(completed)
                return task

            client.pull = monitored_pull

        targets = {
            key: [0.0] * WIDTH
            for key in range(NUM_KEYS)
        }

        start = time.perf_counter()

        with futures.ThreadPoolExecutor(
            max_workers=len(WORKERS)
        ) as executor:

            jobs = []

            for client in clients:
                job = executor.submit(
                    run_worker,
                    client=client,
                    worker_id=client.worker_id,
                    iterations=ITERATIONS,
                    num_keys=NUM_KEYS,
                    keys_per_iter=NUM_KEYS,
                    width=WIDTH,
                    targets=targets,
                    seed=42,
                    compute_ms=(
                        SLOW_WORKER_MS
                        if client.worker_id == 1502
                        else 0
                    ),
                    gradient_mode="fixed",
                    on_iteration=client.iteration_finished,
                )

                jobs.append(job)

            for job in jobs:
                job.result(timeout=30)

        elapsed = time.perf_counter() - start

        assert not errors, errors
        assert len(samples) == len(WORKERS) * ITERATIONS

        progress = manager.consistency_gate.snapshot()

        assert progress == {
            worker_id: ITERATIONS
            for worker_id in WORKERS
        }

        print("All worker iterations completed: PASS")

        # Verify actual staleness observed at Pull time.
        maximum_lag = max(
            sample[3] for sample in samples
        )

        if mode == "sequential":
            assert maximum_lag == 0

        elif mode == "bounded":
            assert maximum_lag <= tau

        # Eventual consistency has no finite lag bound.

        print("Observed clock staleness policy: PASS")

        # Fixed gradients allow an independent final oracle.
        # They avoid making the final result depend on
        # which worker happened to compute first.
        result = clients[0].pull(
            range(NUM_KEYS)
        ).wait(timeout_ms=5000)

        expected = []

        for key in range(NUM_KEYS):
            for component in range(WIDTH):
                gradient_sum = sum(
                    fixed_gradient(
                        worker_id,
                        iteration,
                        key,
                        component,
                    )
                    for worker_id in WORKERS
                    for iteration in range(
                        1, ITERATIONS + 1
                    )
                )

                expected.append(-0.1 * gradient_sum)

        assert all(
            isclose(
                a, b, rel_tol=0, abs_tol=1e-8
            )
            for a, b in zip(result.values, expected)
        )

        print("Fixed-gradient correctness oracle: PASS")

        # Confirm both server partitions received the
        # final logical iteration of both workers.
        final_clock = {
            worker_id: ITERATIONS
            for worker_id in WORKERS
        }

        assert (
            service1.range_clocks["p1"].clock_for_key(0)
            == final_clock
        )

        assert (
            service2.range_clocks["p2"].clock_for_key(7)
            == final_clock
        )

        print("Final vector clock consistency: PASS")

        print(f"Maximum observed lag: {maximum_lag}")
        print(f"Completion time: {elapsed:.3f}s")

        return maximum_lag, elapsed

    finally:
        for client in clients:
            client.close()

        grpc_manager.stop(0).wait()
        server1.stop(0).wait()
        server2.stop(0).wait()


def main():
    results = {}

    for name, mode, tau in [
        ("Sequential", "sequential", 0),
        ("Bounded tau=2", "bounded", 2),
        ("Eventual", "eventual", 0),
    ]:
        print(f"\n=== {name} ===")
        results[name] = run_mode(mode, tau)

    print("\nSummary:")
    print("Mode             | Max lag | Time (s)")

    for name, (lag, elapsed) in results.items():
        print(
            f"{name:16} | "
            f"{lag:7d} | "
            f"{elapsed:8.3f}"
        )

    print("C4.4 straggler and staleness test: PASS")


if __name__ == "__main__":
    main()
