
import time
import grpc

from concurrent import futures
from math import isclose

import manager_pb2_grpc as manager_grpc

from Part_C_Tests.test_c2_server_ownership import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService

from routing import Partition, KEY_SPACE_END
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient

from harness.worker import run_worker
from harness.oracle import fixed_gradient


WORKERS = [1401, 1402]
ITERATIONS = 3
NUM_KEYS = 8
WIDTH = 2


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

    port = grpc_manager.add_insecure_port(
        "127.0.0.1:0"
    )

    assert port != 0

    grpc_manager.start()
    manager_address = f"127.0.0.1:{port}"

    clients = []
    records = []

    try:
        for worker_id in WORKERS:
            distributed = (
                DistributedParameterClient.from_manager(
                    manager_address,
                    worker_id=worker_id,
                    value_width=WIDTH,
                )
            )

            client = CoordinatedClient(
                distributed,
                manager_address,
                worker_id=worker_id,
                total_iterations=ITERATIONS,
            )

            clients.append(client)

        # The same target array is shared by both workers.
        # Fixed gradients make the final result deterministic.
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
                worker_id = client.worker_id

                job = executor.submit(
                    run_worker,
                    client=client,
                    worker_id=worker_id,
                    iterations=ITERATIONS,
                    num_keys=NUM_KEYS,
                    keys_per_iter=NUM_KEYS,
                    width=WIDTH,
                    targets=targets,
                    seed=42,
                    compute_ms=(
                        150 if worker_id == 1402 else 0
                    ),
                    gradient_mode="fixed",
                    on_iteration=client.iteration_finished,
                    on_iteration_record=records.append,
                )

                jobs.append(job)

            for job in jobs:
                job.result(timeout=30)

        elapsed = time.perf_counter() - start

        # Both workers must have completed all iterations.
        progress = manager.consistency_gate.snapshot()

        assert progress == {
            worker_id: ITERATIONS
            for worker_id in WORKERS
        }

        assert len(records) == len(WORKERS) * ITERATIONS

        print(f"{mode} manager progress: PASS")

        # Check that every recorded iteration obeyed the
        # consistency mode's required completion barrier.
        by_worker_iteration = {
            (record["worker"], record["iteration"]): record
            for record in records
        }

        gate = manager.consistency_gate

        for record in records:
            required = gate.required_iteration(
                record["iteration"]
            )

            if required == 0:
                continue

            for worker_id in WORKERS:
                prerequisite = by_worker_iteration[
                    (worker_id, required)
                ]

                assert (
                    prerequisite["t_end"]
                    <= record["t_start"]
                ), (
                    f"Barrier violated for worker "
                    f"{record['worker']}, "
                    f"iteration {record['iteration']}"
                )

        print(f"{mode} iteration barriers: PASS")

        # All workers updated all keys in all iterations.
        # The final weights should equal the sum of the
        # fixed gradients multiplied by -0.1.
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

        assert len(result.values) == len(expected)

        assert all(
            isclose(
                actual,
                wanted,
                rel_tol=0,
                abs_tol=1e-8,
            )
            for actual, wanted in zip(
                result.values, expected
            )
        )

        print(f"{mode} SGD oracle: PASS")

        # The server clocks must reflect the final logical
        # iteration of both workers.
        expected_clock = {
            worker_id: ITERATIONS
            for worker_id in WORKERS
        }

        assert (
            service1.range_clocks["p1"].clock_for_key(0)
            == expected_clock
        )

        assert (
            service2.range_clocks["p2"].clock_for_key(7)
            == expected_clock
        )

        print(f"{mode} vector clock progress: PASS")
        print(f"{mode} elapsed: {elapsed:.3f} seconds")

        return elapsed

    finally:
        for client in clients:
            client.close()

        grpc_manager.stop(0).wait()
        server1.stop(0).wait()
        server2.stop(0).wait()


def main():
    print("\n=== Sequential consistency ===")
    sequential = run_mode("sequential")

    print("\n=== Bounded delay, tau=1 ===")
    bounded = run_mode("bounded", tau=1)

    print("\n=== Eventual consistency ===")
    eventual = run_mode("eventual")

    print("\nMeasured completion times:")
    print(f"Sequential: {sequential:.3f}s")
    print(f"Bounded tau=1: {bounded:.3f}s")
    print(f"Eventual: {eventual:.3f}s")

    print("C4.2B distributed worker integration: PASS")


if __name__ == "__main__":
    main()
