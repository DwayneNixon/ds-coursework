
"""Part E4.1 — Scale from 2 to 4 servers under live workload."""

import threading
import time
from math import isclose
from statistics import median
from concurrent import futures

import grpc
import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from routing import Partition, KEY_SPACE_END
from experiments.e4_reporting import export_e4_results

QUARTER = 1 << 62

KEYS = [
    10,
    QUARTER + 10,
    2 * QUARTER + 10,
    3 * QUARTER + 10,
]

WORKERS = 4
BEFORE_SECONDS = 6
AFTER_SECONDS = 6


def main():
    # Initially start only two parameter servers.
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    running_servers = [server1, server2]

    manager = ServerManager([
        Partition("p1", 0, 2 * QUARTER, address1),
        Partition("p2", 2 * QUARTER, KEY_SPACE_END, address2),
    ])

    initial_table = manager.get_map()

    service1.set_ownership(address1, initial_table)
    service2.set_ownership(address2, initial_table)

    server1.start()
    server2.start()

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager), grpc_manager
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0
    grpc_manager.start()

    manager_address = f"127.0.0.1:{port}"

    stop_event = threading.Event()
    data_lock = threading.Lock()

    counts = {key: 0 for key in KEYS}
    push_records = []
    errors = []

    clients = []
    threads = []
    verifier = None

    try:
        # ---------------------------------------------
        # 1. Create four workers
        # ---------------------------------------------

        for index in range(WORKERS):
            client = DistributedParameterClient.from_manager(
                manager_address,
                worker_id=2001 + index,
                value_width=2,
            )
            clients.append(client)

        def worker_loop(index):
            client = clients[index]
            iteration = 1

            try:
                while not stop_event.is_set():
                    # Every worker visits all four key regions.
                    key = KEYS[(iteration + index) % len(KEYS)]

                    # Simulate the normal pull-compute-push loop.
                    client.pull([key]).wait(timeout_ms=10000)

                    time.sleep(0.01)

                    start_ns = time.perf_counter_ns()

                    result = client.push(
                        [key],
                        [1.0, 1.0],
                        width=2,
                        iteration=iteration,
                    ).wait(timeout_ms=10000)

                    finish_ns = time.perf_counter_ns()

                    if not result.accepted:
                        raise RuntimeError("Push was rejected")

                    latency_ms = (
                        finish_ns - start_ns
                    ) / 1_000_000

                    with data_lock:
                        counts[key] += 1
                        push_records.append(
                            (finish_ns, latency_ms)
                        )

                    iteration += 1

            except Exception as error:
                with data_lock:
                    errors.append(
                        f"Worker {2001 + index}: {error}"
                    )
                stop_event.set()

        for index in range(WORKERS):
            thread = threading.Thread(
                target=worker_loop,
                args=(index,),
                daemon=True,
            )
            threads.append(thread)
            thread.start()

        print("Four workers started: PASS")
        print("Initial server count: 2")

        # ---------------------------------------------
        # 2. Run initial workload for six seconds
        # ---------------------------------------------

        time.sleep(BEFORE_SECONDS)

        if errors:
            raise RuntimeError(str(errors))

        with data_lock:
            pre_count = sum(counts.values())

        assert pre_count > 0
        assert manager.get_map().epoch == 1

        print("Training on two servers: PASS")
        print(f"Initial successful Pushes: {pre_count}")

        # ---------------------------------------------
        # 3. Add server 3 while training continues
        # ---------------------------------------------

        scaling_start_ns = time.perf_counter_ns()

        server3, service3, address3 = make_server()

        service3.set_ownership(
            address3, manager.get_map()
        )
        server3.start()
        running_servers.append(server3)

        manager.move_range(
            source=service1,
            destination=service3,
            destination_address=address3,
            begin=QUARTER,
            end=2 * QUARTER,
        )

        assert manager.get_map().epoch == 2

        print("Third server joined under load: PASS")

        # ---------------------------------------------
        # 4. Add server 4 from a different source
        # ---------------------------------------------

        server4, service4, address4 = make_server()

        service4.set_ownership(
            address4, manager.get_map()
        )
        server4.start()
        running_servers.append(server4)

        manager.move_range(
            source=service2,
            destination=service4,
            destination_address=address4,
            begin=3 * QUARTER,
            end=KEY_SPACE_END,
        )

        scaling_end_ns = time.perf_counter_ns()

        final_table = manager.get_map()

        assert final_table.epoch == 3

        owners = {
            partition.owner
            for partition in final_table.partitions
        }

        assert owners == {
            address1, address2, address3, address4
        }

        print("Fourth server joined under load: PASS")
        print("Ownership map contains four owners: PASS")

        # ---------------------------------------------
        # 5. Continue training after scaling
        # ---------------------------------------------

        time.sleep(AFTER_SECONDS)

        stop_event.set()

        for thread in threads:
            thread.join(timeout=12)
            assert not thread.is_alive(), (
                "Worker did not finish"
            )

        if errors:
            raise RuntimeError(str(errors))

        with data_lock:
            final_counts = dict(counts)
            records = list(push_records)

        total_pushes = sum(final_counts.values())

        assert total_pushes > pre_count

        print("Training continued after scaling: PASS")
        print(f"Total successful Pushes: {total_pushes}")

        # ---------------------------------------------
        # 6. Verify exact parameter correctness
        # ---------------------------------------------

        # Each successful Push applies:
        #     weight -= 0.1 * 1.0
        #
        # Therefore the expected weight for each key is:
        #     -0.1 * successful_push_count

        expected = {
            key: -0.1 * final_counts[key]
            for key in KEYS
        }

        final_services = [
            service1,
            service3,
            service2,
            service4,
        ]

        for key, service in zip(KEYS, final_services):
            assert key in service.weights

            actual = service.weights[key]

            assert len(actual) == 2

            assert all(
                isclose(
                    value,
                    expected[key],
                    rel_tol=0,
                    abs_tol=1e-6,
                )
                for value in actual
            ), (
                f"Incorrect final weight for key {key}: "
                f"expected {expected[key]}, actual {actual}"
            )

        print("All final parameter values correct: PASS")

        # Verify the final state through the public API.
        verifier = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=3000,
            value_width=2,
        )

        response = verifier.pull(
            KEYS
        ).wait(timeout_ms=10000)

        expected_values = [
            expected[key]
            for key in KEYS
            for _ in range(2)
        ]

        assert len(response.values) == len(expected_values)

        assert all(
            isclose(a, b, rel_tol=0, abs_tol=1e-6)
            for a, b in zip(
                response.values, expected_values
            )
        )

        print("Distributed final Pull correct: PASS")

        # ---------------------------------------------
        # 7. Preliminary performance measurements
        # ---------------------------------------------

        five_seconds_ns = 5_000_000_000

        before = [
            latency
            for timestamp, latency in records
            if (
                scaling_start_ns - five_seconds_ns
                <= timestamp < scaling_start_ns
            )
        ]

        after = [
            latency
            for timestamp, latency in records
            if (
                scaling_end_ns
                <= timestamp < scaling_end_ns + five_seconds_ns
            )
        ]

        assert before and after, (
            "Not enough operations for performance comparison"
        )

        before_throughput = len(before) / 5
        after_throughput = len(after) / 5

        print()
        print("PERFORMANCE SUMMARY")
        print("-------------------")
        print(f"Before scaling: {before_throughput:.2f} Pushes/s")
        print(f"After scaling:  {after_throughput:.2f} Pushes/s")
        print(f"Before median latency: {median(before):.3f} ms")
        print(f"After median latency:  {median(after):.3f} ms")
        print(
            "Scaling transition duration: "
            f"{(scaling_end_ns - scaling_start_ns) / 1e9:.3f} s"
        )

        print()
        
        print("Final weights match the Push-count oracle: PASS")

        # Export measurements only after final correctness passes.
        output_dir = export_e4_results(
            records=records,
            scaling_start_ns=scaling_start_ns,
            scaling_end_ns=scaling_end_ns,
            before_seconds=BEFORE_SECONDS,
            after_seconds=AFTER_SECONDS,
            final_counts=final_counts,
        )

        print(f"E4.2 results saved to: {output_dir}")
        print("E4.2 throughput and latency graphs: PASS")
        print("E4.1 four-server live scaling: PASS")


    finally:
        stop_event.set()

        for thread in threads:
            thread.join(timeout=12)

        if verifier is not None:
            verifier.close()

        for client in clients:
            client.close()

        grpc_manager.stop(0).wait()

        for server in running_servers:
            server.stop(0).wait()


if __name__ == "__main__":
    main()
