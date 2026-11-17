
from math import isclose

from Part_C_Tests.test_c1_two_servers import start_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END

from harness.worker import run_worker


def main():
    server1, address1 = start_server()
    server2, address2 = start_server()

    client = None

    try:
        # Divide eight logical keys over two servers.
        table = RoutingTable([
            Partition("p1", 0, 4, address1),
            Partition("p2", 4, KEY_SPACE_END, address2),
        ])

        client = DistributedParameterClient(
            table, worker_id=941, value_width=2
        )

        targets = {
            key: [float(key + 1), -float(key + 1)]
            for key in range(8)
        }

        operation_records = []
        iteration_records = []

        # Run the existing Part B worker unchanged.
        run_worker(
            client=client,
            worker_id=941,
            iterations=3,
            num_keys=8,
            keys_per_iter=8,
            width=2,
            targets=targets,
            seed=42,
            compute_ms=0,
            on_operation=operation_records.append,
            on_iteration_record=iteration_records.append,
        )

        assert len(iteration_records) == 3
        print("Distributed worker iterations: PASS")

        # Three iterations should produce three logical
        # Pulls and three logical Pushes.
        assert len(operation_records) == 6

        assert [r["op"] for r in operation_records] == [
            "pull", "push",
            "pull", "push",
            "pull", "push",
        ]

        assert all(
            r["status"] == "OK"
            for r in operation_records
        )

        print("Logical operation records: PASS")

        # Both servers' serialized messages must be counted.
        assert all(
            isinstance(r["bytes_out"], int)
            and r["bytes_out"] > 0
            and isinstance(r["bytes_in"], int)
            and r["bytes_in"] >= 0
            for r in operation_records
        )

        total_bytes = sum(
            r["bytes_out"] + r["bytes_in"]
            for r in operation_records
        )

        print("Serialized payload bytes: PASS")
        print("Total measured bytes:", total_bytes)

        # Verify mathematical correctness.
        # One worker, SGD learning rate = 0.1,
        # zero initialization and three iterations.
        actual = list(
            client.pull(range(8)).wait(
                timeout_ms=5000
            ).values
        )

        contraction = 1 - (1 - 0.1) ** 3

        expected = [
            contraction * value
            for key in range(8)
            for value in targets[key]
        ]

        assert len(actual) == len(expected)

        assert all(
            isclose(a, b, rel_tol=0, abs_tol=1e-9)
            for a, b in zip(actual, expected)
        )

        print("Final weights oracle: PASS")
        print("C1.7 Part B harness integration: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0)
        server2.stop(0)


if __name__ == "__main__":
    main()
