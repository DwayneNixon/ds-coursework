
from math import isclose

from Part_C_Tests.test_c1_two_servers import start_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def check_values(actual, expected):
    assert len(actual) == len(expected)

    assert all(
        isclose(a, b, abs_tol=1e-9)
        for a, b in zip(actual, expected)
    )


def main():
    server1, address1 = start_server()
    server2, address2 = start_server()

    client = None

    try:
        table = RoutingTable([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address2),
        ])

        client = DistributedParameterClient(
            table, worker_id=921, value_width=2
        )

        # One logical Push spanning two servers.
        task = client.push(
            range(38, 42),
            [10, 20, 30, 40, 50, 60, 70, 80],
            width=2,
        )

        task.wait(timeout_ms=5000)
        assert task.done()

        print("Transparent distributed Push: PASS")

        # One logical dense Pull.
        values = client.pull(
            range(38, 42)
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [-1, -2, -3, -4, -5, -6, -7, -8],
        )

        print("Transparent dense Pull: PASS")

        # Sparse Pull, preserving original order.
        values = client.pull(
            [41, 38, 40, 39]
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [-7, -8, -1, -2, -5, -6, -3, -4],
        )

        print("Transparent sparse Pull: PASS")

        # Sparse Push spanning two servers.
        client.push(
            [41, 38],
            [1, 2, 3, 4],
            width=2,
        ).wait(timeout_ms=5000)

        values = client.pull(
            [41, 38]
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [-7.1, -8.2, -1.3, -2.4],
        )

        print("Transparent sparse Push: PASS")

        # Invalid payload must be rejected.
        try:
            client.push(
                range(38, 42),
                [1, 2],
                width=2,
            )
            raise AssertionError("Expected validation failure")
        except ValueError:
            pass

        print("Pre-dispatch validation: PASS")

        print("C1.5 transparent client: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0)
        server2.stop(0)


if __name__ == "__main__":
    main()
