
from harness.local_servers import start_server

from parameter_client import ParameterClient

from routing import (
    Partition,
    RoutingTable,
    KEY_SPACE_END,
    split_push,
    merge_pull,
)


def main():
    server1, address1 = start_server()
    server2, address2 = start_server()
    clients = {}

    try:
        clients[address1] = ParameterClient(
            address1, worker_id=911
        )
        clients[address2] = ParameterClient(
            address2, worker_id=912
        )

        table = RoutingTable([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address2),
        ])

        # One logical Push spanning both servers.
        target = range(38, 42)
        gradients = [
            10.0, 20.0,
            30.0, 40.0,
            50.0, 60.0,
            70.0, 80.0,
        ]

        routes = table.split_range(38, 42)
        chunks = split_push(target, gradients, routes, 2)

        # Dispatch subrequests without waiting between them.
        push_tasks = []

        for route in routes:
            client = clients[route.owner]
            subrange = range(route.begin, route.end)

            task = client.push(
                subrange,
                chunks[route],
                width=2,
            )
            push_tasks.append(task)

        # One logical Push completes after all subrequests.
        for task in push_tasks:
            task.wait(timeout_ms=5000)

        print("Distributed Push: PASS")

        # Pull the same range from both servers.
        pull_tasks = []

        for route in routes:
            client = clients[route.owner]
            subrange = range(route.begin, route.end)

            task = client.pull(subrange)
            pull_tasks.append((route, task))

        replies = {}

        for route, task in pull_tasks:
            replies[route] = list(
                task.wait(timeout_ms=5000).values
            )

        merged = merge_pull(target, routes, replies, 2)

        expected = [
            -1.0, -2.0,
            -3.0, -4.0,
            -5.0, -6.0,
            -7.0, -8.0,
        ]

        assert len(merged) == len(expected)
        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(merged, expected)
        )

        print("Distributed dense Pull: PASS")

        # Verify sparse routing preserves requested order.
        sparse_keys = [41, 38, 40, 39]
        sparse_routes = table.split_keys(sparse_keys)

        sparse_tasks = []

        for route in sparse_routes:
            task = clients[route.owner].pull(
                list(route.keys)
            )
            sparse_tasks.append((route, task))

        sparse_replies = {
            route: list(task.wait(timeout_ms=5000).values)
            for route, task in sparse_tasks
        }

        sparse_values = merge_pull(
            sparse_keys, sparse_routes, sparse_replies, 2
        )

        sparse_expected = [
            -7.0, -8.0,
            -1.0, -2.0,
            -5.0, -6.0,
            -3.0, -4.0,
        ]

        assert len(sparse_values) == len(sparse_expected)
        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(sparse_values, sparse_expected)
        )

        print("Distributed sparse Pull: PASS")

        # Ensure the wrong server was never updated.
        wrong1 = list(
            clients[address1].pull([40]).wait(
                timeout_ms=5000
            ).values
        )

        wrong2 = list(
            clients[address2].pull([38]).wait(
                timeout_ms=5000
            ).values
        )

        assert wrong1 == [0.0, 0.0]
        assert wrong2 == [0.0, 0.0]

        print("Correct key placement: PASS")
        print("C1 distributed routing integration: PASS")

    finally:
        for client in clients.values():
            client.close()

        server1.stop(0)
        server2.stop(0)


if __name__ == "__main__":
    main()
