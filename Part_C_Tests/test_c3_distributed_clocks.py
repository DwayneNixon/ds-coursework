
from math import isclose

from Part_C_Tests.test_c2_server_ownership import make_server

from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def clock_at(response, partition_id, key):
    """Extract the clock for one key from a logical result."""

    matches = []

    for segment in response.clocks:
        end = (
            KEY_SPACE_END
            if segment.end_is_keyspace_end
            else segment.end
        )

        if (
            segment.partition_id == partition_id
            and segment.begin <= key < end
        ):
            matches.append({
                entry.worker_id: entry.counter
                for entry in segment.entries
            })

    assert len(matches) == 1, (
        f"Expected one clock for {partition_id}, key {key}; "
        f"found {len(matches)}"
    )

    return matches[0]


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    table = RoutingTable([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ])

    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    server1.start()
    server2.start()

    client = None

    try:
        client = DistributedParameterClient(
            table,
            worker_id=1301,
            value_width=2,
        )

        # 1. One iteration spanning two owners.
        push_result = client.push(
            [10, 50],
            [10.0, 20.0, 30.0, 40.0],
            width=2,
            iteration=7,
        ).wait(timeout_ms=5000)

        assert push_result.accepted
        assert clock_at(push_result, "p1", 10) == {1301: 7}
        assert clock_at(push_result, "p2", 50) == {1301: 7}

        print("Distributed iteration forwarding: PASS")
        print("Push clock aggregation: PASS")

        # 2. Iteration 8 touches only Server 1.
        client.push(
            [11], [2.0, 4.0],
            width=2,
            iteration=8,
        ).wait(timeout_ms=5000)

        # Iteration 9 touches both servers.
        task = client.push(
            [10, 50],
            [1.0, 2.0, 3.0, 4.0],
            width=2,
            iteration=9,
        )

        result = task.wait(timeout_ms=5000)

        sequences = {
            route.owner: subtask.task_id[1]
            for route, subtask in task.tasks
        }

        assert sequences[address1] == 3
        assert sequences[address2] == 2

        assert clock_at(result, "p1", 10) == {1301: 9}
        assert clock_at(result, "p2", 50) == {1301: 9}

        print("Logical iterations independent of shard IDs: PASS")

        # 3. A distributed Pull also exposes both clocks.
        pull_result = client.pull(
            [10, 50]
        ).wait(timeout_ms=5000)

        expected = [-1.1, -2.2, -3.3, -4.4]

        assert len(pull_result.values) == len(expected)

        assert all(
            isclose(a, b, rel_tol=0, abs_tol=1e-9)
            for a, b in zip(pull_result.values, expected)
        )

        assert clock_at(pull_result, "p1", 10) == {1301: 9}
        assert clock_at(pull_result, "p2", 50) == {1301: 9}

        print("Distributed Pull values and clocks: PASS")

        # 4. Existing callers can omit iteration.
        legacy = client.push(
            [12], [5.0, 5.0],
            width=2,
        )

        legacy_result = legacy.wait(timeout_ms=5000)

        assert legacy.task_id is None
        assert clock_at(legacy_result, "p1", 12) == {1301: 4}

        print("Distributed legacy fallback: PASS")

        # 5. Invalid metadata is rejected before dispatch.
        before = list(
            client.pull([10, 50]).wait(
                timeout_ms=5000
            ).values
        )

        try:
            client.push(
                [10, 50],
                [0.0, 0.0, 0.0, 0.0],
                width=2,
                iteration=0,
            )
        except ValueError:
            pass
        else:
            raise AssertionError(
                "Invalid iteration should be rejected"
            )

        after = list(
            client.pull([10, 50]).wait(
                timeout_ms=5000
            ).values
        )

        assert before == after

        print("Invalid iteration safely rejected: PASS")
        print("C3.3B2 distributed clocks: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
