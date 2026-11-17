
import parameter_server_pb2 as pb

from Part_C_Tests.test_c2_server_ownership import make_server
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def decode_clocks(response):
    """Turn Protobuf clock segments into readable tuples."""
    result = []

    for segment in response.clocks:
        end = (
            KEY_SPACE_END
            if segment.end_is_keyspace_end
            else segment.end
        )

        clock = {
            entry.worker_id: entry.counter
            for entry in segment.entries
        }

        result.append((
            segment.partition_id,
            segment.begin,
            end,
            clock,
        ))

    return result


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

    client1 = None
    client2 = None

    try:
        client1 = ParameterClient(address1, worker_id=1101)
        client2 = ParameterClient(address2, worker_id=1102)

        # 1. Push returns the clock updated by that operation.
        push_task = client1.push(
            range(38, 40),
            [10.0, 20.0, 30.0, 40.0],
            width=2,
        )

        push_result = push_task.wait(timeout_ms=5000)

        assert push_result.accepted
        assert decode_clocks(push_result) == [
            ("p1", 0, 38, {}),
            ("p1", 38, 40, {1101: 1}),
        ]

        print("Push response contains updated clocks: PASS")

        # 2. Pull returns weights and corresponding clocks.
        pull_result = client1.pull(
            [39, 38]
        ).wait(timeout_ms=5000)

        assert list(pull_result.values) == [
            -3.0, -4.0, -1.0, -2.0
        ]

        assert decode_clocks(pull_result) == [
            ("p1", 0, 38, {}),
            ("p1", 38, 40, {1101: 1}),
        ]

        print("Pull response contains weights and clocks: PASS")

        # 3. A second Push splits the clock interval further.
        client1.push(
            [39], [2.0, 4.0], width=2
        ).wait(timeout_ms=5000)

        result = client1.pull(
            [38, 39]
        ).wait(timeout_ms=5000)

        assert decode_clocks(result) == [
            ("p1", 0, 38, {}),
            ("p1", 38, 39, {1101: 1}),
            ("p1", 39, 40, {1101: 2}),
        ]

        print("Clock splitting visible through gRPC: PASS")

        # 4. Replaying the first Push does not update it twice.
        before = service1.range_clocks["p1"].snapshot()
        before_weights = {
            key: values.copy()
            for key, values in service1.weights.items()
        }

        original = pb.PushRequest()
        original.ParseFromString(
            push_task._push_request_bytes
        )

        duplicate_result = client1.stub.Push(
            original, timeout=5
        )

        assert duplicate_result.accepted
        assert service1.range_clocks["p1"].snapshot() == before
        assert service1.weights == before_weights

        print("Duplicate Push preserves clock state: PASS")

        # 5. Check the largest valid uint64 key.
        last_key = KEY_SPACE_END - 1

        response = client2.push(
            [last_key],
            [10.0, 20.0],
            width=2,
        ).wait(timeout_ms=5000)

        assert response.clocks[-1].end_is_keyspace_end

        assert decode_clocks(response)[-1] == (
            "p2",
            last_key,
            KEY_SPACE_END,
            {1102: 1},
        )

        print("Maximum key-space clock boundary: PASS")
        print("C3.3A clock response protocol: PASS")

    finally:
        if client1 is not None:
            client1.close()

        if client2 is not None:
            client2.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
