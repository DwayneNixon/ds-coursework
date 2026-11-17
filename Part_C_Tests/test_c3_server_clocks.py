
import grpc
import parameter_server_pb2 as pb

from Part_C_Tests.test_c2_server_ownership import make_server
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    # Server 1 deliberately owns two adjacent partitions.
    table = RoutingTable([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, 80, address1),
        Partition("p3", 80, KEY_SPACE_END, address2),
    ])

    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    server1.start()
    server2.start()

    client1 = None
    client2 = None

    try:
        client1 = ParameterClient(address1, worker_id=1001)
        client2 = ParameterClient(address2, worker_id=1002)

        assert service1.range_clocks["p1"].count() == 1
        assert service1.range_clocks["p2"].count() == 1
        assert service2.range_clocks["p3"].count() == 1

        print("Per-partition clock initialization: PASS")

        # 1. Dense Push crosses two partitions on Server 1.
        dense_task = client1.push(
            range(38, 42),
            [10, 20, 30, 40, 50, 60, 70, 80],
            width=2,
        )

        dense_task.wait(timeout_ms=5000)

        assert service1.range_clocks["p1"].snapshot() == [
            (0, 38, {}),
            (38, 40, {1001: 1}),
        ]

        assert service1.range_clocks["p2"].snapshot() == [
            (40, 42, {1001: 1}),
            (42, 80, {}),
        ]

        print("Dense Push clock updates: PASS")

        # 2. Replaying the identical Task ID must not
        # apply another update or advance either clock.
        before_p1 = service1.range_clocks["p1"].snapshot()
        before_p2 = service1.range_clocks["p2"].snapshot()
        before_weights = dict(
            (key, values.copy())
            for key, values in service1.weights.items()
        )

        original_request = pb.PushRequest()
        original_request.ParseFromString(
            dense_task._push_request_bytes
        )

        response = client1.stub.Push(
            original_request, timeout=5
        )

        assert response.accepted
        assert service1.range_clocks["p1"].snapshot() == before_p1
        assert service1.range_clocks["p2"].snapshot() == before_p2
        assert service1.weights == before_weights

        print("Duplicate Push clock idempotence: PASS")

        # 3. Sparse Push updates only selected keys.
        client1.push(
            [39, 41],
            [1, 2, 3, 4],
            width=2,
        ).wait(timeout_ms=5000)

        p1 = service1.range_clocks["p1"]
        p2 = service1.range_clocks["p2"]

        assert p1.clock_for_key(38) == {1001: 1}
        assert p1.clock_for_key(39) == {1001: 2}
        assert p2.clock_for_key(40) == {1001: 1}
        assert p2.clock_for_key(41) == {1001: 2}

        print("Sparse Push clock splitting: PASS")

        # 4. Wrong-owner Push changes neither clock nor weights.
        before_clock = p1.snapshot()
        before_weights = {
            key: values.copy()
            for key, values in service1.weights.items()
        }

        try:
            client1.push(
                [39, 85],
                [1, 2, 3, 4],
                width=2,
            ).wait(timeout_ms=5000)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert error.details().startswith("NOT_OWNER:")
        else:
            raise AssertionError("Expected NOT_OWNER")

        assert p1.snapshot() == before_clock
        assert service1.weights == before_weights

        print("Rejected Push preserves clocks: PASS")

        # 5. A separate server maintains independent clocks.
        client2.push(
            [85],
            [5, 5],
            width=2,
        ).wait(timeout_ms=5000)

        assert service2.range_clocks["p3"].clock_for_key(85) == {
            1002: 1
        }

        print("Independent server clocks: PASS")
        print("C3.2 server clock integration: PASS")

    finally:
        if client1 is not None:
            client1.close()

        if client2 is not None:
            client2.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
