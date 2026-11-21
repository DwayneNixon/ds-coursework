
import grpc
import parameter_server_pb2 as pb

from harness.local_servers import make_server
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


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
        # Same logical worker, communicating with two servers.
        client1 = ParameterClient(address1, worker_id=1201)
        client2 = ParameterClient(address2, worker_id=1201)

        # Iteration 7 updates both servers.
        client1.push(
            [10], [10.0, 20.0],
            width=2, iteration=7,
        ).wait(timeout_ms=5000)

        client2.push(
            [50], [30.0, 40.0],
            width=2, iteration=7,
        ).wait(timeout_ms=5000)

        assert service1.range_clocks["p1"].clock_for_key(10) == {
            1201: 7
        }

        assert service2.range_clocks["p2"].clock_for_key(50) == {
            1201: 7
        }

        print("Explicit iteration clocks: PASS")

        # Iteration 8 updates only Server 1.
        client1.push(
            [11], [1.0, 2.0],
            width=2, iteration=8,
        ).wait(timeout_ms=5000)

        # Iteration 9 updates both servers again.
        task1 = client1.push(
            [10], [1.0, 2.0],
            width=2, iteration=9,
        )

        task2 = client2.push(
            [50], [3.0, 4.0],
            width=2, iteration=9,
        )

        task1.wait(timeout_ms=5000)
        task2.wait(timeout_ms=5000)

        # The Task sequences differ between servers.
        assert task1.task_id == (1201, 3)
        assert task2.task_id == (1201, 2)

        # But their logical training iterations agree.
        assert service1.range_clocks["p1"].clock_for_key(10) == {
            1201: 9
        }

        assert service2.range_clocks["p2"].clock_for_key(50) == {
            1201: 9
        }

        print("Iterations independent of Task sequences: PASS")

        # Replaying the same request must not advance clocks.
        old_clock = service1.range_clocks["p1"].snapshot()
        old_weights = {
            key: values.copy()
            for key, values in service1.weights.items()
        }

        duplicate = pb.PushRequest()
        duplicate.ParseFromString(task1._push_request_bytes)

        response = client1.stub.Push(duplicate, timeout=5)

        assert response.accepted
        assert service1.range_clocks["p1"].snapshot() == old_clock
        assert service1.weights == old_weights

        print("Iteration-aware duplicate safety: PASS")

        # Explicit iteration zero must be rejected.
        bad_request = pb.PushRequest(
            keys=[12],
            gradients=[1.0, 2.0],
            width=2,
            task_id=pb.TaskId(
                worker_id=1201,
                sequence=999,
            ),
            iteration=0,
        )

        try:
            client1.stub.Push(bad_request, timeout=5)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError(
                "Expected invalid iteration rejection"
            )

        assert 12 not in service1.weights

        print("Invalid iteration rejected: PASS")

        # Existing callers without explicit iterations still work.
        legacy_task = client1.push(
            [12], [2.0, 4.0], width=2
        )

        legacy_task.wait(timeout_ms=5000)

        assert legacy_task.task_id == (1201, 4)
        assert service1.range_clocks["p1"].clock_for_key(12) == {
            1201: 4
        }

        print("Legacy Task sequence fallback: PASS")
        print("C3.3B1 logical iteration tracking: PASS")

    finally:
        if client1 is not None:
            client1.close()
        if client2 is not None:
            client2.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
