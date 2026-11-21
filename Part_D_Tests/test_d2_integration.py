
import parameter_server_pb2 as pb

from Part_C_Tests.test_c2_server_ownership import make_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    client = None

    try:
        boundary = 1 << 63

        table = RoutingTable([
            Partition("p1", 0, boundary, address1),
            Partition("p2", boundary, KEY_SPACE_END, address2),
        ])

        service1.set_ownership(address1, table)
        service2.set_ownership(address2, table)

        server1.start()
        server2.start()

        client = DistributedParameterClient(
            table,
            worker_id=2401,
            value_width=2,
            key_caching=True,
            value_compression=True,
        )

        keys = (
            [(1 << 60) + 2 * i for i in range(32)]
            + [boundary + (1 << 60) + 2 * i for i in range(32)]
        )

        gradients = [1.0, 2.0] * len(keys)

        # 1. Initial Pull should return decoded zeros.
        initial = client.pull(keys)
        initial_result = initial.wait(timeout_ms=5000)

        assert list(initial_result.values) == [0.0] * 128

        for _, subtask in initial.tasks:
            physical_response = subtask.wait(timeout_ms=0)

            assert (
                subtask._response_wire_bytes
                < physical_response.ByteSize()
            )

        print("Compressed Pull decoding: PASS")

        # 2. Push gradients using compressed physical requests.
        push_task = client.push(
            keys,
            gradients,
            width=2,
            iteration=1,
        )

        push_task.wait(timeout_ms=5000)

        assert len(push_task.tasks) == 2

        for _, subtask in push_task.tasks:
            request = pb.PushRequest()
            request.ParseFromString(
                subtask._push_request_bytes
            )

            assert request.gradients_compressed
            assert len(request.gradients) == 0
            assert len(request.compressed_gradients) > 0

        print("Compressed distributed Push: PASS")

        # 3. Pull updated parameters and verify decoding.
        final_task = client.pull(keys)
        final = final_task.wait(timeout_ms=5000)

        expected = [-0.1, -0.2] * len(keys)

        assert len(final.values) == len(expected)

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(final.values, expected)
        )

        print("Compressed Pull numerical correctness: PASS")

        # 4. Check that clocks still advance correctly.
        assert (
            service1.range_clocks["p1"].clock_for_key(keys[0])
            == {2401: 1}
        )

        assert (
            service2.range_clocks["p2"].clock_for_key(keys[-1])
            == {2401: 1}
        )

        print("Compressed Push vector clocks: PASS")

        # 5. Replaying an identical compressed Push must
        # not apply its gradients a second time.
        for route, subtask in push_task.tasks:
            original = pb.PushRequest()
            original.ParseFromString(
                subtask._push_request_bytes
            )

            response = client.clients[route.owner].stub.Push(
                original,
                timeout=5,
            )

            assert response.accepted

        replay_values = client.pull(keys).wait(
            timeout_ms=5000
        ).values

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(replay_values, expected)
        )

        print("Compressed Push replay is idempotent: PASS")
        print("D2.2 compression integration: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
