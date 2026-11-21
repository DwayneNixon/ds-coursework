
import grpc
import parameter_server_pb2 as pb

from parameter_client import ParameterClient
from harness.local_servers import make_server
from routing import Partition, RoutingTable, KEY_SPACE_END


def main():
    server, service, address = make_server()
    client = None

    try:
        # Enable ownership and vector-clock tracking.
        table = RoutingTable([
            Partition("p1", 0, KEY_SPACE_END, address),
        ])

        service.set_ownership(address, table)
        server.start()

        client = ParameterClient(address, worker_id=2102)

        keys = [10, 70]
        cache_id = b"C" * 16

        # 1. Register the sparse key list.
        client.stub.Pull(
            pb.PullRequest(
                keys=keys,
                key_cache_id=cache_id,
            ),
            timeout=5,
        )

        assert service.key_cache[cache_id] == tuple(keys)
        print("Key registration: PASS")

        # 2. Send a cached Push without explicit keys.
        request = pb.PushRequest(
            gradients=[1.0, 2.0, 3.0, 4.0],
            width=2,
            key_cache_id=cache_id,
            use_cached_keys=True,
            iteration=7,
        )

        request.task_id.CopyFrom(
            pb.TaskId(worker_id=2101, sequence=1)
        )

        response = client.stub.Push(request, timeout=5)

        assert response.accepted

        expected = [-0.1, -0.2, -0.3, -0.4]

        values = client.pull(keys).wait(timeout_ms=5000).values

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(values, expected)
        )

        print("Cached Push updates correct keys: PASS")

        # 3. Check that explicit iteration 7 reaches both keys.
        assert (
            service.range_clocks["p1"].clock_for_key(10)
            == {2101: 7}
        )

        assert (
            service.range_clocks["p1"].clock_for_key(70)
            == {2101: 7}
        )

        print("Cached Push vector clocks: PASS")

        # 4. Resend exactly the same request.
        # It must not apply gradients a second time.
        retry_response = client.stub.Push(request, timeout=5)

        assert retry_response.accepted

        values = client.pull(keys).wait(timeout_ms=5000).values

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(values, expected)
        )

        print("Duplicate cached Push applied once: PASS")

        # 5. Reusing the Task ID with changed gradients
        # must be rejected.
        conflicting = pb.PushRequest()
        conflicting.CopyFrom(request)
        conflicting.gradients[0] = 99.0

        try:
            client.stub.Push(conflicting, timeout=5)
            raise AssertionError("Expected Task ID conflict")

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT

        print("Task ID fingerprint conflict rejected: PASS")

        # 6. An unknown cached key list must never update weights.
        missing = pb.PushRequest(
            gradients=[9.0, 9.0, 9.0, 9.0],
            width=2,
            key_cache_id=b"D" * 16,
            use_cached_keys=True,
        )

        missing.task_id.CopyFrom(
            pb.TaskId(worker_id=2101, sequence=2)
        )

        try:
            client.stub.Push(missing, timeout=5)
            raise AssertionError("Expected cache miss")

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert (error.details() or "").startswith(
                "KEY_CACHE_MISS:"
            )

        values = client.pull(keys).wait(timeout_ms=5000).values

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(values, expected)
        )

        print("Cache miss leaves weights unchanged: PASS")
        print("D1.2C cached Push: PASS")

    finally:
        if client is not None:
            client.close()

        server.stop(0).wait()


if __name__ == "__main__":
    main()
