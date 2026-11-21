
import grpc
import parameter_server_pb2 as pb

from parameter_client import ParameterClient
from harness.local_servers import make_server


def main():
    server, service, address = make_server()
    client = None

    try:
        server.start()
        client = ParameterClient(address, worker_id=2002)

        keys = [(1 << 60), (1 << 60) + 2]
        cache_id = b"A" * 16

        # 1. Register keys using a full Pull request.
        full_request = pb.PullRequest(
            keys=keys,
            key_cache_id=cache_id,
        )

        full = client.stub.Pull(full_request, timeout=5)

        assert list(full.values) == [0.0] * 4
        assert service.key_cache[cache_id] == tuple(keys)

        print("Key-list registration: PASS")

        # 2. Resolve the same keys using only the cache ID.
        cached_request = pb.PullRequest(
            key_cache_id=cache_id,
            use_cached_keys=True,
        )

        cached = client.stub.Pull(cached_request, timeout=5)

        assert list(cached.values) == list(full.values)
        assert list(cached.keys) == keys
        assert cached_request.ByteSize() < full_request.ByteSize()

        print("Cached Pull lookup: PASS")

        # 3. Verify that the cache does not store parameter values.
        client.push(
            keys,
            [1.0, 2.0, 3.0, 4.0],
            width=2,
        ).wait(timeout_ms=5000)

        updated = client.stub.Pull(cached_request, timeout=5)

        expected = [-0.1, -0.2, -0.3, -0.4]

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(updated.values, expected)
        )

        print("Cached Pull returns updated values: PASS")

        # 4. Unknown cache IDs must produce a safe cache miss.
        missing_request = pb.PullRequest(
            key_cache_id=b"B" * 16,
            use_cached_keys=True,
        )

        try:
            client.stub.Pull(missing_request, timeout=5)
            raise AssertionError("Expected cache miss")

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert (error.details() or "").startswith(
                "KEY_CACHE_MISS:"
            )

        print("Unknown cache ID rejected: PASS")
        print("D1.2B server-side key cache: PASS")

    finally:
        if client is not None:
            client.close()

        server.stop(0).wait()


if __name__ == "__main__":
    main()
