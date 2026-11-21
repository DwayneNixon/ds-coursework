
import parameter_server_pb2 as pb

from harness.local_servers import make_server
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
            worker_id=2201,
            value_width=2,
            key_caching=True,
        )

        keys = (
            [(1 << 60) + 2 * i for i in range(32)]
            + [boundary + (1 << 60) + 2 * i for i in range(32)]
        )

        gradients = [1.0, 2.0] * len(keys)

        # 1. First Pull registers full sparse key lists.
        first = client.pull(keys)
        first.wait(timeout_ms=5000)

        assert len(first.tasks) == 2

        for _, subtask in first.tasks:
            request = pb.PullRequest()
            request.ParseFromString(
                subtask._request_attempts[0]
            )

            assert not request.use_cached_keys
            assert len(request.keys) == 32
            assert len(request.key_cache_id) == 16

        print("Distributed key registration: PASS")

        # 2. Subsequent Pulls transmit references.
        second = client.pull(keys)
        second.wait(timeout_ms=5000)

        for _, subtask in second.tasks:
            request = pb.PullRequest()
            request.ParseFromString(
                subtask._request_attempts[0]
            )

            assert request.use_cached_keys
            assert len(request.keys) == 0

        first_bytes = sum(
            len(subtask._request_attempts[0])
            for _, subtask in first.tasks
        )

        second_bytes = sum(
            len(subtask._request_attempts[0])
            for _, subtask in second.tasks
        )

        assert second_bytes < first_bytes

        print("Cached Pull reduces request bytes: PASS")

        # 3. Pushes automatically use cached references.
        push1 = client.push(
            keys,
            gradients,
            width=2,
            iteration=1,
        )

        push1.wait(timeout_ms=5000)

        for _, subtask in push1.tasks:
            request = pb.PushRequest()
            request.ParseFromString(
                subtask._push_request_bytes
            )

            assert request.use_cached_keys
            assert len(request.keys) == 0

        print("Distributed cached Push: PASS")

        # 4. Evict one server's entry.
        with service1.lock:
            service1.key_cache.clear()

        # The client must recover automatically and apply once.
        push2 = client.push(
            keys,
            gradients,
            width=2,
            iteration=2,
        )

        push2.wait(timeout_ms=5000)

        recovered = [
            subtask
            for route, subtask in push2.tasks
            if route.owner == address1
        ]

        assert len(recovered) == 1
        assert len(recovered[0]._request_attempts) == 3

        print("Automatic cache-miss recovery: PASS")

        # 5. Verify numerical correctness and vector clocks.
        final = client.pull(keys).wait(timeout_ms=5000)

        expected = [-0.2, -0.4] * len(keys)

        assert len(final.values) == len(expected)
        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(final.values, expected)
        )

        assert (
            service1.range_clocks["p1"].clock_for_key(keys[0])
            == {2201: 2}
        )

        assert (
            service2.range_clocks["p2"].clock_for_key(keys[-1])
            == {2201: 2}
        )

        print("Weights and vector clocks: PASS")
        print("D1.3 client integration: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
