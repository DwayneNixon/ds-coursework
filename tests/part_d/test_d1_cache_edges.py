
import grpc
import parameter_server_pb2 as pb
import manager_pb2_grpc as manager_grpc

from concurrent import futures
from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from server_manager import ServerManager
from manager_rpc import ManagerService
from routing import Partition, RoutingTable, KEY_SPACE_END


def test_retry_after_eviction():
    server, service, address = make_server()

    table = RoutingTable([
        Partition("p1", 0, KEY_SPACE_END, address),
    ])

    service.set_ownership(address, table)
    server.start()

    client = None

    try:
        client = DistributedParameterClient(
            table, worker_id=2301, key_caching=True
        )

        keys = [10, 70]
        gradients = [1.0, 2.0, 3.0, 4.0]

        # Register keys, then apply one cached Push.
        client.pull(keys).wait(timeout_ms=5000)

        task = client.push(
            keys, gradients, width=2, iteration=7
        )
        task.wait(timeout_ms=5000)

        subtask = task.tasks[0][1]
        original = pb.PushRequest()
        original.ParseFromString(subtask._push_request_bytes)

        assert original.use_cached_keys

        # Evict the key list AFTER the Push succeeded.
        with service.lock:
            service.key_cache.clear()

        stub = client.clients[address].stub

        # The exact replay initially encounters a cache miss.
        try:
            stub.Push(original, timeout=5)
            raise AssertionError("Expected cache miss")
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert (error.details() or "").startswith(
                "KEY_CACHE_MISS:"
            )

        print("Completed Push survives cache miss: PASS")

        # Re-register keys without modifying the original Push.
        stub.Pull(
            pb.PullRequest(
                keys=keys,
                key_cache_id=original.key_cache_id,
            ),
            timeout=5,
        )

        # Replay the exact same bytes and Task ID.
        replay = stub.Push(original, timeout=5)
        assert replay.accepted

        values = client.pull(keys).wait(timeout_ms=5000).values
        expected = [-0.1, -0.2, -0.3, -0.4]

        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(values, expected)
        )

        assert len(service.completed_pushes) == 1

        assert service.range_clocks["p1"].clock_for_key(10) == {
            2301: 7
        }
        assert service.range_clocks["p1"].clock_for_key(70) == {
            2301: 7
        }

        print("Exact replay does not duplicate updates: PASS")

    finally:
        if client is not None:
            client.close()
        server.stop(0).wait()


def test_ownership_change():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()
    server3, service3, address3 = make_server()

    manager = ServerManager([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ])

    initial = manager.get_map()

    for service, address in (
        (service1, address1),
        (service2, address2),
        (service3, address3),
    ):
        service.set_ownership(address, initial)

    for server in (server1, server2, server3):
        server.start()

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2)
    )

    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager), grpc_manager
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0
    grpc_manager.start()

    client = None

    try:
        client = DistributedParameterClient.from_manager(
            f"127.0.0.1:{port}",
            worker_id=2302,
            key_caching=True,
        )

        keys = [10, 50]

        # Populate both caches under ownership epoch 1.
        client.pull(keys).wait(timeout_ms=5000)

        assert client.table.epoch == 1

        # Move partition p2 from server 2 to server 3.
        manager.publish_map([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address3),
        ])

        updated = manager.get_map()

        for service, address in (
            (service1, address1),
            (service2, address2),
            (service3, address3),
        ):
            service.set_ownership(address, updated)

        # Client still has the old routing map.
        task = client.push(
            keys,
            [1.0, 2.0, 3.0, 4.0],
            width=2,
            iteration=9,
        )

        task.wait(timeout_ms=5000)

        assert client.table.epoch == 2
        assert len(task.tasks) == 3

        print("Cached client refreshes ownership: PASS")

        assert service1.weights[10] == [-0.1, -0.2]
        actual = service3.weights.get(50)
        print("Server 3 key 50:", actual)

        assert actual is not None
        assert len(actual) == 2
        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(actual, [-0.3, -0.4])
        )
        assert 50 not in service2.weights

        assert service1.range_clocks["p1"].clock_for_key(10) == {
            2302: 9
        }
        assert service3.range_clocks["p2"].clock_for_key(50) == {
            2302: 9
        }

        print("Ownership recovery preserves updates: PASS")

        values = client.pull(keys).wait(timeout_ms=5000).values

        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(
                values, [-0.1, -0.2, -0.3, -0.4]
            )
        )

        print("Cached Pull after ownership change: PASS")

    finally:
        if client is not None:
            client.close()

        grpc_manager.stop(0).wait()

        for server in (server1, server2, server3):
            server.stop(0).wait()


if __name__ == "__main__":
    test_retry_after_eviction()
    test_ownership_change()
    print("D1.5 cache recovery tests: PASS")
