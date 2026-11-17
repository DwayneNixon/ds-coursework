
import grpc
from concurrent import futures

import manager_pb2_grpc as manager_grpc

from Part_C_Tests.test_c2_server_ownership import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from routing import Partition, KEY_SPACE_END


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()
    server3, service3, address3 = make_server()

    original_partitions = [
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ]

    manager = ServerManager(original_partitions)
    initial_table = manager.get_map()

    for service, address in [
        (service1, address1),
        (service2, address2),
        (service3, address3),
    ]:
        service.set_ownership(address, initial_table)

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

    manager_address = f"127.0.0.1:{port}"
    client = None

    try:
        client = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=981,
            value_width=2,
        )

        assert client.table.epoch == 1

        # Ownership changes before the client refreshes.
        # Key 50 has not been updated or created yet.
        manager.publish_map([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address3),
        ])

        new_table = manager.get_map()

        for service, address in [
            (service1, address1),
            (service2, address2),
            (service3, address3),
        ]:
            service.set_ownership(address, new_table)

        assert client.table.epoch == 1
        print("Stale client map established: PASS")

        # Client attempts a Push using its old routing map.
        task = client.push(
            [10, 50],
            [10.0, 20.0, 30.0, 40.0],
            width=2,
        )

        try:
            task.wait(timeout_ms=5000)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert error.details().startswith("NOT_OWNER:")
        else:
            raise AssertionError("Expected NOT_OWNER")

        print("Stale Push rejected: PASS")

        # Check the individual physical subrequests.
        succeeded = []
        failed = []

        for route, subtask in task.tasks:
            if subtask._future.exception() is None:
                succeeded.append(route)
            else:
                failed.append(route)

        assert len(succeeded) == 1
        assert len(failed) == 1
        assert list(succeeded[0].keys) == [10]
        assert list(failed[0].keys) == [50]

        # The successful shard has already been updated.
        assert service1.weights[10] == [-1.0, -2.0]

        # The rejected shard has not been updated.
        assert 50 not in service2.weights
        assert 50 not in service3.weights

        print("Partial-success detection: PASS")

        # Refresh the map, then retry ONLY the failed key.
        assert client.refresh_map() is True
        assert client.table.epoch == 2

        client.push(
            [50],
            [30.0, 40.0],
            width=2,
        ).wait(timeout_ms=5000)

        print("Failed shard retried on new owner: PASS")

        # Verify both keys and ensure key 10 was not
        # accidentally updated a second time.
        values = client.pull(
            [10, 50]
        ).wait(timeout_ms=5000).values

        assert list(values) == [
            -1.0, -2.0,
            -3.0, -4.0,
        ]

        assert service1.weights[10] == [-1.0, -2.0]
        assert 50 not in service2.weights

        print("No duplicate successful update: PASS")
        print("C2.4C stale recovery foundation: PASS")

    finally:
        if client is not None:
            client.close()

        grpc_manager.stop(0).wait()

        for server in (server1, server2, server3):
            server.stop(0).wait()


if __name__ == "__main__":
    main()
