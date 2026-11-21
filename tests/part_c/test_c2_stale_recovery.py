
import grpc
from concurrent import futures

import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
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

        # --------------------------------------------------
        # 1. Automatic stale-map Push recovery
        # --------------------------------------------------

        task = client.push(
            [10, 50],
            [10.0, 20.0, 30.0, 40.0],
            width=2,
        )

        # The distributed client now handles NOT_OWNER
        # automatically instead of exposing the error
        # to the logical caller.
        result = task.wait(timeout_ms=5000)

        assert result.accepted
        assert client.table.epoch == 2

        print("Automatic stale-map Push recovery: PASS")

        # --------------------------------------------------
        # 2. Verify physical NOT_OWNER rejection
        # --------------------------------------------------

        # Two original physical requests:
        #   key 10 -> server1 (succeeds)
        #   key 50 -> server2 (rejected)
        #
        # One automatic retry:
        #   key 50 -> server3 (succeeds)

        assert len(task.tasks) == 3

        original_attempts = task.tasks[:2]
        retry_attempts = task.tasks[2:]

        succeeded = []
        rejected = []

        for route, subtask in original_attempts:
            error = subtask._future.exception()

            if error is None:
                succeeded.append(route)
            else:
                assert isinstance(error, grpc.RpcError)
                assert (
                    error.code()
                    == grpc.StatusCode.FAILED_PRECONDITION
                )
                assert (
                    error.details() or ""
                ).startswith("NOT_OWNER:")

                rejected.append(route)

        assert len(succeeded) == 1
        assert len(rejected) == 1

        assert list(succeeded[0].keys) == [10]
        assert list(rejected[0].keys) == [50]

        print("Physical NOT_OWNER rejection preserved: PASS")

        # --------------------------------------------------
        # 3. Verify selective retry
        # --------------------------------------------------

        # Only the rejected key should be retried.
        # The successful key must not be resubmitted.

        assert len(retry_attempts) == 1

        retry_route, retry_task = retry_attempts[0]

        assert retry_route.owner == address3
        assert list(retry_route.keys) == [50]
        assert retry_task.wait(timeout_ms=0).accepted

        print("Selective rejected-shard retry: PASS")

        # --------------------------------------------------
        # 4. Verify physical server state
        # --------------------------------------------------

        # Server1 should have applied key 10 once.
        assert service1.weights[10] == [-1.0, -2.0]

        # Server2 must not apply the rejected key.
        assert 50 not in service2.weights

        # Server3 should have applied key 50 once.
        assert service3.weights[50] == [-3.0, -4.0]

        print("Correct physical server updates: PASS")

        # --------------------------------------------------
        # 5. Verify complete logical result
        # --------------------------------------------------

        values = client.pull(
            [10, 50]
        ).wait(timeout_ms=5000).values

        assert list(values) == [
            -1.0, -2.0,
            -3.0, -4.0,
        ]

        print("Distributed Pull after recovery: PASS")

        # --------------------------------------------------
        # 6. Verify no duplicated updates
        # --------------------------------------------------

        assert service1.weights[10] == [-1.0, -2.0]
        assert service3.weights[50] == [-3.0, -4.0]
        assert 50 not in service2.weights

        print("No duplicate successful update: PASS")

        # --------------------------------------------------
        # Final result
        # --------------------------------------------------

        print("C2.4C stale recovery regression: PASS")

    finally:
        if client is not None:
            client.close()

        grpc_manager.stop(0).wait()

        for server in (server1, server2, server3):
            server.stop(0).wait()


if __name__ == "__main__":
    main()
