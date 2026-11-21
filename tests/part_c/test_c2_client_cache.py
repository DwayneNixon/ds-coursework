
import grpc
from concurrent import futures

import manager_pb2_grpc as manager_grpc

from harness.local_servers import start_server
from parameter_client import ParameterClient
from distributed_client import DistributedParameterClient

from routing import Partition, KEY_SPACE_END
from server_manager import ServerManager
from manager_rpc import ManagerService


def main():
    server1, address1 = start_server()
    server2, address2 = start_server()
    server3, address3 = start_server()

    manager = ServerManager([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, 80, address2),
        Partition("p3", 80, KEY_SPACE_END, address2),
    ])

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2)
    )

    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager),
        grpc_manager,
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0

    grpc_manager.start()
    manager_address = f"127.0.0.1:{port}"

    client = None
    inspector = None

    try:
        # Discover ownership from the manager.
        client = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=951,
            value_width=2,
        )

        assert client.table.epoch == 1
        assert len(client.clients) == 2

        print("Initial manager discovery and caching: PASS")

        # Use both original owners.
        client.push(
            [10, 50],
            [10.0, 20.0, 30.0, 40.0],
            width=2,
        ).wait(timeout_ms=5000)

        values = client.pull(
            [10, 50]
        ).wait(timeout_ms=5000).values

        assert list(values) == [-1.0, -2.0, -3.0, -4.0]

        print("Cached ownership routing: PASS")

        # Move an unused partition to Server 3.
        # No parameter data exists in this region.
        manager.publish_map([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, 80, address2),
            Partition("p3", 80, KEY_SPACE_END, address3),
        ])

        # The client's cached map must remain unchanged.
        assert client.table.epoch == 1
        assert client.table.split_keys([90])[0].owner == address2

        print("Cached map isolation: PASS")

        # Explicitly refresh the ownership information.
        changed = client.refresh_map()

        assert changed is True
        assert client.table.epoch == 2
        assert client.table.split_keys([90])[0].owner == address3
        assert address3 in client.clients

        print("Ownership refresh and new connection: PASS")

        # Refreshing an unchanged epoch should do nothing.
        assert client.refresh_map() is False

        print("Unchanged map refresh: PASS")

        # Update a previously untouched key on the new owner.
        client.push(
            [90],
            [10.0, 20.0],
            width=2,
        ).wait(timeout_ms=5000)

        # Pull keys distributed across all three servers.
        values = client.pull(
            [10, 50, 90]
        ).wait(timeout_ms=5000).values

        assert list(values) == [
            -1.0, -2.0,
            -3.0, -4.0,
            -1.0, -2.0,
        ]

        # Confirm the old owner did not receive key 90's update.
        inspector = ParameterClient(address2, worker_id=952)

        old_owner_values = inspector.pull(
            [90]
        ).wait(timeout_ms=5000).values

        assert list(old_owner_values) == [0.0, 0.0]

        print("Routing after refresh: PASS")
        print("C2.3 client caching and refresh: PASS")

    finally:
        if client is not None:
            client.close()

        if inspector is not None:
            inspector.close()

        grpc_manager.stop(0).wait()

        for server in (server1, server2, server3):
            server.stop(0).wait()


if __name__ == "__main__":
    main()
