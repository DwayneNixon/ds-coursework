
import grpc
from concurrent import futures

import manager_pb2_grpc as pb_grpc

from routing import Partition, KEY_SPACE_END
from server_manager import ServerManager
from manager_rpc import ManagerService, fetch_ownership_map


def main():
    manager = ServerManager([
        Partition("p1", 0, 40, "127.0.0.1:50051"),
        Partition("p2", 40, KEY_SPACE_END, "127.0.0.1:50052"),
    ])

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2)
    )

    pb_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager),
        server,
    )

    port = server.add_insecure_port("127.0.0.1:0")
    assert port != 0

    server.start()
    address = f"127.0.0.1:{port}"

    try:
        # Retrieve the first ownership map.
        cached = fetch_ownership_map(address)

        assert cached.epoch == 1
        assert len(cached.partitions) == 2

        print("Initial gRPC ownership discovery: PASS")

        routes = cached.split_keys([10, 50])

        assert routes[0].owner == "127.0.0.1:50051"
        assert routes[1].owner == "127.0.0.1:50052"

        print("Remote ownership routing: PASS")

        # Publish an updated map on the manager.
        manager.publish_map([
            Partition("p1", 0, 40, "127.0.0.1:50051"),
            Partition("p2", 40, KEY_SPACE_END, "127.0.0.1:50053"),
        ])

        refreshed = fetch_ownership_map(address)

        assert refreshed.epoch == 2
        assert cached.epoch == 1

        print("Ownership epoch refresh: PASS")

        # The old snapshot retains its original owner.
        assert (
            cached.split_keys([50])[0].owner
            == "127.0.0.1:50052"
        )

        assert (
            refreshed.split_keys([50])[0].owner
            == "127.0.0.1:50053"
        )

        print("Cached snapshot isolation: PASS")

        # Verify the full uint64 key-space endpoint survived
        # the Protobuf serialization and decoding.
        assert refreshed.partitions[-1].end == KEY_SPACE_END

        print("Maximum key-space boundary: PASS")
        print("C2.2 gRPC ownership discovery: PASS")

    finally:
        server.stop(0).wait()


if __name__ == "__main__":
    main()
