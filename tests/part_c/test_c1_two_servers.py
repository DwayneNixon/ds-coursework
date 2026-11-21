from harness.local_servers import start_server

import grpc
from concurrent import futures

from server import ParameterServer
from parameter_client import ParameterClient
import parameter_server_pb2_grpc



def main():
    server1, address1 = start_server()
    server2, address2 = start_server()

    clients = []

    try:
        client1 = ParameterClient(address1, worker_id=901)
        client2 = ParameterClient(address2, worker_id=902)
        clients.extend([client1, client2])

        print("Server 1:", address1)
        print("Server 2:", address2)

        # Update only Server 1.
        client1.push(
            [10], [10.0, 20.0], width=2
        ).wait(timeout_ms=5000)

        values1 = list(
            client1.pull([10]).wait(timeout_ms=5000).values
        )
        values2 = list(
            client2.pull([10]).wait(timeout_ms=5000).values
        )

        assert values1 == [-1.0, -2.0]
        assert values2 == [0.0, 0.0]
        print("Independent server state: PASS")

        # Now update Server 2.
        client2.push(
            [10], [30.0, 40.0], width=2
        ).wait(timeout_ms=5000)

        values1 = list(
            client1.pull([10]).wait(timeout_ms=5000).values
        )
        values2 = list(
            client2.pull([10]).wait(timeout_ms=5000).values
        )

        assert values1 == [-1.0, -2.0]
        assert values2 == [-3.0, -4.0]
        print("Separate server updates: PASS")

        print("C1.4 multi-server smoke test: PASS")

    finally:
        for client in clients:
            client.close()

        server1.stop(0)
        server2.stop(0)


if __name__ == "__main__":
    main()
