from harness.local_servers import make_server

import grpc
from concurrent import futures

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END



def expect_not_owner(task):
    try:
        task.wait(timeout_ms=5000)

    except grpc.RpcError as error:
        assert error.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert error.details().startswith("NOT_OWNER:")

    else:
        raise AssertionError("Expected NOT_OWNER rejection")


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    table = RoutingTable([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ])

    # Install ownership before starting either server.
    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    server1.start()
    server2.start()

    client1 = None
    client2 = None

    try:
        client1 = ParameterClient(address1, worker_id=971)
        client2 = ParameterClient(address2, worker_id=972)

        # 1. Correctly routed operations must succeed.
        client1.push(
            [38], [10.0, 20.0], width=2
        ).wait(timeout_ms=5000)

        values = client1.pull(
            [38]
        ).wait(timeout_ms=5000).values

        assert list(values) == [-1.0, -2.0]

        print("Correct owner operations: PASS")

        # 2. A request spanning two owners must be
        # rejected completely, with no partial update.
        expect_not_owner(
            client1.push(
                [38, 40],
                [50.0, 60.0, 70.0, 80.0],
                width=2,
            )
        )

        values = client1.pull(
            [38]
        ).wait(timeout_ms=5000).values

        assert list(values) == [-1.0, -2.0]
        assert 40 not in service1.weights

        print("Cross-partition Push rejected atomically: PASS")

        # 3. An incorrectly routed Pull must also fail.
        expect_not_owner(client1.pull([50]))

        values = client2.pull(
            [50]
        ).wait(timeout_ms=5000).values

        assert list(values) == [0.0, 0.0]

        print("Wrong-owner Pull rejected: PASS")

        # 4. Publish a new ownership snapshot locally.
        # Key 50 has never been updated, so no data
        # migration is needed for this particular test.
        new_table = RoutingTable([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address1),
        ], epoch=2)

        service1.set_ownership(address1, new_table)
        service2.set_ownership(address2, new_table)

        # Server 2 must now reject requests for key 50.
        expect_not_owner(client2.pull([50]))

        # Server 1 must now accept them.
        client1.push(
            [50], [30.0, 40.0], width=2
        ).wait(timeout_ms=5000)

        values = client1.pull(
            [50]
        ).wait(timeout_ms=5000).values

        assert list(values) == [-3.0, -4.0]
        assert 50 not in service2.weights

        print("New epoch ownership enforced: PASS")
        print("C2.4B server-side ownership: PASS")

    finally:
        if client1 is not None:
            client1.close()

        if client2 is not None:
            client2.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
