
import grpc
from concurrent import futures

import manager_pb2_grpc as manager_grpc

from Part_C_Tests.test_c2_server_ownership import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from harness.metrics import measure_operation
from routing import Partition, KEY_SPACE_END


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()
    server3, service3, address3 = make_server()

    manager = ServerManager([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ])

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

    writer = None
    reader = None

    try:
        # Both clients cache the original ownership map.
        writer = DistributedParameterClient.from_manager(
            manager_address, worker_id=991
        )
        reader = DistributedParameterClient.from_manager(
            manager_address, worker_id=992
        )

        # Change ownership before either client refreshes.
        # The reassigned region has no existing parameter data.
        manager.publish_map([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address3),
        ])

        updated_table = manager.get_map()

        for service, address in [
            (service1, address1),
            (service2, address2),
            (service3, address3),
        ]:
            service.set_ownership(address, updated_table)

        assert writer.table.epoch == 1
        assert reader.table.epoch == 1

        print("Two stale client maps established: PASS")

        records = []

        # No manual refresh or retry!
        push_task, _ = measure_operation(
            create_task=lambda: writer.push(
                [10, 50],
                [10.0, 20.0, 30.0, 40.0],
                width=2,
                iteration=17,
            ),
            worker_id=991,
            iteration=1,
            op="push",
            n_keys=2,
            record_callback=records.append,
        )

        assert push_task.done()
        assert writer.table.epoch == 2
        assert len(push_task.tasks) == 3

        print("Automatic rejected-shard Push recovery: PASS")

        assert service1.weights[10] == [-1.0, -2.0]
        assert service3.weights[50] == [-3.0, -4.0]
        assert (
            service1.range_clocks["p1"].clock_for_key(10)
            == {991: 17}
        )

        assert (
            service3.range_clocks["p2"].clock_for_key(50)
            == {991: 17}
        )

        print("Iteration preserved across stale-map retry: PASS")
        assert 50 not in service2.weights

        print("No duplicate successful update: PASS")

        # Reader is still stale and must recover its Pull.
        pull_task, result = measure_operation(
            create_task=lambda: reader.pull([10, 50]),
            worker_id=992,
            iteration=1,
            op="pull",
            n_keys=2,
            target=[10, 50],
            record_callback=records.append,
        )

        assert list(result.values) == [
            -1.0, -2.0, -3.0, -4.0
        ]

        assert reader.table.epoch == 2
        assert len(pull_task.tasks) == 3

        print("Automatic stale Pull recovery: PASS")

        assert len(records) == 2

        for record in records:
            assert record["status"] == "OK"
            assert record["bytes_out"] > 0
            assert record["bytes_in"] >= 0

        print("Retry-inclusive payload accounting: PASS")
        print("C2.4C automatic stale recovery: PASS")

    finally:
        if writer is not None:
            writer.close()
        if reader is not None:
            reader.close()

        grpc_manager.stop(0).wait()

        for server in (server1, server2, server3):
            server.stop(0).wait()


if __name__ == "__main__":
    main()
