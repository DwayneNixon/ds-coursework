
"""Part E1.2 — End-to-end ownership cutover."""

from concurrent import futures
from math import isclose

import grpc
import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from parameter_client import ParameterClient
from routing import Partition, KEY_SPACE_END


def check_values(actual, expected):
    assert len(actual) == len(expected)
    assert all(
        isclose(a, b, rel_tol=0, abs_tol=1e-9)
        for a, b in zip(actual, expected)
    )


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    manager = ServerManager([
        Partition("p1", 0, KEY_SPACE_END, address1)
    ])

    original_table = manager.get_map()

    service1.set_ownership(address1, original_table)
    service2.set_ownership(address2, original_table)

    server1.start()
    server2.start()

    grpc_manager = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
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
    old_owner_client = None

    try:
        client = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=701,
            value_width=2,
        )

        # ---------------------------------------------
        # 1. Populate the original server
        # ---------------------------------------------

        client.push(
            [10, 50, 90],
            [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            width=2,
            iteration=1,
        ).wait(timeout_ms=5000)

        client.push(
            [50],
            [1.0, 2.0],
            width=2,
            iteration=2,
        ).wait(timeout_ms=5000)

        check_values(
            client.pull([10, 50, 90]).wait(
                timeout_ms=5000
            ).values,
            [-1.0, -2.0, -3.1, -4.2, -5.0, -6.0],
        )

        print("Initial distributed parameter state: PASS")

        # ---------------------------------------------
        # 2. Move [40, 80) to the new server
        # ---------------------------------------------

        assert client.table.epoch == 1

        new_table = manager.move_range(
            source=service1,
            destination=service2,
            destination_address=address2,
            begin=40,
            end=80,
        )

        assert new_table.epoch == 2
        assert manager.get_map().epoch == 2
        assert client.table.epoch == 1

        assert [
            (part.begin, part.end, part.owner)
            for part in new_table.partitions
        ] == [
            (0, 40, address1),
            (40, 80, address2),
            (80, KEY_SPACE_END, address1),
        ]

        print("Manager ownership split: PASS")
        print("Ownership epoch incremented: PASS")

        # ---------------------------------------------
        # 3. Verify parameter placement
        # ---------------------------------------------

        assert 50 not in service1.weights
        assert 10 in service1.weights
        assert 90 in service1.weights

        assert 50 in service2.weights
        assert 10 not in service2.weights
        assert 90 not in service2.weights

        check_values(
            service2.weights[50],
            [-3.1, -4.2],
        )

        print("Transferred values preserved: PASS")
        print("Unmoved values retained: PASS")

        # ---------------------------------------------
        # 4. Verify split vector clocks
        # ---------------------------------------------

        left, moved, right = new_table.partitions

        assert service1.range_clocks[
            left.partition_id
        ].clock_for_key(10) == {701: 1}

        assert service2.range_clocks[
            moved.partition_id
        ].clock_for_key(50) == {701: 2}

        assert service1.range_clocks[
            right.partition_id
        ].clock_for_key(90) == {701: 1}

        print("All partition clocks preserved: PASS")

        # The completed Push entirely inside [40,80)
        # must exist at the new owner for deduplication.
        assert (701, 2) in service2.completed_pushes
        assert service2.task_states[(701, 2)] == "DONE"

        print("Migrated Push history preserved: PASS")

        # ---------------------------------------------
        # 5. Stale client recovers automatically
        # ---------------------------------------------

        # Client still holds epoch 1 and sends the Pull
        # to its previous owner.
        values = client.pull(
            [10, 50, 90]
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [-1.0, -2.0, -3.1, -4.2, -5.0, -6.0],
        )

        assert client.table.epoch == 2

        print("Automatic map refresh after migration: PASS")
        print("Distributed Pull after migration: PASS")

        # A direct stale request to the old owner
        # must now receive NOT_OWNER.
        old_owner_client = ParameterClient(
            address1, worker_id=702
        )

        try:
            old_owner_client.pull(
                [50]
            ).wait(timeout_ms=5000)

        except grpc.RpcError as error:
            assert (
                error.code()
                == grpc.StatusCode.FAILED_PRECONDITION
            )
            assert (
                error.details() or ""
            ).startswith("NOT_OWNER:")

        else:
            raise AssertionError(
                "Old owner must reject migrated keys"
            )

        print("Old owner rejects migrated keys: PASS")

        # ---------------------------------------------
        # 6. Continue training across both servers
        # ---------------------------------------------

        result = client.push(
            [10, 50, 90],
            [1.0, 2.0, 1.0, 2.0, 1.0, 2.0],
            width=2,
            iteration=3,
        ).wait(timeout_ms=5000)

        assert result.accepted

        values = client.pull(
            [10, 50, 90],
            required_clock={701: 3},
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [-1.1, -2.2, -3.2, -4.4, -5.1, -6.2],
        )

        print("Training continues after migration: PASS")
        print("Required vector-clock dependencies: PASS")
        print("No lost or duplicate parameter updates: PASS")

        # ---------------------------------------------
        # 7. Final checks
        # ---------------------------------------------

        assert service1._transfer is None
        assert service2._incoming_transfer is None

        print("Migration state cleaned up: PASS")
        print("E1.2 live ownership cutover: PASS")

    finally:
        if client is not None:
            client.close()

        if old_owner_client is not None:
            old_owner_client.close()

        grpc_manager.stop(0).wait()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
