
"""Part E2 — Graceful removal of a server with two partitions."""

from concurrent import futures
from math import isclose

import grpc
import manager_pb2_grpc as manager_grpc

from Part_C_Tests.test_c2_server_ownership import make_server
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
    ), f"Expected {expected}, received {actual}"


def clock_for_key(service, table, key):
    partition = next(
        part for part in table.partitions
        if part.begin <= key < part.end
    )

    return service.range_clocks[
        partition.partition_id
    ].clock_for_key(key)


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    manager = ServerManager([
        Partition("p1", 0, KEY_SPACE_END, address1)
    ])

    initial_table = manager.get_map()

    service1.set_ownership(address1, initial_table)
    service2.set_ownership(address2, initial_table)

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
    server2_stopped = False

    try:
        client = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=901,
            value_width=2,
        )

        # ---------------------------------------------
        # 1. Populate the original server
        # ---------------------------------------------

        keys = [10, 30, 50, 70, 90]

        client.push(
            keys,
            [10.0] * 10,
            width=2,
            iteration=1,
        ).wait(timeout_ms=5000)

        print("Initial parameter state established: PASS")

        # ---------------------------------------------
        # 2. Give server2 two separate partitions
        # ---------------------------------------------

        manager.move_range(
            source=service1,
            destination=service2,
            destination_address=address2,
            begin=20,
            end=40,
        )

        manager.move_range(
            source=service1,
            destination=service2,
            destination_address=address2,
            begin=60,
            end=80,
        )

        table_before_removal = manager.get_map()

        assert table_before_removal.epoch == 3

        owned_by_server2 = [
            part for part in table_before_removal.partitions
            if part.owner == address2
        ]

        assert len(owned_by_server2) == 2

        # Update the distributed client's routing map.
        assert client.refresh_map()
        assert client.table.epoch == 3

        print("Server2 owns two partitions: PASS")

        # ---------------------------------------------
        # 3. Continue training before removal
        # ---------------------------------------------

        client.push(
            [30],
            [2.0, 2.0],
            width=2,
            iteration=2,
        ).wait(timeout_ms=5000)

        client.push(
            [70],
            [3.0, 3.0],
            width=2,
            iteration=3,
        ).wait(timeout_ms=5000)

        client.push(
            [10],
            [1.0, 1.0],
            width=2,
            iteration=4,
        ).wait(timeout_ms=5000)

        expected_before = [
            -1.1, -1.1,
            -1.2, -1.2,
            -1.0, -1.0,
            -1.3, -1.3,
            -1.0, -1.0,
        ]

        check_values(
            client.pull(keys).wait(
                timeout_ms=5000
            ).values,
            expected_before,
        )

        print("Training across both servers: PASS")

        # ---------------------------------------------
        # 4. Gracefully remove server2
        # ---------------------------------------------

        final_table = manager.remove_server(
            source=service2,
            destination=service1,
            destination_address=address1,
        )

        # Two partitions were moved, one per epoch.
        assert final_table.epoch == 5

        assert all(
            partition.owner == address1
            for partition in final_table.partitions
        )

        assert manager.get_map() == final_table or (
            manager.get_map().epoch == final_table.epoch
            and manager.get_map().partitions
            == final_table.partitions
        )

        print("Both partitions transferred: PASS")
        print("Removed server has no ownership: PASS")
        print("Ownership epochs updated: PASS")

        # ---------------------------------------------
        # 5. Verify the physical parameter placement
        # ---------------------------------------------

        assert all(
            key in service1.weights
            for key in keys
        )

        assert 30 not in service2.weights
        assert 70 not in service2.weights

        check_values(
            [v for key in keys for v in service1.weights[key]],
            expected_before,
        )

        print("All parameter values preserved: PASS")

        # ---------------------------------------------
        # 6. Verify clock and completed-Push history
        # ---------------------------------------------

        expected_clocks = {
            10: {901: 4},
            30: {901: 2},
            50: {901: 1},
            70: {901: 3},
            90: {901: 1},
        }

        for key, expected_clock in expected_clocks.items():
            assert clock_for_key(
                service1, final_table, key
            ) == expected_clock

        # The two completed Pushes originally handled
        # by server2 must have migrated to server1.
        assert (901, 2) in service1.completed_pushes
        assert (901, 3) in service1.completed_pushes

        assert service1.task_states[(901, 2)] == "DONE"
        assert service1.task_states[(901, 3)] == "DONE"

        print("All vector clocks preserved: PASS")
        print("Completed Push history preserved: PASS")

        # ---------------------------------------------
        # 7. Stale client recovers after server removal
        # ---------------------------------------------

        assert client.table.epoch == 3

        values = client.pull(
            keys
        ).wait(timeout_ms=5000).values

        check_values(values, expected_before)

        assert client.table.epoch == 5

        print("Stale routing recovered after removal: PASS")

        # Direct requests to the departing server
        # should be rejected before it is shut down.
        old_owner_client = ParameterClient(
            address2,
            worker_id=902,
        )

        try:
            old_owner_client.pull(
                [30]
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
                "Departing server accepted an old key"
            )

        print("Departing server rejects old keys: PASS")

        # ---------------------------------------------
        # 8. Stop server2 and continue training
        # ---------------------------------------------

        server2.stop(0).wait()
        server2_stopped = True

        # The client has already refreshed its map.
        # All subsequent operations must use server1.
        client.push(
            keys,
            [1.0] * 10,
            width=2,
            iteration=5,
        ).wait(timeout_ms=5000)

        expected_after = [
            value - 0.1
            for value in expected_before
        ]

        values = client.pull(
            keys,
            required_clock={901: 5},
        ).wait(timeout_ms=5000).values

        check_values(values, expected_after)

        print("Training continues after server shutdown: PASS")
        print("Required clocks remain satisfied: PASS")
        print("No lost or duplicated updates: PASS")

        print("E2 graceful server removal: PASS")

    finally:
        if client is not None:
            client.close()

        if old_owner_client is not None:
            old_owner_client.close()

        grpc_manager.stop(0).wait()
        server1.stop(0).wait()

        if not server2_stopped:
            server2.stop(0).wait()


if __name__ == "__main__":
    main()
