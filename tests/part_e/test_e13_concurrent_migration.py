
"""Part E1.3 — Concurrent updates during live migration."""

import threading
import time
from math import isclose
from concurrent import futures

import grpc
import manager_pb2_grpc as manager_grpc

from harness.local_servers import make_server
from server_manager import ServerManager
from manager_rpc import ManagerService
from distributed_client import DistributedParameterClient
from routing import Partition, KEY_SPACE_END


def check_values(actual, expected):
    assert len(actual) == len(expected)
    assert all(
        isclose(a, b, rel_tol=0, abs_tol=1e-9)
        for a, b in zip(actual, expected)
    ), f"Expected {expected}, received {actual}"


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
        ManagerService(manager), grpc_manager
    )

    port = grpc_manager.add_insecure_port("127.0.0.1:0")
    assert port != 0
    grpc_manager.start()

    manager_address = f"127.0.0.1:{port}"

    client = None
    migration_thread = None

    # Synchronize the test with the migration phases.
    snapshot_staged = threading.Event()
    continue_copy = threading.Event()

    source_frozen = threading.Event()
    continue_cutover = threading.Event()

    migration_result = {}

    try:
        client = DistributedParameterClient.from_manager(
            manager_address,
            worker_id=811,
            value_width=2,
        )

        # ---------------------------------------------
        # 1. Initial state
        # ---------------------------------------------

        client.push(
            [10, 50, 90],
            [10.0] * 6,
            width=2,
            iteration=1,
        ).wait(timeout_ms=5000)

        check_values(
            client.pull([10, 50, 90]).wait(
                timeout_ms=5000
            ).values,
            [-1.0] * 6,
        )

        print("Initial state established: PASS")

        # ---------------------------------------------
        # 2. Pause migration after snapshot staging
        # ---------------------------------------------

        original_stage = service2.stage_incoming_transfer

        def staged_transfer(partition_id, snapshot):
            original_stage(partition_id, snapshot)

            snapshot_staged.set()

            if not continue_copy.wait(timeout=10):
                raise TimeoutError(
                    "Test timed out during snapshot staging"
                )

        service2.stage_incoming_transfer = staged_transfer

        # Pause again after the source range is frozen.
        original_freeze = service1.freeze_range_transfer

        def frozen_transfer():
            delta = original_freeze()

            source_frozen.set()

            if not continue_cutover.wait(timeout=10):
                raise TimeoutError(
                    "Test timed out during frozen cutover"
                )

            return delta

        service1.freeze_range_transfer = frozen_transfer

        def migrate():
            try:
                migration_result["table"] = manager.move_range(
                    source=service1,
                    destination=service2,
                    destination_address=address2,
                    begin=40,
                    end=80,
                )
            except Exception as error:
                migration_result["error"] = error

        migration_thread = threading.Thread(target=migrate)
        migration_thread.start()

        assert snapshot_staged.wait(timeout=5), (
            "Migration did not reach snapshot staging"
        )

        # ---------------------------------------------
        # 3. Push while the destination holds a snapshot
        # ---------------------------------------------

        # This Push must still go to the original owner,
        # and its effect must appear in the final delta.
        client.push(
            [50],
            [2.0, 2.0],
            width=2,
            iteration=2,
        ).wait(timeout_ms=5000)

        # An unaffected key can also be updated.
        client.push(
            [10],
            [1.0, 1.0],
            width=2,
            iteration=2,
        ).wait(timeout_ms=5000)

        check_values(
            service1.weights[50],
            [-1.2, -1.2],
        )

        check_values(
            service1.weights[10],
            [-1.1, -1.1],
        )

        print("Concurrent source updates accepted: PASS")

        # Continue to the freeze phase.
        continue_copy.set()

        assert source_frozen.wait(timeout=5), (
            "Migration did not reach the frozen phase"
        )

        # ---------------------------------------------
        # 4. Push while the moving interval is frozen
        # ---------------------------------------------

        blocked = client.push(
            [50],
            [3.0, 3.0],
            width=2,
            iteration=3,
        )

        time.sleep(0.15)

        assert not blocked.done(), (
            "Push to frozen interval should not finish "
            "before the ownership transition"
        )

        print("Push waits during ownership cutover: PASS")

        # Finish installing the destination state and
        # publishing the manager's new ownership map.
        continue_cutover.set()

        migration_thread.join(timeout=10)

        assert not migration_thread.is_alive(), (
            "Migration did not finish"
        )

        if "error" in migration_result:
            raise migration_result["error"]

        assert migration_result["table"].epoch == 2

        # The blocked Push should receive NOT_OWNER from
        # the source, refresh routing, and succeed at
        # the destination.
        assert blocked.wait(timeout_ms=5000).accepted

        assert client.table.epoch == 2

        print("Frozen Push automatically rerouted: PASS")

        # ---------------------------------------------
        # 5. Verify the final parameters
        # ---------------------------------------------

        # Key 10:
        #   Initial gradient 10 -> -1.0
        #   Concurrent gradient 1 -> -1.1
        #
        # Key 50:
        #   Initial gradient 10 -> -1.0
        #   During-copy gradient 2 -> -1.2
        #   During-cutover gradient 3 -> -1.5
        #
        # Key 90:
        #   Initial gradient 10 -> -1.0

        values = client.pull(
            [10, 50, 90]
        ).wait(timeout_ms=5000).values

        check_values(
            values,
            [
                -1.1, -1.1,
                -1.5, -1.5,
                -1.0, -1.0,
            ],
        )

        assert 50 not in service1.weights

        check_values(
            service2.weights[50],
            [-1.5, -1.5],
        )

        print("All concurrent updates preserved exactly once: PASS")

        # ---------------------------------------------
        # 6. Verify migrated and retained vector clocks
        # ---------------------------------------------

        left, moved, right = (
            migration_result["table"].partitions
        )

        assert service1.range_clocks[
            left.partition_id
        ].clock_for_key(10) == {811: 2}

        assert service2.range_clocks[
            moved.partition_id
        ].clock_for_key(50) == {811: 3}

        assert service1.range_clocks[
            right.partition_id
        ].clock_for_key(90) == {811: 1}

        # Required-clock Pulls must still work.
        client.pull(
            [50],
            required_clock={811: 3},
        ).wait(timeout_ms=5000)

        client.pull(
            [10],
            required_clock={811: 2},
        ).wait(timeout_ms=5000)

        print("Vector clocks preserved through cutover: PASS")
        print("Required-clock dependencies remain valid: PASS")

        # ---------------------------------------------
        # Final result
        # ---------------------------------------------

        assert service1._transfer is None
        assert service2._incoming_transfer is None

        print("Migration resources cleaned up: PASS")
        print("E1.3 concurrent live migration: PASS")

    finally:
        # Always release test pauses to avoid hanging.
        continue_copy.set()
        continue_cutover.set()

        if migration_thread is not None:
            migration_thread.join(timeout=10)

        if client is not None:
            client.close()

        grpc_manager.stop(0).wait()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
