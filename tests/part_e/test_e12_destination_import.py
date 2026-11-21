
"""Part E1.2 — Destination import and change-set application."""

import grpc
from math import isclose

from harness.local_servers import make_server
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def check_values(actual, expected):
    assert len(actual) == len(expected)
    assert all(
        isclose(a, b, rel_tol=0, abs_tol=1e-9)
        for a, b in zip(actual, expected)
    )


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    # Initially, only server1 owns the keyspace.
    table = RoutingTable([
        Partition("p1", 0, KEY_SPACE_END, address1)
    ])

    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    server1.start()
    server2.start()

    source_client = ParameterClient(
        address1, worker_id=501
    )

    destination_client = ParameterClient(
        address2, worker_id=700
    )

    try:
        # ---------------------------------------------
        # 1. Create existing source data
        # ---------------------------------------------

        source_client.push(
            [50],
            [10.0, 20.0],
            width=2,
        ).wait(timeout_ms=5000)

        source_client.push(
            [10],
            [4.0, 4.0],
            width=2,
        ).wait(timeout_ms=5000)

        check_values(service1.weights[50], [-1.0, -2.0])
        check_values(service1.weights[10], [-0.4, -0.4])

        print("Initial source state: PASS")

        # ---------------------------------------------
        # 2. Snapshot the moving interval
        # ---------------------------------------------

        snapshot = service1.begin_range_transfer(40, 60)

        service2.stage_incoming_transfer(
            "p1-moved-e2",
            snapshot,
        )

        assert service2._incoming_transfer is not None
        assert 50 not in service2.weights
        assert "p1-moved-e2" not in service2.range_clocks

        print("Destination snapshot staged: PASS")
        print("Staged state not prematurely activated: PASS")

        # Do not allow installation before final changes.
        try:
            service2.install_incoming_transfer()
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Installing before catch-up should fail"
            )

        print("Premature installation rejected: PASS")

        # ---------------------------------------------
        # 3. Continue serving requests on the source
        # ---------------------------------------------

        source_client.push(
            [50],
            [1.0, 2.0],
            width=2,
        ).wait(timeout_ms=5000)

        # An unaffected key also continues changing.
        source_client.push(
            [10],
            [1.0, 1.0],
            width=2,
        ).wait(timeout_ms=5000)

        check_values(service1.weights[50], [-1.1, -2.2])
        check_values(service1.weights[10], [-0.5, -0.5])

        # The staged snapshot still contains old values.
        check_values(
            service2._incoming_transfer["weights"][50],
            [-1.0, -2.0],
        )

        print("Source updates continue during staging: PASS")
        print("Snapshot remains independent: PASS")

        # ---------------------------------------------
        # 4. Freeze the source and transfer final changes
        # ---------------------------------------------

        delta = service1.freeze_range_transfer()

        service2.apply_incoming_delta(delta)

        staged = service2._incoming_transfer

        assert staged["caught_up"]
        check_values(staged["weights"][50], [-1.1, -2.2])

        assert staged["clocks"].clock_for_key(50) == {
            501: 3
        }

        print("Destination final changes applied: PASS")
        print("Destination clocks caught up: PASS")

        # ---------------------------------------------
        # 5. Install the received data
        # ---------------------------------------------

        service2.install_incoming_transfer()

        assert service2._incoming_transfer is None

        check_values(service2.weights[50], [-1.1, -2.2])

        # Key 10 was outside the moving interval.
        assert 10 not in service2.weights

        migrated_clocks = service2.range_clocks[
            "p1-moved-e2"
        ]

        assert migrated_clocks.clock_for_key(50) == {
            501: 3
        }

        assert len(service2.completed_pushes) == 2
        assert service2.completed_pushes.keys() == {
            (501, 1),
            (501, 3),
        }

        assert service2.task_states[(501, 1)] == "DONE"
        assert service2.task_states[(501, 3)] == "DONE"

        print("Destination parameter state installed: PASS")
        print("Destination clock history installed: PASS")
        print("Destination Push history installed: PASS")

        # ---------------------------------------------
        # 6. Ownership must remain unchanged
        # ---------------------------------------------

        # The destination has the data, but it is not
        # yet authoritative for key 50.
        try:
            destination_client.pull(
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
                "Destination should not serve keys before cutover"
            )

        print("Premature destination ownership prevented: PASS")

        # ---------------------------------------------
        # 7. Cancel the source transfer
        # ---------------------------------------------

        # This test checks destination import only.
        # Manager cutover will be implemented separately.
        service1.cancel_range_transfer()

        values = source_client.pull(
            [50]
        ).wait(timeout_ms=5000).values

        check_values(values, [-1.1, -2.2])

        assert service1._transfer is None

        print("Source remains authoritative: PASS")
        print("E1.2 destination import: PASS")

    finally:
        if service1._transfer is not None:
            service1.cancel_range_transfer()

        if service2._incoming_transfer is not None:
            service2.cancel_incoming_transfer()

        source_client.close()
        destination_client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
