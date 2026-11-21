
"""Part E1.2 — Source snapshot and concurrent-change tracking."""

import time
from math import isclose

from harness.local_servers import make_server
from parameter_client import ParameterClient
from range_clocks import RangeClocks
from routing import Partition, RoutingTable, KEY_SPACE_END


def check_values(actual, expected):
    assert len(actual) == len(expected)
    assert all(
        isclose(a, b, rel_tol=0, abs_tol=1e-9)
        for a, b in zip(actual, expected)
    )


def main():
    server, service, address = make_server()

    table = RoutingTable([
        Partition("p1", 0, KEY_SPACE_END, address)
    ])

    service.set_ownership(address, table)
    server.start()

    client = ParameterClient(address, worker_id=501)

    try:
        # --------------------------------------------------
        # 1. Create existing parameter and clock state
        # --------------------------------------------------

        client.push(
            [50], [10.0, 20.0], width=2
        ).wait(timeout_ms=5000)

        client.push(
            [10], [4.0, 4.0], width=2
        ).wait(timeout_ms=5000)

        check_values(service.weights[50], [-1.0, -2.0])
        check_values(service.weights[10], [-0.4, -0.4])

        print("Initial parameter state: PASS")

        # --------------------------------------------------
        # 2. Begin transferring [40, 60)
        # --------------------------------------------------

        snapshot = service.begin_range_transfer(40, 60)

        assert snapshot["begin"] == 40
        assert snapshot["end"] == 60
        assert set(snapshot["weights"]) == {50}

        check_values(
            snapshot["weights"][50],
            [-1.0, -2.0],
        )

        initial_clocks = RangeClocks.from_segments(
            40, 60, snapshot["clock_segments"]
        )

        assert initial_clocks.clock_for_key(50) == {
            501: 1
        }

        # Only the Push affecting key 50 belongs
        # entirely to the moving interval.
        assert len(snapshot["completed_pushes"]) == 1

        print("Consistent initial snapshot: PASS")

        # --------------------------------------------------
        # 3. Continue serving updates during the copy
        # --------------------------------------------------

        client.push(
            [50], [1.0, 2.0], width=2
        ).wait(timeout_ms=5000)

        check_values(
            client.pull([50]).wait(
                timeout_ms=5000
            ).values,
            [-1.1, -2.2],
        )

        print("Push/Pull during snapshot transfer: PASS")

        # The original snapshot must remain unchanged.
        check_values(
            snapshot["weights"][50],
            [-1.0, -2.0],
        )

        assert initial_clocks.clock_for_key(50) == {
            501: 1
        }

        print("Snapshot isolation: PASS")

        # --------------------------------------------------
        # 4. Freeze and capture changes
        # --------------------------------------------------

        delta = service.freeze_range_transfer()

        assert set(delta["weights"]) == {50}

        check_values(
            delta["weights"][50],
            [-1.1, -2.2],
        )

        updated_clocks = RangeClocks.from_segments(
            40, 60, delta["clock_segments"]
        )

        assert updated_clocks.clock_for_key(50) == {
            501: 3
        }

        assert len(delta["completed_pushes"]) == 1

        print("Post-snapshot changes captured: PASS")
        print("Updated vector clocks captured: PASS")
        print("Completed Push history captured: PASS")

        # --------------------------------------------------
        # 5. Verify selective freezing
        # --------------------------------------------------

        # Key 50 is in the frozen interval.
        blocked = client.push(
            [50], [1.0, 1.0], width=2
        )

        time.sleep(0.15)

        assert not blocked.done(), (
            "Push to frozen range should be waiting"
        )

        print("Moving interval temporarily fenced: PASS")

        # Key 10 is outside the moving interval.
        # It must remain available.
        unaffected = client.push(
            [10], [1.0, 1.0], width=2
        )

        assert unaffected.wait(timeout_ms=5000).accepted

        check_values(
            service.weights[10],
            [-0.5, -0.5],
        )

        print("Unaffected range remains writable: PASS")

        # --------------------------------------------------
        # 6. Cancel and resume
        # --------------------------------------------------

        service.cancel_range_transfer()

        assert blocked.wait(timeout_ms=5000).accepted

        check_values(
            service.weights[50],
            [-1.2, -2.3],
        )

        assert service._transfer is None

        print("Transfer cancellation resumes requests: PASS")
        print("E1.2 source transfer foundations: PASS")

    finally:
        if service._transfer is not None:
            service.cancel_range_transfer()

        client.close()
        server.stop(0).wait()


if __name__ == "__main__":
    main()
