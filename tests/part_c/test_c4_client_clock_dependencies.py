
import time

from harness.local_servers import make_server
from parameter_client import ParameterClient
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def main():
    # ---------------------------------------------------------
    # SETUP: Two parameter servers and two partitions
    # ---------------------------------------------------------

    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    table = RoutingTable([
        Partition("p1", 0, 40, address1),
        Partition("p2", 40, KEY_SPACE_END, address2),
    ])

    service1.set_ownership(address1, table)
    service2.set_ownership(address2, table)

    # Give dependent requests enough time to wait.
    service1.dependency_timeout = 2.0
    service2.dependency_timeout = 2.0

    server1.start()
    server2.start()

    writer = None
    reader = None
    direct = None

    try:
        writer = DistributedParameterClient(
            table,
            worker_id=1801,
            value_width=2,
        )

        reader = DistributedParameterClient(
            table,
            worker_id=1802,
            value_width=2,
        )

        direct = ParameterClient(
            address1,
            worker_id=1803,
        )

        # =====================================================
        # TEST 1: Distributed Pull waits across both owners
        # =====================================================

        print("\n=== Test 1: Distributed Pull Dependency ===")

        pending = reader.pull(
            [10, 50],
            required_clock={1801: 5},
        )

        time.sleep(0.05)

        print(
            "DEBUG: Is distributed Pull done?",
            pending.done(),
        )

        # If it completed early, inspect the actual result.
        if pending.done():

            print("\nDEBUG: Distributed Pull finished early")

            try:
                result = pending.wait(timeout_ms=100)

                print("DEBUG: Pull unexpectedly succeeded")
                print("Values:", list(result.values))
                print(
                    "Clock segments:",
                    len(result.clocks),
                )

            except Exception as error:

                print("DEBUG: Pull failed")
                print(
                    "Exception type:",
                    type(error).__name__,
                )
                print(
                    "Exception:",
                    repr(error),
                )

                if hasattr(error, "code"):
                    print(
                        "gRPC status:",
                        error.code(),
                    )

                if hasattr(error, "details"):
                    print(
                        "gRPC details:",
                        error.details(),
                    )

            raise AssertionError(
                "Distributed Pull finished before "
                "its required clock advanced. "
                "See DEBUG output above."
            )

        print(
            "Distributed Pull correctly waiting: PASS"
        )

        # Now advance both partitions to iteration 5.
        writer.push(
            [10, 50],
            [1.0, 2.0, 3.0, 4.0],
            width=2,
            iteration=5,
        ).wait(timeout_ms=5000)

        result = pending.wait(timeout_ms=5000)

        expected = [
            -0.1, -0.2,
            -0.3, -0.4,
        ]

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(
                result.values,
                expected,
            )
        )

        assert len(result.values) == len(expected)

        print(
            "Distributed Pull waits across owners: PASS"
        )

        # =====================================================
        # TEST 2: Single-server clock dependency API
        # =====================================================

        print("\n=== Test 2: Single-Server Clock API ===")

        direct_result = direct.pull(
            [10],
            required_clock={1801: 5},
        ).wait(timeout_ms=5000)

        assert abs(
            direct_result.values[0] + 0.1
        ) < 1e-9

        assert abs(
            direct_result.values[1] + 0.2
        ) < 1e-9

        print(
            "Single-server clock dependency API: PASS"
        )

        # =====================================================
        # TEST 3: Distributed Push clock dependency
        # =====================================================

        print("\n=== Test 3: Distributed Push Dependency ===")

        dependent = reader.push(
            [10, 50],
            [1.0, 2.0, 3.0, 4.0],
            width=2,
            iteration=1,
            required_clock={1801: 5},
        )

        push_result = dependent.wait(
            timeout_ms=5000
        )

        assert push_result.accepted

        current = reader.pull(
            [10, 50]
        ).wait(timeout_ms=5000)

        expected = [
            -0.2, -0.4,
            -0.6, -0.8,
        ]

        assert len(current.values) == len(expected)

        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(
                current.values,
                expected,
            )
        )

        print(
            "Distributed Push clock requirement: PASS"
        )

        # =====================================================
        # TEST 4: Clock requirements are per key
        # =====================================================

        print("\n=== Test 4: Per-Key Clock Isolation ===")

        pending = reader.pull(
            [10, 51],
            required_clock={1801: 6},
        )

        time.sleep(0.05)

        assert not pending.done(), (
            "Pull should wait for iteration 6"
        )

        # Update only key 10.
        writer.push(
            [10],
            [1.0, 2.0],
            width=2,
            iteration=6,
        ).wait(timeout_ms=5000)

        time.sleep(0.05)

        # Key 51 has not received iteration 6.
        assert not pending.done(), (
            "Update to key 10 must not satisfy "
            "the requirement for key 51"
        )

        # Now update key 51 as well.
        writer.push(
            [51],
            [5.0, 6.0],
            width=2,
            iteration=6,
        ).wait(timeout_ms=5000)

        result = pending.wait(
            timeout_ms=5000
        )

        assert len(result.values) == 4

        print(
            "Per-key dependency isolation: PASS"
        )

        # =====================================================
        # TEST 5: Invalid clock metadata rejected locally
        # =====================================================

        print("\n=== Test 5: Clock Validation ===")

        invalid_requirements = [
            {1801: 0},
            {-1: 5},
            {1801: -1},
            {1801: 1 << 64},
        ]

        for invalid in invalid_requirements:

            try:
                reader.pull(
                    [10, 50],
                    required_clock=invalid,
                )

            except ValueError:
                pass

            else:
                raise AssertionError(
                    f"Invalid clock accepted: {invalid}"
                )

        print(
            "Clock metadata validation: PASS"
        )

        # =====================================================
        # TEST 6: Existing client behavior is unchanged
        # =====================================================

        print("\n=== Test 6: Legacy Compatibility ===")

        result = reader.pull(
            [10, 50]
        ).wait(timeout_ms=5000)

        assert len(result.values) == 4

        print(
            "Legacy client compatibility: PASS"
        )

        # =====================================================
        # FINAL RESULT
        # =====================================================

        print()
        print("=" * 55)
        print("C4.5B clock dependency client API: PASS")
        print("=" * 55)

    finally:
        # Close all client connections.
        if direct is not None:
            direct.close()

        if writer is not None:
            writer.close()

        if reader is not None:
            reader.close()

        # Stop both parameter servers.
        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
