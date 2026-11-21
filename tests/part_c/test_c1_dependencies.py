
from Part_C_Tests.test_c1_two_servers import start_server
from distributed_client import DistributedParameterClient

from parameter_client import (
    ParameterClient,
    Task,
    DependencyFailedError,
)

from routing import Partition, RoutingTable, KEY_SPACE_END


def main():
    server1, address1 = start_server()
    server2, address2 = start_server()

    client = None
    inspector1 = None
    inspector2 = None

    try:
        table = RoutingTable([
            Partition("p1", 0, 40, address1),
            Partition("p2", 40, KEY_SPACE_END, address2),
        ])

        client = DistributedParameterClient(
            table, worker_id=931, value_width=2
        )

        inspector1 = ParameterClient(address1, worker_id=932)
        inspector2 = ParameterClient(address2, worker_id=933)

        # Create an unfinished prerequisite.
        gate = Task()

        push_task = client.push(
            [38, 40],
            [10.0, 20.0, 30.0, 40.0],
            width=2,
            deps=[gate],
        )

        # This Pull depends on the complete distributed Push.
        pull_task = client.pull(
            [38, 40],
            deps=[push_task],
        )

        assert not push_task.done()
        assert not pull_task.done()

        # Inspect both servers before releasing the dependency.
        before1 = list(
            inspector1.pull([38]).wait(timeout_ms=5000).values
        )
        before2 = list(
            inspector2.pull([40]).wait(timeout_ms=5000).values
        )

        assert before1 == [0.0, 0.0]
        assert before2 == [0.0, 0.0]

        print("Dependent requests correctly blocked: PASS")

        # Complete the prerequisite.
        gate._future.set_result(None)

        push_task.wait(timeout_ms=5000)

        values = list(
            pull_task.wait(timeout_ms=5000).values
        )

        assert values == [-1.0, -2.0, -3.0, -4.0]

        print("Dependencies released correctly: PASS")
        print("Distributed Pull after Push: PASS")

        # Now simulate a failed prerequisite.
        failed_gate = Task()
        failed_gate._future.set_exception(
            RuntimeError("Intentional prerequisite failure")
        )

        rejected_push = client.push(
            [38, 40],
            [100.0, 100.0, 100.0, 100.0],
            width=2,
            deps=[failed_gate],
        )

        try:
            rejected_push.wait(timeout_ms=5000)
        except DependencyFailedError:
            print("Failed dependency propagated: PASS")
        else:
            raise AssertionError(
                "Dependent Push should have failed"
            )

        # Failed dependency must not modify either server.
        final_values = list(
            client.pull([38, 40]).wait(
                timeout_ms=5000
            ).values
        )

        assert final_values == [-1.0, -2.0, -3.0, -4.0]

        print("No update after failed dependency: PASS")
        print("C1.6 distributed dependencies: PASS")

    finally:
        if client is not None:
            client.close()
        if inspector1 is not None:
            inspector1.close()
        if inspector2 is not None:
            inspector2.close()

        server1.stop(0)
        server2.stop(0)


if __name__ == "__main__":
    main()
