
import time
import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from Part_C_Tests.test_c2_server_ownership import make_server
from parameter_client import ParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END


def requirement(worker, iteration):
    return [
        pb.ClockEntry(
            worker_id=worker,
            counter=iteration,
        )
    ]


def expect_clock_timeout(operation):
    try:
        operation()
    except grpc.RpcError as error:
        assert error.code() == (
            grpc.StatusCode.DEADLINE_EXCEEDED
        )
        assert "CLOCK_DEPENDENCY_TIMEOUT" in error.details()
    else:
        raise AssertionError(
            "Expected unsatisfied clock dependency"
        )


def main():
    server, service, address = make_server()

    table = RoutingTable([
        Partition("p1", 0, KEY_SPACE_END, address)
    ])

    service.set_ownership(address, table)
    service.dependency_timeout = 0.3

    server.start()

    channel = grpc.insecure_channel(address)
    stub = pb_grpc.ParameterServerStub(channel)

    writer = ParameterClient(
        address, worker_id=1701
    )

    try:
        # 1. Older requests still work without clock requirements.
        result = writer.pull([10]).wait(timeout_ms=5000)
        assert list(result.values) == [0.0, 0.0]

        print("Legacy requests remain compatible: PASS")

        # 2. A dependent Pull must wait for iteration 3.
        pull_request = pb.PullRequest(
            keys=[10],
            required_clock=requirement(1701, 3),
        )

        pending = stub.Pull.future(
            pull_request, timeout=5
        )

        time.sleep(0.05)
        assert not pending.done()

        writer.push(
            [10], [1.0, 2.0],
            width=2, iteration=3,
        ).wait(timeout_ms=5000)

        result = pending.result(timeout=5)

        assert list(result.values) == [-0.1, -0.2]

        print("Pull waits for required clock: PASS")

        # 3. An update to key 10 must not satisfy key 11.
        expect_clock_timeout(
            lambda: stub.Pull(
                pb.PullRequest(
                    keys=[11],
                    required_clock=requirement(1701, 3),
                ),
                timeout=5,
            )
        )

        print("Sparse key dependency isolation: PASS")

        # 4. Dense range requires every included key.
        writer.push(
            [11], [1.0, 2.0],
            width=2, iteration=3,
        ).wait(timeout_ms=5000)

        dense = stub.Pull(
            pb.PullRequest(
                key_range=pb.KeyRange(begin=10, end=12),
                required_clock=requirement(1701, 3),
            ),
            timeout=5,
        )

        assert list(dense.keys) == [10, 11]

        print("Dense range clock coverage: PASS")

        # 5. A Push must not modify weights before
        # its required clock becomes satisfied.
        dependent_push = pb.PushRequest(
            keys=[12],
            gradients=[3.0, 4.0],
            width=2,
            task_id=pb.TaskId(
                worker_id=1702,
                sequence=1,
            ),
            iteration=1,
            required_clock=requirement(1701, 4),
        )

        pending_push = stub.Push.future(
            dependent_push, timeout=5
        )

        time.sleep(0.05)

        assert not pending_push.done()
        assert 12 not in service.weights

        # This independent Push advances the clock.
        writer.push(
            [12], [1.0, 2.0],
            width=2, iteration=4,
        ).wait(timeout_ms=5000)

        result = pending_push.result(timeout=5)
        assert result.accepted

        # Both updates must now be present.
        actual = service.weights[12]

        assert abs(actual[0] - (-0.4)) < 1e-9
        assert abs(actual[1] - (-0.6)) < 1e-9

        print("Dependent Push waits without partial writes: PASS")

        # 6. A requirement from an unknown worker
        # cannot be satisfied by someone else's update.
        expect_clock_timeout(
            lambda: stub.Pull(
                pb.PullRequest(
                    keys=[12],
                    required_clock=requirement(1999, 1),
                ),
                timeout=5,
            )
        )

        print("Wrong-worker clock dependency rejected: PASS")
        print("C4.5A clock-aware dependency enforcement: PASS")

    finally:
        writer.close()
        channel.close()
        server.stop(0).wait()


if __name__ == "__main__":
    main()
