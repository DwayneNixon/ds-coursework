
import math
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


class FailureServer(ParameterServer):
    """Simulate selected RPC failures and count requests."""

    def __init__(self):
        super().__init__()

        self.count_lock = threading.Lock()
        self.pull_keys_seen = []
        self.push_keys_seen = []

    def Pull(self, request, context):
        keys = tuple(request.keys)

        with self.count_lock:
            self.pull_keys_seen.append(keys)

        if keys == (42000,):
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Simulated server unavailability"
            )

        if keys == (42010,):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Simulated invalid request"
            )

        return super().Pull(request, context)

    def Push(self, request, context):
        with self.count_lock:
            self.push_keys_seen.append(
                tuple(request.keys)
            )

        return super().Push(request, context)


def failure_name(task):
    """Return the observed failure type of a task."""

    try:
        task.wait(timeout_ms=3000)

    except grpc.RpcError as error:
        return error.code().name

    except Exception as error:
        return type(error).__name__

    raise AssertionError(
        "Expected a failed task, but it succeeded"
    )


def main():
    service = FailureServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=8)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service,
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(
        f"127.0.0.1:{port}",
        worker_id=91
    )

    try:
        # TEST 1: Simulate an unavailable server.
        unavailable = client.pull([42000])

        dependent_a = client.pull(
            [42001],
            deps=[unavailable]
        )

        dependent_b = client.push(
            [42002],
            [1.0, 2.0],
            width=2,
            deps=[dependent_a]
        )

        root_status = failure_name(unavailable)
        child_status = failure_name(dependent_a)
        grandchild_status = failure_name(dependent_b)

        assert root_status == "UNAVAILABLE"
        assert child_status == "DependencyFailedError"
        assert grandchild_status == "DependencyFailedError"

        print("Unavailable prerequisite:", root_status)
        print("Direct dependent:", child_status)
        print("Second-level dependent:", grandchild_status)

        # TEST 2: Invalid argument propagation.
        invalid = client.pull([42010])

        invalid_dependent = client.pull(
            [42011],
            deps=[invalid]
        )

        invalid_status = failure_name(invalid)
        invalid_child_status = failure_name(
            invalid_dependent
        )

        assert invalid_status == "INVALID_ARGUMENT"
        assert invalid_child_status == "DependencyFailedError"

        print("Invalid prerequisite:", invalid_status)
        print("Invalid dependent:", invalid_child_status)

        # TEST 3: Independent tasks still succeed.
        healthy_push = client.push(
            [42003],
            [2.0, 4.0],
            width=2
        )

        response = healthy_push.wait(timeout_ms=3000)
        assert response.accepted

        healthy_pull = client.pull(
            [42003],
            deps=[healthy_push]
        )

        values = list(
            healthy_pull.wait(
                timeout_ms=3000
            ).values
        )

        expected = [-0.2, -0.4]

        assert len(values) == 2

        assert all(
            math.isclose(
                actual,
                target,
                abs_tol=1e-12
            )
            for actual, target in zip(
                values,
                expected
            )
        )

        print("Independent push succeeded: True")
        print("Successful dependency values:", values)

        # TEST 4: Repeated waits return the same failures.
        with service.count_lock:
            pulls_before = list(
                service.pull_keys_seen
            )
            pushes_before = list(
                service.push_keys_seen
            )

        assert failure_name(unavailable) == root_status
        assert failure_name(dependent_a) == child_status
        assert failure_name(dependent_b) == grandchild_status
        assert failure_name(invalid) == invalid_status

        with service.count_lock:
            pulls_after = list(
                service.pull_keys_seen
            )
            pushes_after = list(
                service.push_keys_seen
            )

        assert pulls_before == pulls_after
        assert pushes_before == pushes_after

        print("Repeated waits preserve failures: True")

        # TEST 5: Failed dependencies did not execute RPCs.
        assert (42001,) not in pulls_after
        assert (42002,) not in pushes_after
        assert (42011,) not in pulls_after

        print("Failed dependent RPCs submitted: 0")

        # TEST 6: Every task is terminal.
        tasks = [
            unavailable,
            dependent_a,
            dependent_b,
            invalid,
            invalid_dependent,
            healthy_push,
            healthy_pull
        ]

        assert all(task.done() for task in tasks)

        print("All tasks terminal: True")
        print("All Stage 8C.2 checks passed!")

    finally:
        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
