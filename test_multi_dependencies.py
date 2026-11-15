
import threading
from concurrent import futures

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import (
    ParameterClient,
    DependencyFailedError,
)


class ControlledServer(ParameterServer):
    """Test server that can pause selected push requests."""

    def __init__(self):
        super().__init__()

        self.started = {
            key: threading.Event()
            for key in [1000, 1001, 1002]
        }

        self.release = {
            key: threading.Event()
            for key in [1000, 1001, 1002]
        }

        self.pull_count = 0
        self.count_lock = threading.Lock()

    def Push(self, request, context):
        for key in request.keys:
            if key in self.started:
                self.started[key].set()

                if not self.release[key].wait(timeout=10):
                    context.abort(
                        grpc.StatusCode.UNAVAILABLE,
                        "Test push was not released"
                    )

        return super().Push(request, context)

    def Pull(self, request, context):
        with self.count_lock:
            self.pull_count += 1

        return super().Pull(request, context)

    def get_pull_count(self):
        with self.count_lock:
            return self.pull_count


def main():
    service = ControlledServer()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(f"127.0.0.1:{port}")

    try:
        # TEST 1: Both prerequisites must succeed.

        task_a = client.push(
            [1000], [10.0, 20.0], width=2
        )
        task_b = client.push(
            [1001], [30.0, 40.0], width=2
        )

        assert service.started[1000].wait(timeout=3)
        assert service.started[1001].wait(timeout=3)

        task_c = client.pull(
            [1000, 1001],
            deps=[task_a, task_b]
        )

        assert not task_c.done()
        assert service.get_pull_count() == 0

        # Release only Task A.
        service.release[1000].set()
        assert task_a.wait(timeout_ms=3000).accepted

        assert not task_c.done()
        assert service.get_pull_count() == 0

        print("Task C waits for both dependencies: True")

        # Release Task B.
        service.release[1001].set()
        assert task_b.wait(timeout_ms=3000).accepted

        values = list(
            task_c.wait(timeout_ms=3000).values
        )

        assert values == [-1.0, -2.0, -3.0, -4.0]

        print("Multiple dependency result:", values)

        # TEST 2: One failed prerequisite blocks execution.

        previous_pulls = service.get_pull_count()

        delayed_task = client.push(
            [1002], [10.0, 20.0], width=2
        )

        assert service.started[1002].wait(timeout=3)

        invalid_task = client.push(
            [1003], [1.0], width=2
        )

        blocked_task = client.pull(
            [1002, 1003],
            deps=[delayed_task, invalid_task]
        )

        try:
            invalid_task.wait(timeout_ms=3000)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            print("Failed prerequisite: INVALID_ARGUMENT")
        else:
            raise AssertionError("Expected invalid push")

        try:
            blocked_task.wait(timeout_ms=3000)
        except DependencyFailedError:
            print("Dependent task: DEPENDENCY_FAILED")
        else:
            raise AssertionError("Expected dependency failure")

        # Even after the other prerequisite succeeds,
        # the failed dependent task must not execute.
        service.release[1002].set()
        assert delayed_task.wait(timeout_ms=3000).accepted

        assert service.get_pull_count() == previous_pulls
        print("Failed dependency prevented execution: True")

        # TEST 3: Dependency chain A -> B -> C.

        first = client.push(
            [1010], [5.0, 10.0], width=2
        )

        second = client.pull([1010], deps=[first])
        third = client.pull([1010], deps=[second])

        chain_result = list(
            third.wait(timeout_ms=3000).values
        )

        assert chain_result == [-0.5, -1.0]

        print("Dependency chain result:", chain_result)
        print("All Stage 4C checks passed!")

    finally:
        for event in service.release.values():
            event.set()

        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
