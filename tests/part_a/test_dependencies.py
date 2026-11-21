
import threading
from concurrent import futures

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import (
    ParameterClient,
    TaskWaitTimeout,
    DependencyFailedError,
)


class ControlledServer(ParameterServer):
    """Test server that pauses a push to key 100."""

    def __init__(self):
        super().__init__()

        self.push_started = threading.Event()
        self.release_push = threading.Event()
        self.pull_received = threading.Event()

    def Push(self, request, context):
        if 100 in request.keys:
            self.push_started.set()

            if not self.release_push.wait(timeout=5):
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "Test push was not released"
                )

        return super().Push(request, context)

    def Pull(self, request, context):
        self.pull_received.set()
        return super().Pull(request, context)


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
        # Test 1: The dependent pull must wait.
        task_a = client.push(
            [100], [10.0, 20.0], width=2
        )

        assert service.push_started.wait(timeout=2)

        task_b = client.pull([100], deps=[task_a])

        try:
            task_b.wait(timeout_ms=50)
        except TaskWaitTimeout:
            print("Dependent task waits: True")
        else:
            raise AssertionError(
                "Dependent task executed too early"
            )

        assert not service.pull_received.is_set()
        print("No premature pull RPC: True")

        # Allow Task A to finish.
        service.release_push.set()

        assert task_a.wait(timeout_ms=2000).accepted

        values = list(
            task_b.wait(timeout_ms=2000).values
        )

        assert values == [-1.0, -2.0]
        print("Dependent pull result:", values)

        # Test 2: A failed prerequisite prevents execution.
        service.pull_received.clear()

        invalid_push = client.push(
            [200], [1.0], width=2
        )

        blocked_pull = client.pull(
            [200], deps=[invalid_push]
        )

        try:
            invalid_push.wait(timeout_ms=2000)
        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            print("Prerequisite error:", error.code().name)
        else:
            raise AssertionError("Expected invalid push to fail")

        try:
            blocked_pull.wait(timeout_ms=2000)
        except DependencyFailedError:
            print("Dependent error: DEPENDENCY_FAILED")
        else:
            raise AssertionError(
                "Dependent task should have failed"
            )

        assert not service.pull_received.wait(timeout=0.1)
        print("Failed dependency prevented pull: True")

        print("All Stage 4B checks passed!")

    finally:
        service.release_push.set()
        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
