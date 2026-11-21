
import threading
from concurrent import futures

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from parameter_client import ParameterClient, TaskWaitTimeout


class SlowPullServer(pb_grpc.ParameterServerServicer):
    """A test-only server that pauses pull requests."""

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()

    def Pull(self, request, context):
        # Tell the test that the request reached the server.
        self.started.set()

        # Pause until the test allows this request to finish.
        if not self.release.wait(timeout=5):
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Test request was not released"
            )

        # Return two zero-valued parameters per key.
        return pb.PullResponse(
            keys=request.keys,
            values=[0.0] * (len(request.keys) * 2),
            width=2
        )


def main():
    # Create a temporary gRPC server for this test.
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2)
    )

    slow_service = SlowPullServer()

    pb_grpc.add_ParameterServerServicer_to_server(
        slow_service,
        server
    )

    # Port 0 lets the operating system choose a free port.
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(f"127.0.0.1:{port}")

    try:
        # Test 1: Submit an asynchronous request.
        task = client.pull([100])

        # Wait until the server has received the request.
        assert slow_service.started.wait(timeout=2)

        print("Slow pull reached server: True")

        # The server is paused, so the task cannot be done.
        assert not task.done()
        print("Task is pending before release: True")

        # Test 2: A short wait should time out.
        try:
            task.wait(timeout_ms=50)

        except TaskWaitTimeout:
            print("Short wait: TIMEOUT")

        else:
            raise AssertionError("Expected a wait timeout")

        # Test 3: The task must still be running.
        assert not task.done()
        print("Task still running after wait timeout: True")

        # Allow the server to finish the original request.
        slow_service.release.set()

        # Test 4: Wait again for the SAME task.
        response = task.wait(timeout_ms=2000)

        assert list(response.keys) == [100]
        assert list(response.values) == [0.0, 0.0]
        assert response.width == 2

        print("Second wait returned values:", list(response.values))

        assert task.done()
        print("Task finished: True")

        # Test 5: Repeated waits return the same result.
        second_response = task.wait(timeout_ms=2000)

        assert list(second_response.values) == [0.0, 0.0]

        print("Repeated wait returned same result: True")
        print("All Stage 3B checks passed!")

    finally:
        # Make sure the server cannot remain paused.
        slow_service.release.set()

        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
