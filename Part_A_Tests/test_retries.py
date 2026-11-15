
import threading
from concurrent import futures

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


class LostAcknowledgementServer(ParameterServer):
    """Test server that loses its first push acknowledgement."""

    def __init__(self):
        super().__init__()

        self.push_count = 0
        self.count_lock = threading.Lock()

    def Push(self, request, context):
        # First, process the update normally.
        response = super().Push(request, context)

        with self.count_lock:
            self.push_count += 1
            lose_ack = self.push_count == 1

        if lose_ack:
            # The update succeeded, but the client gets
            # an error instead of the acknowledgement.
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "Simulated lost acknowledgement"
            )

        return response


def main():
    service = LostAcknowledgementServer()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(
        f"127.0.0.1:{port}",
        worker_id=7
    )

    try:
        # TEST 1: The server applies a push,
        # but its acknowledgement is unavailable.
        original = client.push(
            [9000],
            [10.0, 20.0],
            width=2
        )

        assert original.task_id == (7, 1)

        try:
            original.wait(timeout_ms=3000)

        except grpc.RpcError as error:
            assert error.code() == grpc.StatusCode.UNAVAILABLE
            print("Original RPC:", error.code().name)

        else:
            raise AssertionError(
                "Expected the simulated acknowledgement failure"
            )

        # The server should already have applied the update.
        before_retry = list(
            client.pull([9000])
            .wait(timeout_ms=3000).values
        )

        assert before_retry == [-1.0, -2.0]

        print("Weights after failed acknowledgement:", before_retry)

        # TEST 2: Retry with the SAME Task ID.
        retry = client.retry_push(original)

        assert retry.task_id == original.task_id
        assert retry.wait(timeout_ms=3000).accepted

        after_retry = list(
            client.pull([9000])
            .wait(timeout_ms=3000).values
        )

        assert after_retry == [-1.0, -2.0]

        print("Retry Task ID:", retry.task_id)
        print("Weights after retry:", after_retry)

        # The server received two attempts.
        assert service.push_count == 2

        print("RPC attempts received:", service.push_count)

        # TEST 3: A successful push should not be retried.
        try:
            client.retry_push(retry)
        except ValueError:
            print("Successful retry rejected: True")
        else:
            raise AssertionError(
                "Retrying a successful task was allowed"
            )

        # TEST 4: A new logical push gets a NEW Task ID.
        new_task = client.push(
            [9000],
            [10.0, 20.0],
            width=2
        )

        assert new_task.task_id == (7, 2)
        assert new_task.wait(timeout_ms=3000).accepted

        final_values = list(
            client.pull([9000])
            .wait(timeout_ms=3000).values
        )

        assert final_values == [-2.0, -4.0]

        print("New Task ID:", new_task.task_id)
        print("Final values:", final_values)

        print("All Stage 5C checks passed!")

    finally:
        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
