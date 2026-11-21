
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


class PausedPullServer(ParameterServer):
    """Pause the first Pull while other tasks are created."""

    def __init__(self):
        super().__init__()

        self.first_pull_started = threading.Event()
        self.release_first_pull = threading.Event()

        self.count_lock = threading.Lock()
        self.pull_calls = 0

    def Pull(self, request, context):
        with self.count_lock:
            self.pull_calls += 1
            call_number = self.pull_calls

        if call_number == 1:
            self.first_pull_started.set()

            # Keep the first RPC pending.
            self.release_first_pull.wait(timeout=15)

        return super().Pull(request, context)


def describe_task(name, task):
    """Observe a task without assuming its error type."""

    try:
        task.wait(timeout_ms=3000)
        outcome = "SUCCESS"

    except grpc.RpcError as error:
        outcome = error.code().name

    except Exception as error:
        outcome = type(error).__name__

    finished = task.done()

    print(
        f"{name}: outcome={outcome}, "
        f"done={finished}"
    )

    return finished


def main():
    service = PausedPullServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(
        f"127.0.0.1:{port}",
        worker_id=91
    )

    try:
        # Start a Pull that will pause inside the server.
        task_a = client.pull([41000])

        started = service.first_pull_started.wait(
            timeout=5
        )

        assert started, (
            "First Pull did not reach the server"
        )

        # B cannot begin until A succeeds.
        task_b = client.pull(
            [41000],
            deps=[task_a]
        )

        # C cannot begin until B succeeds.
        task_c = client.pull(
            [41000],
            deps=[task_b]
        )

        with service.count_lock:
            calls_before = service.pull_calls

        print("First Pull reached server:", started)
        print("Task A pending:", not task_a.done())
        print("Task B pending:", not task_b.done())
        print("Task C pending:", not task_c.done())
        print(
            "Pull RPCs before shutdown:",
            calls_before
        )

        # Exactly one RPC should have reached the server.
        assert calls_before == 1
        assert not task_a.done()
        assert not task_b.done()
        assert not task_c.done()

        # Close the client while A remains blocked.
        client.close()

        print("Client close returned: True")

        # Inspect how each task behaves after shutdown.
        finished_a = describe_task("Task A", task_a)
        finished_b = describe_task("Task B", task_b)
        finished_c = describe_task("Task C", task_c)

        with service.count_lock:
            calls_after = service.pull_calls

        print(
            "All tasks terminal after shutdown:",
            all([
                finished_a,
                finished_b,
                finished_c
            ])
        )

        print(
            "Dependent RPCs reached server:",
            calls_after - 1
        )

        print("Stage 8C.1 diagnostic completed.")

    finally:
        # Ensure that the test server cannot stay blocked.
        service.release_first_pull.set()

        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
