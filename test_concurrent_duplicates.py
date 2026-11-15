
import math
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


class BarrierServer(ParameterServer):
    """Test server that synchronizes incoming pushes."""

    def __init__(self):
        super().__init__()

        # Wait for all requests in each test scenario.
        self.barriers = {
            8000: threading.Barrier(12),
            8100: threading.Barrier(2)
        }

    def Push(self, request, context):
        # Synchronize requests before processing them.
        if len(request.keys) == 1:
            key = request.keys[0]

            if key in self.barriers:
                try:
                    self.barriers[key].wait(timeout=10)
                except threading.BrokenBarrierError:
                    context.abort(
                        grpc.StatusCode.INTERNAL,
                        "Test synchronization failed"
                    )

        # Use the real server implementation.
        return super().Push(request, context)


def make_request(key, gradients, sequence):
    """Construct a push with an explicit Task ID."""

    return pb.PushRequest(
        keys=[key],
        gradients=gradients,
        width=2,
        task_id=pb.TaskId(
            worker_id=7,
            sequence=sequence
        )
    )


def main():
    service = BarrierServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=16)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service, server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}"
        ) as channel:

            stub = pb_grpc.ParameterServerStub(channel)

            def send_push(request):
                try:
                    response = stub.Push(
                        request, timeout=15
                    )

                    status = (
                        "OK" if response.accepted
                        else "NOT_ACCEPTED"
                    )

                except grpc.RpcError as error:
                    status = error.code().name

                return status, list(request.gradients)

            def read_weights(key):
                response = stub.Pull(
                    pb.PullRequest(keys=[key]),
                    timeout=5
                )
                return list(response.values)

            # TEST 1: Twelve identical concurrent requests.
            requests = [
                make_request(
                    key=8000,
                    gradients=[10.0, 20.0],
                    sequence=100
                )
                for _ in range(12)
            ]

            with ThreadPoolExecutor(max_workers=12) as pool:
                results = list(pool.map(send_push, requests))

            statuses = [status for status, _ in results]

            assert statuses == ["OK"] * 12

            weights = read_weights(8000)

            assert weights == [-1.0, -2.0]

            print("Identical concurrent requests:", len(requests))
            print("Successful acknowledgements:", statuses.count("OK"))
            print("Weights after duplicates:", weights)
            print("Duplicate suppression correct: True")

            # TEST 2: Same ID, different request contents.
            conflicting_requests = [
                make_request(
                    key=8100,
                    gradients=[10.0, 20.0],
                    sequence=101
                ),
                make_request(
                    key=8100,
                    gradients=[30.0, 40.0],
                    sequence=101
                )
            ]

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(send_push, conflicting_requests)
                )

            statuses = [status for status, _ in results]

            assert sorted(statuses) == [
                "INVALID_ARGUMENT", "OK"
            ]

            # Find which request was accepted.
            winning_gradients = next(
                gradients
                for status, gradients in results
                if status == "OK"
            )

            expected = [
                -0.1 * gradient
                for gradient in winning_gradients
            ]

            actual = read_weights(8100)

            assert len(actual) == len(expected)

            assert all(
                math.isclose(
                    a, b,
                    rel_tol=1e-12,
                    abs_tol=1e-12
                )
                for a, b in zip(actual, expected)
            )

            print("Conflicting requests:", len(results))
            print("Conflict outcomes:", sorted(statuses))
            print("Winning gradients:", winning_gradients)
            print("Weights match accepted request: True")

            print("All Stage 6B checks passed!")

    finally:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
