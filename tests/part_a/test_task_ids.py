
from concurrent import futures

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


def read_weights(stub):
    """Read the parameters used by this test."""
    response = stub.Pull(
        pb.PullRequest(keys=[7000])
    )
    return list(response.values)


def main():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        ParameterServer(),
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}"
        ) as channel:

            stub = pb_grpc.ParameterServerStub(channel)

            # First logical operation from worker 7.
            first_request = pb.PushRequest(
                keys=[7000],
                gradients=[10.0, 20.0],
                width=2,
                task_id=pb.TaskId(
                    worker_id=7,
                    sequence=1
                )
            )

            # TEST 1: Apply a new push.
            first_ack = stub.Push(first_request)

            assert first_ack.accepted
            assert read_weights(stub) == [-1.0, -2.0]

            print("First push:", read_weights(stub))

            # TEST 2: Retry the exact same request.
            duplicate_ack = stub.Push(first_request)

            assert duplicate_ack.accepted
            assert read_weights(stub) == [-1.0, -2.0]

            print("After duplicate:", read_weights(stub))

            # TEST 3: Reuse the ID with different contents.
            conflicting_request = pb.PushRequest(
                keys=[7000],
                gradients=[50.0, 60.0],
                width=2,
                task_id=pb.TaskId(
                    worker_id=7,
                    sequence=1
                )
            )

            try:
                stub.Push(conflicting_request)
            except grpc.RpcError as error:
                assert (
                    error.code()
                    == grpc.StatusCode.INVALID_ARGUMENT
                )
                print("Conflicting task:", error.code().name)
            else:
                raise AssertionError(
                    "Conflicting Task ID was accepted"
                )

            assert read_weights(stub) == [-1.0, -2.0]

            # TEST 4: A new sequence is a new operation.
            second_request = pb.PushRequest(
                keys=[7000],
                gradients=[10.0, 20.0],
                width=2,
                task_id=pb.TaskId(
                    worker_id=7,
                    sequence=2
                )
            )

            stub.Push(second_request)

            assert read_weights(stub) == [-2.0, -4.0]
            print("New task:", read_weights(stub))

            # TEST 5: Another worker can use sequence 1.
            other_worker_request = pb.PushRequest(
                keys=[7000],
                gradients=[10.0, 20.0],
                width=2,
                task_id=pb.TaskId(
                    worker_id=8,
                    sequence=1
                )
            )

            stub.Push(other_worker_request)

            assert read_weights(stub) == [-3.0, -6.0]

            print("Different worker:", read_weights(stub))
            print("All Stage 5A checks passed!")

    finally:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
