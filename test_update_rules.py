
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


def additive_update(weights, gradients, learning_rate):
    """Add the incoming gradient to each weight."""
    return [
        weight + gradient
        for weight, gradient in zip(weights, gradients)
    ]


def make_push(sequence):
    return pb.PushRequest(
        keys=[12000],
        gradients=[10.0, 20.0],
        width=2,
        task_id=pb.TaskId(
            worker_id=7,
            sequence=sequence
        )
    )


def read_weights(stub):
    response = stub.Pull(
        pb.PullRequest(keys=[12000])
    )
    return list(response.values)


def main():
    service = ParameterServer()

    server = grpc.server(
        ThreadPoolExecutor(max_workers=4)
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

            # TEST 1: SGD should be the default.
            assert service.active_update_rule == "sgd"

            first_request = make_push(sequence=1)
            assert stub.Push(first_request).accepted

            assert read_weights(stub) == [-1.0, -2.0]

            print("Default rule:", service.active_update_rule)
            print("After SGD:", read_weights(stub))

            # TEST 2: Register and select a new rule.
            service.register_update_rule(
                "additive", additive_update
            )

            service.set_update_rule("additive")

            second_request = make_push(sequence=2)
            assert stub.Push(second_request).accepted

            assert read_weights(stub) == [9.0, 18.0]

            print("Selected rule:", service.active_update_rule)
            print("After additive:", read_weights(stub))

            # TEST 3: Switch back to SGD.
            service.set_update_rule("sgd")

            third_request = make_push(sequence=3)
            assert stub.Push(third_request).accepted

            assert read_weights(stub) == [8.0, 16.0]

            print("Restored rule:", service.active_update_rule)
            print("After restoring SGD:", read_weights(stub))

            # TEST 4: Old requests are still deduplicated.
            assert stub.Push(second_request).accepted
            assert read_weights(stub) == [8.0, 16.0]

            print("Duplicate still suppressed: True")

            # TEST 5: Unknown rules are rejected.
            try:
                service.set_update_rule("unknown")
            except ValueError:
                print("Unknown rule rejected: True")
            else:
                raise AssertionError(
                    "Unknown update rule was accepted"
                )

            print("All Stage 7A checks passed!")

    finally:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
