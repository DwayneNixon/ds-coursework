
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


def additive_update(weights, gradients, learning_rate):
    """Add each gradient value directly to its weight."""

    return [
        weight + gradient
        for weight, gradient in zip(weights, gradients)
    ]


def main():
    service = ParameterServer()

    # Register the alternative rule before starting.
    service.register_update_rule(
        "additive",
        additive_update
    )

    server = grpc.server(
        ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service,
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    address = f"127.0.0.1:{port}"

    client_a = ParameterClient(address, worker_id=7)
    client_b = ParameterClient(address, worker_id=8)

    def read_weights():
        task = client_a.pull([13000])
        response = task.wait(timeout_ms=3000)
        return list(response.values)

    try:
        # TEST 1: Default rule is SGD.
        assert service.active_update_rule == "sgd"

        task = client_a.push(
            [13000],
            [10.0, 20.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted
        assert read_weights() == [-1.0, -2.0]

        print("Default rule:", service.active_update_rule)
        print("After SGD:", read_weights())

        # TEST 2: Client A selects the additive rule.
        selected = client_a.set_update_rule("additive")

        assert selected == "additive"
        assert service.active_update_rule == "additive"

        print("Remote selection:", selected)

        # Client B now submits an update.
        task = client_b.push(
            [13000],
            [10.0, 20.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted
        assert read_weights() == [9.0, 18.0]

        print("After additive:", read_weights())

        # TEST 3: Unknown rule must be rejected.
        try:
            client_a.set_update_rule("unknown")

        except grpc.RpcError as error:
            assert (
                error.code()
                == grpc.StatusCode.INVALID_ARGUMENT
            )

            print("Unknown rule:", error.code().name)

        else:
            raise AssertionError(
                "Unknown rule was accepted"
            )

        assert service.active_update_rule == "additive"

        # TEST 4: Empty rule name must be rejected.
        try:
            client_a.set_update_rule("")

        except grpc.RpcError as error:
            assert (
                error.code()
                == grpc.StatusCode.INVALID_ARGUMENT
            )

            print("Empty rule:", error.code().name)

        else:
            raise AssertionError(
                "Empty rule name was accepted"
            )

        # TEST 5: Client B restores SGD.
        selected = client_b.set_update_rule("sgd")

        assert selected == "sgd"

        task = client_a.push(
            [13000],
            [10.0, 20.0],
            width=2
        )

        assert task.wait(timeout_ms=3000).accepted
        assert read_weights() == [8.0, 16.0]

        print("Restored rule:", selected)
        print("After restoring SGD:", read_weights())

        # TEST 6: An earlier task remains deduplicated.
        duplicate = pb.PushRequest(
            keys=[13000],
            gradients=[10.0, 20.0],
            width=2,
            task_id=pb.TaskId(
                worker_id=8,
                sequence=1
            )
        )

        response = client_a.stub.Push(duplicate)

        assert response.accepted
        assert read_weights() == [8.0, 16.0]

        print("Duplicate still suppressed: True")

        print("All Stage 7B checks passed!")

    finally:
        client_a.close()
        client_b.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
