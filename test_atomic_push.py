
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


def main():
    # Count how many times the update rule runs.
    state = {"calls": 0}

    def faulty_update(weights, gradients, learning_rate):
        state["calls"] += 1

        # Deliberately fail on the second parameter.
        if state["calls"] == 2:
            raise RuntimeError(
                "Simulated failure on second key"
            )

        # Normal SGD for the first parameter.
        return [
            weight - learning_rate * gradient
            for weight, gradient in zip(weights, gradients)
        ]

    service = ParameterServer()

    service.register_update_rule(
        "faulty",
        faulty_update
    )

    service.set_update_rule("faulty")

    server = grpc.server(
        ThreadPoolExecutor(max_workers=4)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        service,
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        with grpc.insecure_channel(
            f"127.0.0.1:{port}"
        ) as channel:

            stub = pb_grpc.ParameterServerStub(channel)

            def read_weights():
                response = stub.Pull(
                    pb.PullRequest(
                        keys=[30000, 30001]
                    ),
                    timeout=5
                )

                return list(response.values)

            # Read initial parameter values.
            before = read_weights()

            print("Weights before:", before)

            # One logical push updating TWO keys.
            request = pb.PushRequest(
                keys=[30000, 30001],
                gradients=[
                    10.0, 20.0,
                    30.0, 40.0
                ],
                width=2,
                task_id=pb.TaskId(
                    worker_id=91,
                    sequence=1
                )
            )

            # Submit the push.
            try:
                response = stub.Push(
                    request,
                    timeout=5
                )

                status = (
                    "OK"
                    if response.accepted
                    else "NOT_ACCEPTED"
                )

            except grpc.RpcError as error:
                status = error.code().name

            print("Push status:", status)
            print("Update rule calls:", state["calls"])

            # Read both keys after the failed push.
            after = read_weights()

            print("Weights after:", after)

            # Determine whether the push was atomic.
            if status != "OK" and before == after:
                print("Atomicity preserved: True")
                print("No partial updates detected.")

            elif status != "OK" and before != after:
                print("Atomicity preserved: False")
                print("Partial update detected!")

            else:
                print(
                    "Unexpected result: failing rule "
                    "did not produce an RPC error."
                )

            print("Stage 8B diagnostic completed.")

            # The custom update rule must have failed on its
            # second invocation.
            assert state["calls"] == 2

            # An unexpected server-side rule failure should
            # produce a deliberate gRPC error.
            assert status == "INTERNAL", (
                f"Expected INTERNAL, received {status}"
            )

            # Neither parameter may change after the failed push.
            assert before == after, (
                "A failed multi-key push modified parameter state"
            )

            print("All Stage 8B atomicity checks passed!")

    finally:
        server.stop(grace=0)

if __name__ == "__main__":
    main()
