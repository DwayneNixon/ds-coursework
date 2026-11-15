
import math
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer


def make_faulty_rule(mode):
    """Create an update rule that fails on its second key."""

    calls = {"count": 0}

    def update(weights, gradients, learning_rate):
        calls["count"] += 1

        if mode == "mutation":
            # Deliberately modify the input list.
            weights[0] = 9999.0

            if calls["count"] == 2:
                raise RuntimeError("Simulated update failure")

        result = [
            weight - learning_rate * gradient
            for weight, gradient in zip(weights, gradients)
        ]

        if calls["count"] == 2:
            if mode == "wrong_width":
                return result[:1]

            if mode == "nan":
                return [math.nan, result[1]]

        return result

    return update, calls


def make_push(sequence, gradients):
    return pb.PushRequest(
        keys=[30000, 30001],
        gradients=gradients,
        width=2,
        task_id=pb.TaskId(
            worker_id=91,
            sequence=sequence
        )
    )


def main():
    service = ParameterServer()

    # Register three different faulty rules.
    scenarios = {}

    for name in ("wrong_width", "nan", "mutation"):
        rule, calls = make_faulty_rule(name)
        service.register_update_rule(name, rule)
        scenarios[name] = calls

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

            def read_weights():
                response = stub.Pull(
                    pb.PullRequest(
                        keys=[30000, 30001]
                    ),
                    timeout=5
                )
                return list(response.values)

            def send_push(request):
                try:
                    response = stub.Push(
                        request,
                        timeout=5
                    )

                    return (
                        "OK" if response.accepted
                        else "NOT_ACCEPTED"
                    )

                except grpc.RpcError as error:
                    return error.code().name

            # Establish nonzero parameter values.
            initial = make_push(
                sequence=1,
                gradients=[10.0, 20.0, 30.0, 40.0]
            )

            assert send_push(initial) == "OK"

            baseline = read_weights()

            assert baseline == [
                -1.0, -2.0, -3.0, -4.0
            ]

            print("Initial weights:", baseline)

            # Test all three failure scenarios.
            last_failed_request = None

            for sequence, name in enumerate(
                ("wrong_width", "nan", "mutation"),
                start=2
            ):
                service.set_update_rule(name)

                request = make_push(
                    sequence=sequence,
                    gradients=[1.0, 2.0, 3.0, 4.0]
                )

                status = send_push(request)
                after = read_weights()

                calls = scenarios[name]["count"]

                assert status == "INTERNAL", (
                    f"{name}: unexpected status {status}"
                )

                assert calls == 2, (
                    f"{name}: expected two rule calls"
                )

                assert after == baseline, (
                    f"{name}: parameter state changed"
                )

                # Failed operations must not be marked complete.
                assert (91, sequence) not in (
                    service.completed_pushes
                )

                print(
                    f"{name}: status={status}, "
                    f"calls={calls}, unchanged=True"
                )

                last_failed_request = request

            # Restore a working rule.
            service.set_update_rule("sgd")

            # Retry the EXACT request that previously failed.
            retry_status = send_push(last_failed_request)

            assert retry_status == "OK"

            after_retry = read_weights()

            expected = [
                original - 0.1 * gradient
                for original, gradient in zip(
                    baseline,
                    [1.0, 2.0, 3.0, 4.0]
                )
            ]

            assert all(
                math.isclose(
                    actual,
                    target,
                    abs_tol=1e-12
                )
                for actual, target in zip(
                    after_retry,
                    expected
                )
            )

            print("Failed task retry accepted: True")
            print("Weights after retry:", after_retry)

            # Once successful, the same Task ID must
            # not apply another update.
            assert send_push(last_failed_request) == "OK"
            assert read_weights() == after_retry

            print("Successful retry deduplicated: True")
            print("All Stage 8B safety checks passed!")

    finally:
        server.stop(grace=0)


if __name__ == "__main__":
    main()
