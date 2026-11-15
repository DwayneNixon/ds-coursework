
import math
import grpc

from parameter_client import ParameterClient, Task


def main():
    client = ParameterClient()

    try:
        keys = [100, 101]
        gradients = [10.0, 20.0, 30.0, 40.0]

        # Test 1: Pull returns a Task object.
        pull_task = client.pull(keys)

        print("Pull returned Task:", isinstance(pull_task, Task))

        before = list(
            pull_task.wait(timeout_ms=2000).values
        )

        print("Before push:", before)

        # Test 2: Push also returns a Task.
        push_task = client.push(
            target=keys,
            gradients=gradients,
            width=2
        )

        print("Push returned Task:", isinstance(push_task, Task))

        acknowledgement = push_task.wait(timeout_ms=2000)

        print("Push accepted:", acknowledgement.accepted)
        print("Push finished:", push_task.done())

        # Test 3: Verify the numerical update.
        dense_task = client.pull(range(100, 102))

        after = list(
            dense_task.wait(timeout_ms=2000).values
        )

        expected = [
            weight - 0.1 * gradient
            for weight, gradient in zip(before, gradients)
        ]

        assert len(after) == len(expected)
        assert all(
            math.isclose(actual, target)
            for actual, target in zip(after, expected)
        )

        print("After push:", after)
        print("SGD update correct: True")

        # Test 4: Waiting again must not reapply the push.
        second_ack = push_task.wait(timeout_ms=2000)

        assert second_ack.accepted
        assert list(
            client.pull(keys).wait(timeout_ms=2000).values
        ) == after

        print("Repeated wait safe: True")

        # Test 5: RPC errors must propagate through wait().
        invalid_task = client.push(
            target=keys,
            gradients=[1.0],
            width=2
        )

        try:
            invalid_task.wait(timeout_ms=2000)
        except grpc.RpcError as error:
            print("Invalid push:", error.code().name)
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
        else:
            raise AssertionError("Invalid push was accepted")

        print("All Stage 3 checks passed!")

    finally:
        client.close()


if __name__ == "__main__":
    main()
