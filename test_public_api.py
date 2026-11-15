
import math

from parameter_client import ParameterClient


def assert_vectors_close(actual, expected):
    assert len(actual) == len(expected)

    assert all(
        math.isclose(a, b, abs_tol=1e-12)
        for a, b in zip(actual, expected)
    )


def main():
    address = "127.0.0.1:50051"

    worker_a = ParameterClient(
        address,
        worker_id=101
    )

    worker_b = ParameterClient(
        address,
        worker_id=102
    )

    keys = [51000, 51001]

    try:
        # TEST 1: Read zero-initialized parameters.
        initial = worker_a.pull(keys).wait(
            timeout_ms=5000
        )

        initial_values = list(initial.values)

        assert_vectors_close(
            initial_values,
            [0.0, 0.0, 0.0, 0.0]
        )

        print("Initial values:", initial_values)

        # TEST 2: Worker A submits an asynchronous push.
        push_a = worker_a.push(
            keys,
            [10.0, 20.0, 30.0, 40.0],
            width=2
        )

        # This pull must wait for the push.
        dependent_pull = worker_a.pull(
            keys,
            deps=[push_a]
        )

        after_a = list(
            dependent_pull.wait(
                timeout_ms=5000
            ).values
        )

        assert push_a.wait(timeout_ms=5000).accepted

        assert_vectors_close(
            after_a,
            [-1.0, -2.0, -3.0, -4.0]
        )

        print("Worker A Task ID:", push_a.task_id)
        print("After Worker A:", after_a)

        # TEST 3: Worker B updates the same storage.
        push_b = worker_b.push(
            [51000],
            [5.0, 5.0],
            width=2
        )

        assert push_b.wait(timeout_ms=5000).accepted

        # Worker A observes Worker B's update.
        final = list(
            worker_a.pull(keys).wait(
                timeout_ms=5000
            ).values
        )

        assert_vectors_close(
            final,
            [-1.5, -2.5, -3.0, -4.0]
        )

        print("Worker B Task ID:", push_b.task_id)
        print("Final shared values:", final)

        # TEST 4: All returned tasks are terminal.
        assert push_a.done()
        assert dependent_pull.done()
        assert push_b.done()

        print("All tasks completed: True")
        print("All Stage 8D.1 checks passed!")

    finally:
        worker_a.close()
        worker_b.close()


if __name__ == "__main__":
    main()
