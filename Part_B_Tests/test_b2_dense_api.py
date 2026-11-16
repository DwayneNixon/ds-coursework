
import math

from parameter_client import ParameterClient


def main():
    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=301,
    )

    try:
        # Native dense range: keys 100, 101, 102, 103
        dense_range = range(100, 104)
        sparse_keys = [100, 101, 102, 103]

        # Confirm zero initialization
        initial = client.pull(dense_range).wait(
            timeout_ms=5000
        )

        assert list(initial.values) == [0.0] * 8
        print("Dense initialization: PASS")

        # One gradient vector per key, width 2
        gradients = [1.0, -2.0] * 4

        push_task = client.push(
            dense_range,
            gradients,
            width=2,
        )
        push_task.wait(timeout_ms=5000)

        # Compare dense and sparse reads
        dense_values = list(
            client.pull(dense_range).wait(
                timeout_ms=5000
            ).values
        )

        sparse_values = list(
            client.pull(sparse_keys).wait(
                timeout_ms=5000
            ).values
        )

        assert dense_values == sparse_values
        print("Dense/sparse equivalence: PASS")

        # Expected SGD result with learning rate 0.1
        expected = [-0.1, 0.2] * 4

        assert all(
            math.isclose(a, b, abs_tol=1e-9)
            for a, b in zip(dense_values, expected)
        )

        print("Dense SGD correctness: PASS")
        print("\nB2 dense API integration: PASS")

    finally:
        client.close()


if __name__ == "__main__":
    main()
