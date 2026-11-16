
import math
import random

from parameter_client import ParameterClient
from harness.worker import run_worker, sample_keys


def main():
    # Check seeded key selection independently
    rng_a = random.Random(42)
    rng_b = random.Random(42)

    samples_a = [sample_keys(rng_a, 10, 3) for _ in range(5)]
    samples_b = [sample_keys(rng_b, 10, 3) for _ in range(5)]

    assert samples_a == samples_b
    print("Seed reproducibility: PASS")

    # B1 smoke-test configuration
    targets = {
        0: [1.0, -2.0],
        1: [3.0, 4.0],
    }

    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=101,
    )

    try:
        run_worker(
            client=client,
            worker_id=101,
            iterations=3,
            num_keys=2,
            keys_per_iter=2,
            width=2,
            targets=targets,
            seed=42,
            compute_ms=0,
        )

        # Read the final parameters
        result = client.pull([0, 1]).wait(timeout_ms=5000)
        actual = list(result.values)

        expected = [0.271, -0.542, 0.813, 1.084]

        print("\nExpected:", expected)
        print("Actual:  ", actual)

        assert len(actual) == len(expected)

        for observed, target in zip(actual, expected):
            assert math.isclose(
                observed, target,
                rel_tol=1e-9,
                abs_tol=1e-9,
            )

        print("\nB1 smoke test: PASS")

    finally:
        client.close()


if __name__ == "__main__":
    main()
