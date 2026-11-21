
import math

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import TargetVector, expected_weight


SEED = 42
NUM_KEYS = 4
WIDTH = 2
ITERATIONS = 5
LEARNING_RATE = 0.1


def main():
    targets_a = TargetVector(NUM_KEYS, WIDTH, SEED)
    targets_b = TargetVector(NUM_KEYS, WIDTH, SEED)

    # Same seed must generate the same target.
    for key in range(NUM_KEYS):
        assert targets_a[key] == targets_b[key]

    print("Target reproducibility: PASS")

    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=401,
    )

    try:
        # Each iteration updates every parameter key.
        run_worker(
            client=client,
            worker_id=401,
            iterations=ITERATIONS,
            num_keys=NUM_KEYS,
            keys_per_iter=NUM_KEYS,
            width=WIDTH,
            targets=targets_a,
            seed=SEED,
            compute_ms=0,
        )

        keys = list(range(NUM_KEYS))

        result = client.pull(keys).wait(timeout_ms=5000)
        actual = list(result.values)

        expected = []

        for key in keys:
            for target_value in targets_a[key]:
                expected.append(
                    expected_weight(
                        target_value,
                        LEARNING_RATE,
                        ITERATIONS,
                    )
                )

        assert len(actual) == len(expected)

        for observed, predicted in zip(actual, expected):
            assert math.isclose(
                observed,
                predicted,
                rel_tol=1e-9,
                abs_tol=1e-9,
            )

        print("Analytical oracle: PASS")

        # Compare measured convergence with the formula.
        initial_distance_sq = 0.0
        final_distance_sq = 0.0

        for index, key in enumerate(keys):
            for component in range(WIDTH):
                position = index * WIDTH + component
                target = targets_a[key][component]

                initial_distance_sq += target ** 2
                final_distance_sq += (actual[position] - target) ** 2

        initial_distance = math.sqrt(initial_distance_sq)
        final_distance = math.sqrt(final_distance_sq)

        predicted_distance = (
            (1.0 - LEARNING_RATE) ** ITERATIONS
            * initial_distance
        )

        assert math.isclose(
            final_distance,
            predicted_distance,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )

        print("Convergence distance: PASS")
        print()
        print("Predicted distance:", predicted_distance)
        print("Measured distance: ", final_distance)
        print("\nB3.1 correctness oracle: PASS")

    finally:
        client.close()


if __name__ == "__main__":
    main()
