
import math
from concurrent.futures import ThreadPoolExecutor

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import expected_fixed_weights


SERVER_ADDRESS = "127.0.0.1:50051"

WORKER_IDS = [501, 502, 503]
ITERATIONS = 4
NUM_KEYS = 4
WIDTH = 2
LEARNING_RATE = 0.1


def run_one_worker(worker_id):
    client = ParameterClient(
        SERVER_ADDRESS,
        worker_id=worker_id,
    )

    try:
        run_worker(
            client=client,
            worker_id=worker_id,
            iterations=ITERATIONS,
            num_keys=NUM_KEYS,
            keys_per_iter=NUM_KEYS,
            width=WIDTH,
            targets=None,
            seed=42,
            compute_ms=1,
            gradient_mode="fixed",
        )

    finally:
        client.close()


def main():
    # Run three workers concurrently.
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(run_one_worker, worker_id)
            for worker_id in WORKER_IDS
        ]

        for future in futures:
            future.result()

    print("\nAll workers completed.")

    # Calculate expected weights offline.
    expected = expected_fixed_weights(
        worker_ids=WORKER_IDS,
        iterations=ITERATIONS,
        num_keys=NUM_KEYS,
        width=WIDTH,
        learning_rate=LEARNING_RATE,
    )

    # Read actual server weights.
    observer = ParameterClient(
        SERVER_ADDRESS,
        worker_id=999,
    )

    try:
        result = observer.pull(
            list(range(NUM_KEYS))
        ).wait(timeout_ms=5000)

        actual = list(result.values)

    finally:
        observer.close()

    print("Expected:", expected)
    print("Actual:  ", actual)

    assert len(actual) == len(expected)

    for observed, predicted in zip(actual, expected):
        assert math.isclose(
            observed,
            predicted,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )

    print("\nFixed-gradient oracle: PASS")
    print("Concurrent update correctness: PASS")
    print("\nB3.2 fixed-gradient test: PASS")


if __name__ == "__main__":
    main()
