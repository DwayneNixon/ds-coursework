
import math
import random
from concurrent.futures import ThreadPoolExecutor

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.workload import WorkloadGenerator


# Experiment configuration
SEED = 42
NUM_KEYS = 32
WIDTH = 2
WORKERS = 4
ITERATIONS = 5
KEYS_PER_ITER = 8
STRAGGLER_FRACTION = 0.25

SERVER_ADDRESS = "127.0.0.1:50051"


def main():
    # Every key has the same simple target vector.
    targets = {
        key: [1.0, -1.0]
        for key in range(NUM_KEYS)
    }

    worker_ids = list(range(201, 201 + WORKERS))

    # Select stragglers reproducibly.
    rng = random.Random(f"{SEED}:stragglers")

    straggler_count = int(WORKERS * STRAGGLER_FRACTION)
    stragglers = set(
        rng.sample(worker_ids, straggler_count)
    )

    print("Workers:", worker_ids)
    print("Stragglers:", sorted(stragglers))
    print()

    def run_one_worker(worker_id):
        # Each worker needs its own client and workload.
        client = ParameterClient(
            SERVER_ADDRESS,
            worker_id=worker_id,
        )

        try:
            workload = WorkloadGenerator(
                num_keys=NUM_KEYS,
                seed=SEED,
                worker_id=worker_id,
                mode="sparse",
                keys_per_iter=KEYS_PER_ITER,
                zipf_s=1.1,
                scatter=True,
                compute_median_ms=20,
                compute_sigma=0.4,
                is_straggler=(worker_id in stragglers),
                straggler_slowdown=5,
            )

            run_worker(
                client=client,
                worker_id=worker_id,
                iterations=ITERATIONS,
                num_keys=NUM_KEYS,
                keys_per_iter=KEYS_PER_ITER,
                width=WIDTH,
                targets=targets,
                seed=SEED,
                workload=workload,
            )

        finally:
            client.close()

    # Execute all workers concurrently.
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(run_one_worker, worker_id)
            for worker_id in worker_ids
        ]

        # Propagate any worker exceptions to the driver.
        for future in futures:
            future.result()

    print("\nAll workers completed.")

    # Read the final parameters through a separate client.
    observer = ParameterClient(
        SERVER_ADDRESS,
        worker_id=999,
    )

    try:
        keys = list(range(NUM_KEYS))
        result = observer.pull(keys).wait(timeout_ms=5000)
        final_values = list(result.values)

        assert len(final_values) == NUM_KEYS * WIDTH

        assert all(
            math.isfinite(value)
            for value in final_values
        )

        assert any(
            abs(value) > 1e-9
            for value in final_values
        )

        print("Final parameter values:", len(final_values))
        print("Sample values:", final_values[:10])
        print("\nB2 multiworker integration: PASS")

    finally:
        observer.close()


if __name__ == "__main__":
    main()
