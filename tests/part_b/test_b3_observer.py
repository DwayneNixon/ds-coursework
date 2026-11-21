
import math
import threading
from concurrent.futures import ThreadPoolExecutor

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import TargetVector
from harness.observer import ConvergenceObserver


SEED = 42
NUM_KEYS = 4
WIDTH = 2
ITERATIONS = 5
LEARNING_RATE = 0.1

SERVER_ADDRESS = "127.0.0.1:50051"


def main():
    targets = TargetVector(NUM_KEYS, WIDTH, SEED)

    worker_client = ParameterClient(
        SERVER_ADDRESS, worker_id=601
    )

    observer_client = ParameterClient(
        SERVER_ADDRESS, worker_id=999
    )

    progress = {"iteration": 0}
    progress_lock = threading.Lock()

    def update_progress(iteration):
        with progress_lock:
            progress["iteration"] = iteration

    def get_progress():
        with progress_lock:
            return progress["iteration"]

    observer = ConvergenceObserver(
        client=observer_client,
        targets=targets,
        num_keys=NUM_KEYS,
        width=WIDTH,
    )

    stop_event = threading.Event()

    try:
        # Measure the zero-initialized model.
        initial = observer.sample(iteration=0)

        # Run the observer independently of the worker.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                observer.monitor,
                stop_event,
                get_progress,
                0.05,
            )

            try:
                run_worker(
                    client=worker_client,
                    worker_id=601,
                    iterations=ITERATIONS,
                    num_keys=NUM_KEYS,
                    keys_per_iter=NUM_KEYS,
                    width=WIDTH,
                    targets=targets,
                    seed=SEED,
                    compute_ms=80,
                    on_iteration=update_progress,
                )

            finally:
                stop_event.set()

            # Propagate any observer errors.
            future.result()

        # Measure once more after all updates finish.
        final = observer.sample(iteration=ITERATIONS)

        predicted = (
            initial["dist_to_target"]
            * (1.0 - LEARNING_RATE) ** ITERATIONS
        )

        print("\nConvergence observations:")

        for record in observer.records:
            print(
                f"Iteration {record['iteration']} | "
                f"Distance: {record['dist_to_target']:.8f}"
            )

        assert len(observer.records) >= 3
        print("\nPeriodic observation: PASS")

        assert final["dist_to_target"] < initial["dist_to_target"]
        print("Convergence improvement: PASS")

        assert math.isclose(
            final["dist_to_target"],
            predicted,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )

        print("Final analytical distance: PASS")
        print()
        print("Predicted distance:", predicted)
        print("Measured distance: ", final["dist_to_target"])
        print("\nB3.3 observer test: PASS")

    finally:
        worker_client.close()
        observer_client.close()


if __name__ == "__main__":
    main()
