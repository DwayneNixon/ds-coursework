
import math
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import parameter_server_pb2_grpc as pb_grpc

from server import ParameterServer
from parameter_client import ParameterClient


def main():
    server = grpc.server(
        ThreadPoolExecutor(max_workers=20)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        ParameterServer(),
        server
    )

    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    client = ParameterClient(
        f"127.0.0.1:{port}",
        worker_id=7
    )

    worker_count = 20
    pushes_per_worker = 5
    key = 8000

    # Synchronize the workers' initial submissions.
    barrier = threading.Barrier(worker_count)

    def worker(worker_index):
        task_ids = []

        # Wait until all workers are ready.
        barrier.wait(timeout=10)

        for _ in range(pushes_per_worker):
            task = client.push(
                [key],
                [1.0, 2.0],
                width=2
            )

            response = task.wait(timeout_ms=5000)

            assert response.accepted
            task_ids.append(task.task_id)

        return task_ids

    try:
        # Run all workers concurrently.
        with ThreadPoolExecutor(
            max_workers=worker_count
        ) as pool:
            batches = list(
                pool.map(worker, range(worker_count))
            )

        # Collect all generated task identifiers.
        task_ids = [
            task_id
            for batch in batches
            for task_id in batch
        ]

        expected_count = worker_count * pushes_per_worker

        assert len(task_ids) == expected_count
        assert len(set(task_ids)) == expected_count

        # Read the final parameter vector.
        response = client.pull([key]).wait(
            timeout_ms=5000
        )

        actual = list(response.values)

        expected = [-10.0, -20.0]

        assert len(actual) == len(expected)

        assert all(
            math.isclose(
                a, b,
                rel_tol=1e-12,
                abs_tol=1e-10
            )
            for a, b in zip(actual, expected)
        )

        print("Workers:", worker_count)
        print("Pushes per worker:", pushes_per_worker)
        print("Total pushes:", expected_count)
        print("Unique Task IDs:", len(set(task_ids)))
        print("Expected weights:", expected)
        print("Actual weights:", actual)
        print("Concurrent aggregation correct: True")

        print("All Stage 6A checks passed!")

    finally:
        client.close()
        server.stop(grace=0)


if __name__ == "__main__":
    main()
