
from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END
from harness.metrics import measure_operation, make_summary
import sys

USE_KEY_CACHE = "--cache" in sys.argv
USE_COMPRESSION = "--compress" in sys.argv

ITERATIONS = 200
WORKER_ID = 2001
WIDTH = 2

# 64 sparse keys: 32 on each server.
SPLIT_KEY = 1 << 63

KEYS = (
    [(1 << 60) + 2 * i for i in range(32)]
    + [(1 << 63) + (1 << 60) + 2 * i for i in range(32)]
)

# Two gradient components for each key.
GRADIENTS = [value for key in KEYS for value in (1.0, 2.0)]


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    client = None

    try:
        table = RoutingTable([
            Partition("p1", 0, SPLIT_KEY, address1),
            Partition("p2", SPLIT_KEY, KEY_SPACE_END, address2),
        ])

        service1.set_ownership(address1, table)
        service2.set_ownership(address2, table)

        server1.start()
        server2.start()

        client = DistributedParameterClient(
            table,
            worker_id=WORKER_ID,
            value_width=WIDTH,
            key_caching=USE_KEY_CACHE,
            value_compression=USE_COMPRESSION,
        )

        records = []
        physical_rpcs = 0

        # Repeated sparse workload without caching.
        for iteration in range(1, ITERATIONS + 1):

            pull_task, result = measure_operation(
                create_task=lambda: client.pull(KEYS),
                worker_id=WORKER_ID,
                iteration=iteration,
                op="pull",
                n_keys=len(KEYS),
                target=KEYS,
                record_callback=records.append,
            )

            assert len(result.values) == len(GRADIENTS)

            push_task, _ = measure_operation(
                create_task=lambda: client.push(
                    KEYS,
                    GRADIENTS,
                    width=WIDTH,
                ),
                worker_id=WORKER_ID,
                iteration=iteration,
                op="push",
                n_keys=len(KEYS),
                record_callback=records.append,
            )

            # Each logical operation should contact both servers.
            assert len(pull_task.tasks) == 2
            assert len(push_task.tasks) == 2

            physical_rpcs += (
                len(pull_task.tasks) + len(push_task.tasks)
            )

        # Independently verify the final SGD result.
        final_values = client.pull(KEYS).wait(
            timeout_ms=5000
        ).values

        expected = [
            -0.1 * ITERATIONS * gradient
            for gradient in GRADIENTS
        ]

        assert len(final_values) == len(expected)

        assert all(
            abs(actual - wanted) < 1e-8
            for actual, wanted in zip(final_values, expected)
        )

        # Summarise the recorded workload only.
        assert len(records) == ITERATIONS * 2
        assert all(record["status"] == "OK" for record in records)

        summary = make_summary(records, [])

        request_bytes = sum(
            record["bytes_out"] for record in records
        )
        response_bytes = sum(
            record["bytes_in"] for record in records
        )

        assert summary["total_bytes"] == (
            request_bytes + response_bytes
        )

        print("\n--- D1.1D Sparse Baseline ---")
        print(f"Keys per operation: {len(KEYS)}")
        print(f"Iterations: {ITERATIONS}")
        print(f"Logical operations: {len(records)}")
        print(f"Physical RPCs: {physical_rpcs}")

        print(f"Request bytes: {request_bytes}")
        print(f"Response bytes: {response_bytes}")
        print(
            f"\nCaching: {'ENABLED' if USE_KEY_CACHE else 'DISABLED'}"
        )
        print(
            f"Compression: {'ENABLED' if USE_COMPRESSION else 'DISABLED'}"
        )
        print(f"Total bytes: {summary['total_bytes']}")

        print(
            f"Push P50: {summary['p50_push_latency_ms']:.3f} ms"
        )
        print(
            f"Push P99: {summary['p99_push_latency_ms']:.3f} ms"
        )
        print(
            f"Pull P50: {summary['p50_pull_latency_ms']:.3f} ms"
        )
        print(
            f"Pull P99: {summary['p99_pull_latency_ms']:.3f} ms"
        )

        print("Final parameter correctness: PASS")
        print("D1.1D sparse baseline: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
