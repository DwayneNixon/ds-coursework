
from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END
from harness.metrics import measure_operation, make_summary

def main():
    # 1. Create two fresh parameter servers.
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    client = None

    try:
        # 2. Divide the key space between the servers.
        table = RoutingTable([
            Partition("p1", 0, 64, address1),
            Partition("p2", 64, KEY_SPACE_END, address2),
        ])

        # 3. Configure ownership and start both servers.
        service1.set_ownership(address1, table)
        service2.set_ownership(address2, table)

        server1.start()
        server2.start()

        # 4. Connect a distributed worker.
        client = DistributedParameterClient(
            table,
            worker_id=2001,
            value_width=2,
        )

        
        # 5. Repeated sparse Pull/Push workload.
        keys = [10, 70]
        gradients = [1.0, 2.0, 3.0, 4.0]
        iterations = 5
        records = []

        for iteration in range(iterations):
            # Pull the current parameter values.
            pull_task, result = measure_operation(
                create_task=lambda: client.pull(keys),
                worker_id=2001,
                iteration=iteration + 1,
                op="pull",
                n_keys=len(keys),
                target=keys,
                record_callback=records.append,
            )

            # Check values before the next update.
            expected = [
                -0.1 * iteration,
                -0.2 * iteration,
                -0.3 * iteration,
                -0.4 * iteration,
            ]

            assert len(result.values) == len(expected)
            assert all(
                abs(actual - wanted) < 1e-9
                for actual, wanted in zip(result.values, expected)
            )

            # Apply the same gradients to the same sparse keys.
            push_task, _ = measure_operation(
                create_task=lambda: client.push(
                    keys,
                    gradients,
                    width=2,
                ),
                worker_id=2001,
                iteration=iteration + 1,
                op="push",
                n_keys=len(keys),
                record_callback=records.append,
            )

            print(f"Iteration {iteration + 1}/{iterations}: PASS")

        # 6. Verify the final parameters.
        final_values = client.pull(keys).wait(timeout_ms=5000).values

        expected_final = [-0.5, -1.0, -1.5, -2.0]

        assert len(final_values) == len(expected_final)
        assert all(
            abs(actual - wanted) < 1e-9
            for actual, wanted in zip(final_values, expected_final)
        )

        print("Repeated sparse Pull/Push: PASS")
        print("Final parameter values: PASS")
        print("D1.1B: PASS")

        # 7. Summarise communication measurements.
        assert len(records) == iterations * 2
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

        print("\n--- D1.1C Baseline Measurements ---")
        print(f"Logical operations: {len(records)}")
        print(f"Request bytes: {request_bytes}")
        print(f"Response bytes: {response_bytes}")
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

        print("D1.1C communication measurements: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
