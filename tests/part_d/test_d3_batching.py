
from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END
from harness.metrics import distributed_payload_sizes


def main():
    server1, service1, address1 = make_server()
    server2, service2, address2 = make_server()

    client = None

    try:
        table = RoutingTable([
            Partition("p1", 0, 64, address1),
            Partition("p2", 64, KEY_SPACE_END, address2),
        ])

        service1.set_ownership(address1, table)
        service2.set_ownership(address2, table)

        server1.start()
        server2.start()

        client = DistributedParameterClient(
            table,
            worker_id=2501,
            value_width=2,
        )

        # Eight independent requests.
        # Each contains one key from each server.
        groups = [[i, 64 + i] for i in range(8)]
        all_keys = [key for group in groups for key in group]

        # Establish nonzero parameter values.
        client.push(
            all_keys,
            [1.0, 2.0, 3.0, 4.0] * len(groups),
            width=2,
            iteration=1,
        ).wait(timeout_ms=5000)

        # 1. Original approach: eight separate Pulls.
        baseline_tasks = []
        baseline_values = []

        for group in groups:
            task = client.pull(group)
            result = task.wait(timeout_ms=5000)

            baseline_tasks.append(task)
            baseline_values.append(list(result.values))

        baseline_rpcs = sum(
            len(task.tasks) for task in baseline_tasks
        )

        assert baseline_rpcs == 16
        print("Unbatched Pulls: PASS")

        # 2. New approach: one batched Pull.
        batched_task = client.pull_batch(groups)
        batched_result = batched_task.wait(timeout_ms=5000)

        assert len(batched_task.tasks) == 2
        assert batched_result.values == baseline_values

        print("Batched Pull correctness: PASS")

        # 3. Verify actual expected parameter values.
        expected_group = [-0.1, -0.2, -0.3, -0.4]

        for values in batched_result.values:
            assert all(
                abs(actual - expected) < 1e-9
                for actual, expected in zip(
                    values, expected_group
                )
            )

        print("Batched parameter values: PASS")

        # 4. Compare serialized Protobuf traffic.
        baseline_bytes = sum(
            sum(distributed_payload_sizes(task, "pull"))
            for task in baseline_tasks
        )

        batched_bytes = sum(
            distributed_payload_sizes(batched_task, "pull")
        )

        assert batched_bytes < baseline_bytes

        print(f"Original physical RPCs: {baseline_rpcs}")
        print(f"Batched physical RPCs: {len(batched_task.tasks)}")

        print(f"Original payload bytes: {baseline_bytes}")
        print(f"Batched payload bytes: {batched_bytes}")

        saving = 100 * (1 - batched_bytes / baseline_bytes)
        print(f"Payload reduction: {saving:.2f}%")

        print("D3 request batching: PASS")

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


if __name__ == "__main__":
    main()
