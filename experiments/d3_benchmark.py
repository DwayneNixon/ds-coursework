
import argparse
import csv
import time
from pathlib import Path

from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from routing import Partition, RoutingTable, KEY_SPACE_END
from harness.metrics import distributed_payload_sizes, percentile


ROUNDS = 100
TRIALS = 3
GROUPS = [[i, 64 + i] for i in range(8)]
ALL_KEYS = [key for group in GROUPS for key in group]


def run_batch(client, batched):
    start = time.perf_counter_ns()

    if batched:
        task = client.pull_batch(GROUPS)
        result = task.wait(timeout_ms=5000)
        values = result.values
        tasks = [task]

    else:
        tasks = []
        values = []

        for group in GROUPS:
            task = client.pull(group)
            result = task.wait(timeout_ms=5000)

            tasks.append(task)
            values.append(list(result.values))

    latency_ms = (time.perf_counter_ns() - start) / 1_000_000

    expected = [-0.1, -0.2, -0.3, -0.4]

    assert len(values) == len(GROUPS)

    for group_values in values:
        assert len(group_values) == len(expected)
        assert all(
            abs(a - b) < 1e-9
            for a, b in zip(group_values, expected)
        )

    sizes = [
        distributed_payload_sizes(task, "pull")
        for task in tasks
    ]

    return {
        "latency_ms": latency_ms,
        "request_bytes": sum(out for out, _ in sizes),
        "response_bytes": sum(inc for _, inc in sizes),
        "rpcs": sum(len(task.tasks) for task in tasks),
    }


def run_trial(number, cache, compression):
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
            worker_id=2600 + number,
            value_width=2,
            key_caching=cache,
            value_compression=compression,
        )

        # Establish identical nonzero parameters.
        client.push(
            ALL_KEYS,
            [1.0, 2.0, 3.0, 4.0] * len(GROUPS),
            width=2,
            iteration=1,
        ).wait(timeout_ms=5000)

        # Warm up both access patterns.
        # These operations are excluded from measurements.
        run_batch(client, batched=False)
        run_batch(client, batched=True)

        records = {"unbatched": [], "batched": []}

        for i in range(ROUNDS):
            # Alternate ordering to reduce execution-order bias.
            modes = (
                [False, True] if i % 2 == 0
                else [True, False]
            )

            for batched in modes:
                name = "batched" if batched else "unbatched"

                records[name].append(
                    run_batch(client, batched)
                )

        print(f"\n--- D3 Trial {number} ---")
        print(f"Caching: {cache}")
        print(f"Compression: {compression}")

        trial_rows = []
        for name, data in records.items():
            latencies = [r["latency_ms"] for r in data]

            request_bytes = sum(
                r["request_bytes"] for r in data
            )
            response_bytes = sum(
                r["response_bytes"] for r in data
            )
            rpcs = sum(r["rpcs"] for r in data)

            print(f"\n{name.upper()}")
            print(f"Logical Pulls: {ROUNDS * len(GROUPS)}")
            print(f"Physical RPCs: {rpcs}")
            print(f"Request bytes: {request_bytes}")
            print(f"Response bytes: {response_bytes}")
            print(
                f"Total bytes: {request_bytes + response_bytes}"
            )
            print(f"P50 batch latency: {percentile(latencies, 50):.3f} ms")
            print(f"P99 batch latency: {percentile(latencies, 99):.3f} ms")
            trial_rows.append({
                "trial": number,
                "mode": name,
                "caching": cache,
                "compression": compression,
                "logical_pulls": ROUNDS * len(GROUPS),
                "physical_rpcs": rpcs,
                "request_bytes": request_bytes,
                "response_bytes": response_bytes,
                "total_bytes": request_bytes + response_bytes,
                "p50_latency_ms": percentile(latencies, 50),
                "p99_latency_ms": percentile(latencies, 99),
            })

        baseline = sum(
            r["request_bytes"] + r["response_bytes"]
            for r in records["unbatched"]
        )

        optimized = sum(
            r["request_bytes"] + r["response_bytes"]
            for r in records["batched"]
        )

        saving = 100 * (1 - optimized / baseline)

        assert sum(r["rpcs"] for r in records["unbatched"]) == 1600
        assert sum(r["rpcs"] for r in records["batched"]) == 200
        assert optimized < baseline

        print(f"\nPayload reduction: {saving:.2f}%")
        print("Correctness and RPC reduction: PASS")
        return trial_rows

    finally:
        if client is not None:
            client.close()

        server1.stop(0).wait()
        server2.stop(0).wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", action="store_true")
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--output", default="results/d3_benchmark/comparison.csv")
    args = parser.parse_args()

    rows = []
    for trial in range(1, TRIALS + 1):
        rows.extend(run_trial(trial, args.cache, args.compress))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved D3 comparison: {output}")
    print("\\nD3 benchmark: PASS")


if __name__ == "__main__":
    main()
