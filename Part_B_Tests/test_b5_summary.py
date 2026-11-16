
import json
import math

from harness.metrics import (
    percentile,
    rolling_throughput,
    make_summary,
    write_summary_json,
)


def main():

    # --------------------------------------------------
    # 1. Test percentile calculations
    # --------------------------------------------------

    values = [10, 20, 30, 40, 50]

    assert percentile(values, 50) == 30

    assert math.isclose(
        percentile(values, 99),
        49.6,
    )

    print("Latency percentiles: PASS")

    # --------------------------------------------------
    # 2. Create synthetic operation records
    # --------------------------------------------------

    start_ns = 1_800_000_000_000_000_000

    # Three pulls and three completed pushes.
    operations = []

    samples = [
        ("pull", 0.0, 10),
        ("push", 0.2, 20),
        ("pull", 0.5, 30),
        ("push", 0.8, 40),
        ("pull", 1.5, 50),
        ("push", 1.7, 60),
    ]

    for op, time_s, latency in samples:
        operations.append({
            "ts_ns": start_ns + int(time_s * 1e9),
            "op": op,
            "latency_ms": latency,
            "status": "OK",
            "bytes_out": 50,
            "bytes_in": 100,
        })

    # --------------------------------------------------
    # 3. Test 1-second sliding throughput
    # --------------------------------------------------

    rates = rolling_throughput(operations)

    assert rates == [1, 2, 2]

    print("Sliding-window throughput: PASS")

    # --------------------------------------------------
    # 4. Test summary generation
    # --------------------------------------------------

    convergence = [
        {
            "ts_ns": start_ns,
            "dist_to_target": 1.0,
        },
        {
            "ts_ns": start_ns + 2_000_000_000,
            "dist_to_target": 0.8,
        },
    ]

    summary = make_summary(
        operations,
        convergence,
    )

    assert summary["throughput"]["completed_pushes"] == 3
    assert summary["throughput"]["peak_pushes_per_s"] == 2

    assert summary["p50_push_latency_ms"] == 40
    assert summary["p50_pull_latency_ms"] == 30

    assert math.isclose(
        summary["p99_push_latency_ms"],
        59.6,
    )

    assert summary["total_bytes"] == 900
    assert summary["final_distance"] == 0.8

    print("Summary statistics: PASS")

    # --------------------------------------------------
    # 5. Test missing byte measurements
    # --------------------------------------------------

    operations[0]["bytes_in"] = None

    incomplete = make_summary(
        operations,
        convergence,
    )

    assert incomplete["total_bytes"] is None

    print("Missing byte measurements: PASS")

    # --------------------------------------------------
    # 6. Export summary.json
    # --------------------------------------------------

    path = write_summary_json(
        summary,
        "results/b5_summary",
    )

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        saved = json.load(file)

    assert saved == summary

    print("JSON summary export: PASS")
    print("\nOutput:", path)

    print("\nB5.4 summary statistics: PASS")


if __name__ == "__main__":
    main()
