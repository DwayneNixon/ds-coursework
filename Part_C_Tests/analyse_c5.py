
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median


INPUT = Path("results/c5_evaluation/summary.csv")

MODES = [
    "sequential",
    "bounded_1",
    "bounded_2",
    "bounded_4",
    "eventual",
]

TRIALS = 3


def values(rows, field):
    return [float(row[field]) for row in rows]


def main():
    with INPUT.open(newline="", encoding="utf-8") as file:
        records = list(csv.DictReader(file))

    groups = defaultdict(list)

    for record in records:
        key = (
            int(record["straggler_ms"]),
            record["mode"],
        )
        groups[key].append(record)

    assert len(records) == 30, (
        f"Expected 30 scenarios, found {len(records)}"
    )

    for slow_ms in (0, 60):
        print(f"\n=== Straggler delay: {slow_ms} ms ===")

        print(
            f"{'Mode':12} | {'Median rate':>11} | "
            f"{'Rate range':>19} | {'Max lag':>7} | "
            f"{'Final dist.':>11} | {'Fast finish':>11}"
        )

        for mode in MODES:
            rows = groups[(slow_ms, mode)]

            assert len(rows) == TRIALS, (
                f"Expected {TRIALS} trials for "
                f"{mode}, slow={slow_ms}"
            )

            rates = values(rows, "pushes_per_s")
            lags = values(rows, "max_lag")
            distances = values(rows, "final_distance")

            fast_times = values(
                rows, "fast_worker_finish_s"
            )

            slow_times = values(
                rows, "slow_worker_finish_s"
            )

            print(
                f"{mode:12} | "
                f"{median(rates):11.2f} | "
                f"{min(rates):7.2f}-{max(rates):7.2f} | "
                f"{int(max(lags)):7d} | "
                f"{median(distances):11.6f} | "
                f"{median(fast_times):10.3f}s"
            )

            # Check that repeated experiments
            # still respect their consistency bounds.
            if mode == "sequential":
                assert max(lags) == 0

            if mode.startswith("bounded_"):
                tau = int(mode.split("_")[1])
                assert max(lags) <= tau

            assert all(
                distance >= 0
                for distance in distances
            )

            assert all(
                fast <= slow + 0.05
                for fast, slow in zip(
                    fast_times, slow_times
                )
            )

    print("\nAll 30 scenarios accounted for: PASS")
    print("Repeated consistency bounds: PASS")
    print("C5.3 repeated-trial analysis: PASS")


if __name__ == "__main__":
    main()
