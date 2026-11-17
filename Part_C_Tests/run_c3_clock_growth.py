
import csv
import random
from pathlib import Path

from range_clocks import RangeClocks


KEY_COUNT = 10_000
BLOCK_SIZE = 1_000
OPERATIONS = 400
SEED = 42

CHECKPOINTS = {0, 50, 100, 200, 300, 400}


def count_entries(clocks):
    """Count stored worker-counter entries."""
    return sum(
        len(clock)
        for _, _, clock in clocks.snapshot()
    )


def main():
    dense = RangeClocks(0, KEY_COUNT)
    sparse = RangeClocks(0, KEY_COUNT)

    dense_rng = random.Random(SEED)

    # Random distinct keys make the fragmentation
    # experiment reproducible.
    sparse_rng = random.Random(SEED + 1)
    sparse_keys = sparse_rng.sample(
        range(KEY_COUNT), OPERATIONS
    )

    records = []

    def record_progress(operations):
        records.append({
            "operations": operations,
            "range_segments": dense.count(),
            "single_key_segments": sparse.count(),
            "range_entries": count_entries(dense),
            "single_key_entries": count_entries(sparse),
        })

    record_progress(0)

    for index in range(OPERATIONS):
        worker_id = 1 + (index % 2)

        # Each worker advances its own logical counter.
        iteration = index // 2 + 1

        # Workload A: repeatedly update aligned ranges.
        block = dense_rng.randrange(
            KEY_COUNT // BLOCK_SIZE
        )

        begin = block * BLOCK_SIZE

        dense.record(
            begin,
            begin + BLOCK_SIZE,
            worker_id,
            iteration,
        )

        # Workload B: update one randomly selected key.
        key = sparse_keys[index]

        sparse.record(
            key,
            key + 1,
            worker_id,
            iteration,
        )

        completed = index + 1

        if completed in CHECKPOINTS:
            record_progress(completed)

    # Aligned updates can create at most 10 regions.
    assert dense.count() <= KEY_COUNT // BLOCK_SIZE

    # Scattered updates should cause much more splitting.
    assert sparse.count() > dense.count()
    assert sparse.count() > 200

    # Export actual measurements for the report.
    output = Path("results/c3_clock_growth.csv")
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=list(records[0])
        )

        writer.writeheader()
        writer.writerows(records)

    print(
        "Operations | Range segments | "
        "Single-key segments | Range entries | "
        "Single-key entries"
    )

    for row in records:
        print(
            f"{row['operations']:10d} | "
            f"{row['range_segments']:14d} | "
            f"{row['single_key_segments']:19d} | "
            f"{row['range_entries']:13d} | "
            f"{row['single_key_entries']:18d}"
        )

    print("Reproducible clock-growth workload: PASS")
    print("Range-heavy clock bound: PASS")
    print("Single-key fragmentation: PASS")
    print("Clock-growth CSV export: PASS")
    print("C3.4 vector clock growth: PASS")
    print("Output:", output)


if __name__ == "__main__":
    main()
