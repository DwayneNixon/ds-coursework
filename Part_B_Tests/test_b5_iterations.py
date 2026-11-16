
import csv

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import TargetVector
from harness.metrics import write_iterations_csv


def main():
    records = []

    targets = TargetVector(
        num_keys=2,
        width=2,
        seed=42,
    )

    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=902,
    )

    try:
        run_worker(
            client=client,
            worker_id=902,
            iterations=3,
            num_keys=2,
            keys_per_iter=2,
            width=2,
            targets=targets,
            seed=42,
            compute_ms=5,
            on_iteration_record=records.append,
        )

    finally:
        client.close()

    # Exactly three completed iterations
    assert len(records) == 3

    for index, record in enumerate(records, start=1):
        assert record["worker"] == 902
        assert record["iteration"] == index
        assert record["t_end"] > record["t_start"]

    print("Iteration recording: PASS")
    print("Iteration timestamps: PASS")

    # Staleness is not available in Part A.
    assert all(
        record["staleness"] is None
        for record in records
    )

    print("Unavailable staleness handled: PASS")

    # Export iteration records
    path = write_iterations_csv(
        records,
        "results/b5_iterations",
    )

    with path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        saved = list(csv.DictReader(file))

    assert len(saved) == 3

    for row in saved:
        assert int(row["worker"]) == 902
        assert row["staleness"] == ""

    print("CSV export: PASS")

    print("\nIteration durations:")

    for record in records:
        duration_ms = (
            record["t_end"] - record["t_start"]
        ) / 1_000_000

        print(
            f"Iteration {record['iteration']}: "
            f"{duration_ms:.3f} ms"
        )

    print("\nOutput:", path)
    print("B5.2 iteration metrics: PASS")


if __name__ == "__main__":
    main()
