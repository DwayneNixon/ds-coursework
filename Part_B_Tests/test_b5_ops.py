
import csv

from parameter_client import ParameterClient
from harness.worker import run_worker
from harness.oracle import TargetVector
from harness.metrics import write_ops_csv


def main():
    records = []

    targets = TargetVector(
        num_keys=2,
        width=2,
        seed=42,
    )

    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=901,
    )

    try:
        run_worker(
            client=client,
            worker_id=901,
            iterations=3,
            num_keys=2,
            keys_per_iter=2,
            width=2,
            targets=targets,
            seed=42,
            compute_ms=0,
            on_operation=records.append,
        )

    finally:
        client.close()

    # Three iterations produce three pulls and pushes.
    assert len(records) == 6
    assert [r["op"] for r in records] == [
        "pull", "push",
        "pull", "push",
        "pull", "push",
    ]

    print("Operation recording: PASS")

    for record in records:
        assert record["worker"] == 901
        assert record["n_keys"] == 2
        assert record["status"] == "OK"
        assert record["latency_ms"] >= 0

    print("Operation latency: PASS")

    path = write_ops_csv(
        records,
        "results/b5_ops",
    )

    with path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        saved = list(csv.DictReader(file))

    assert len(saved) == 6
    assert all(row["op"] in ("pull", "push") for row in saved)

    print("CSV export: PASS")
    print("Output:", path)
    print("\nB5.1 operation metrics: PASS")


if __name__ == "__main__":
    main()
