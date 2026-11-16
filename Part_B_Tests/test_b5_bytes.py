
import csv
import json

import parameter_server_pb2 as pb

from parameter_client import ParameterClient
from harness.metrics import (
    measure_operation,
    make_summary,
    write_ops_csv,
    write_summary_json,
)


def main():
    records = []
    keys = [0, 1]

    client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=905,
    )

    try:
        # ------------------------------------------
        # 1. Sparse pull
        # ------------------------------------------

        pull_task, pull_response = measure_operation(
            create_task=lambda: client.pull(keys),
            worker_id=905,
            iteration=1,
            op="pull",
            n_keys=2,
            target=keys,
            record_callback=records.append,
        )

        expected_pull = pb.PullRequest(
            keys=keys,
            task_id=pb.TaskId(
                worker_id=905,
                sequence=(1 << 63) + 1,
            ),
        )

        assert records[0]["bytes_out"] == (
            expected_pull.ByteSize()
        )

        assert records[0]["bytes_in"] == (
            pull_response.ByteSize()
        )

        print("Sparse pull byte accounting: PASS")

        # ------------------------------------------
        # 2. Push depending on the previous pull
        # ------------------------------------------

        gradients = [-1.0, -2.0, -3.0, -4.0]

        push_task, push_response = measure_operation(
            create_task=lambda: client.push(
                keys,
                gradients,
                width=2,
                deps=[pull_task],
            ),
            worker_id=905,
            iteration=1,
            op="push",
            n_keys=2,
            record_callback=records.append,
        )

        assert records[1]["bytes_out"] == len(
            push_task._push_request_bytes
        )

        assert records[1]["bytes_in"] == (
            push_response.ByteSize()
        )

        # Check that the serialized request includes
        # the actual dependency Task ID.
        saved_request = pb.PushRequest()
        saved_request.ParseFromString(
            push_task._push_request_bytes
        )

        assert len(saved_request.dependencies) == 1

        dependency = saved_request.dependencies[0]

        assert dependency.worker_id == pull_task.task_id[0]
        assert dependency.sequence == pull_task.task_id[1]

        print("Push and dependency bytes: PASS")

        # ------------------------------------------
        # 3. Dense pull depending on the push
        # ------------------------------------------

        dense_task, dense_response = measure_operation(
            create_task=lambda: client.pull(
                range(0, 2),
                deps=[push_task],
            ),
            worker_id=905,
            iteration=2,
            op="pull",
            n_keys=2,
            target=range(0, 2),
            deps=[push_task],
            record_callback=records.append,
        )

        expected_dense = pb.PullRequest(
            key_range=pb.KeyRange(begin=0, end=2),
            task_id=pb.TaskId(
                worker_id=905,
                sequence=(1 << 63) + 2,
            ),
        )

        expected_dense.dependencies.add(
            worker_id=push_task.task_id[0],
            sequence=push_task.task_id[1],
        )

        assert records[2]["bytes_out"] == (
            expected_dense.ByteSize()
        )

        assert records[2]["bytes_in"] == (
            dense_response.ByteSize()
        )

        print("Dense pull byte accounting: PASS")

    finally:
        client.close()

    # ----------------------------------------------
    # 4. Validate all records
    # ----------------------------------------------

    assert len(records) == 3

    for record in records:
        assert record["status"] == "OK"
        assert record["bytes_out"] > 0
        assert record["bytes_in"] > 0

    print("Recorded payload sizes: PASS")

    # ----------------------------------------------
    # 5. Verify summary totals
    # ----------------------------------------------

    expected_total = sum(
        record["bytes_out"] + record["bytes_in"]
        for record in records
    )

    summary = make_summary(records, [])

    assert summary["total_bytes"] == expected_total

    print("Summary byte total: PASS")

    # ----------------------------------------------
    # 6. Export and verify files
    # ----------------------------------------------

    output_dir = "results/b5_bytes"

    ops_path = write_ops_csv(records, output_dir)
    summary_path = write_summary_json(
        summary,
        output_dir,
    )

    with ops_path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 3
    assert all(
        int(row["bytes_out"]) > 0
        and int(row["bytes_in"]) > 0
        for row in rows
    )

    with summary_path.open(
        encoding="utf-8",
    ) as file:
        saved_summary = json.load(file)

    assert saved_summary["total_bytes"] == expected_total

    print("CSV and JSON byte exports: PASS")

    print("\nMeasured payloads:")
    for record in records:
        print(
            f"{record['op']:4s} | "
            f"out={record['bytes_out']} bytes | "
            f"in={record['bytes_in']} bytes"
        )

    print("\nTotal:", expected_total, "bytes")
    print("Output:", ops_path)
    print("Output:", summary_path)

    print("\nB5.7 Protobuf byte accounting: PASS")


if __name__ == "__main__":
    main()
