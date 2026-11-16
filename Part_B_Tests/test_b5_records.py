
import csv
import json

from parameter_client import ParameterClient

from harness.oracle import TargetVector
from harness.observer import ConvergenceObserver
from harness.scenarios import ScenarioEngine

from harness.metrics import (
    write_convergence_csv,
    write_events_csv,
)


OUTPUT_DIR = "results/b5_records"


def main():
    # ----------------------------------------------
    # 1. Test convergence recording
    # ----------------------------------------------

    targets = TargetVector(
        num_keys=2,
        width=2,
        seed=42,
    )

    observer_client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=903,
    )

    worker_client = ParameterClient(
        "127.0.0.1:50051",
        worker_id=904,
    )

    try:
        observer = ConvergenceObserver(
            client=observer_client,
            targets=targets,
            num_keys=2,
            width=2,
        )

        # Measure initial distance.
        initial = observer.sample(iteration=0)

        # Apply one update towards the target.
        gradients = []

        for key in range(2):
            for value in targets[key]:
                gradients.append(-value)

        worker_client.push(
            [0, 1],
            gradients,
            width=2,
        ).wait(timeout_ms=5000)

        # Measure distance after the update.
        final = observer.sample(iteration=1)

        assert final["dist_to_target"] < initial["dist_to_target"]
        assert len(observer.records) == 2

        print("Convergence recording: PASS")

        # Export convergence data.
        convergence_path = write_convergence_csv(
            observer.records,
            OUTPUT_DIR,
        )

        with convergence_path.open(
            newline="",
            encoding="utf-8",
        ) as file:
            convergence_rows = list(csv.DictReader(file))

        assert len(convergence_rows) == 2
        assert int(convergence_rows[0]["iteration"]) == 0
        assert int(convergence_rows[1]["iteration"]) == 1

        print("Convergence CSV export: PASS")

    finally:
        observer_client.close()
        worker_client.close()

    # ----------------------------------------------
    # 2. Test scenario event recording
    # ----------------------------------------------

    def fake_execute(action, args):
        # Only testing recording/export here.
        return "SIMULATED"

    events = [
        {
            "at": {"time_s": 1.0},
            "action": "delay_link",
            "a": 1,
            "b": "server1",
            "ms": 100,
        },
        {
            "at": {"iteration": 2},
            "action": "kill_worker",
            "target": 2,
        },
    ]

    engine = ScenarioEngine(
        events,
        fake_execute,
    )

    engine.tick(1.0, {1: 0})
    engine.tick(1.5, {1: 2})

    assert len(engine.records) == 2

    print("Scenario event recording: PASS")

    # Export events.
    events_path = write_events_csv(
        engine.records,
        OUTPUT_DIR,
    )

    with events_path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        event_rows = list(csv.DictReader(file))

    assert len(event_rows) == 2

    assert event_rows[0]["action"] == "delay_link"
    assert event_rows[1]["action"] == "kill_worker"

    # Verify JSON arguments survived CSV export.
    first_args = json.loads(event_rows[0]["args"])

    assert first_args["ms"] == 100
    assert first_args["b"] == "server1"

    assert all(
        row["outcome"] == "SIMULATED"
        for row in event_rows
    )

    print("Event CSV export: PASS")
    print("JSON argument preservation: PASS")

    print("\nGenerated files:")
    print(convergence_path)
    print(events_path)

    print("\nB5.3 record exports: PASS")


if __name__ == "__main__":
    main()
