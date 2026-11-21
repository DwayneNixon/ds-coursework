
import sys

from harness.cluster import LocalCluster
from harness.scenarios import ScenarioEngine


def main():
    cluster = LocalCluster()

    # Two temporary Python processes.
    sleep_command = [
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
    ]

    try:
        server_id = cluster.start_server(sleep_command)
        worker_id = cluster.start_worker(sleep_command)

        assert cluster.is_running(server_id)
        assert cluster.is_running(worker_id)

        print("Process launching: PASS")

        # Use the existing scenario engine.
        events = [
            {
                "at": {"iteration": 2},
                "action": "kill_worker",
                "target": worker_id,
            },
            {
                "at": {"time_s": 3.0},
                "action": "kill_server",
                "target": server_id,
            },
        ]

        engine = ScenarioEngine(
            events,
            cluster.execute,
        )

        # No events due yet.
        engine.tick(1.0, {1: 1})
        assert cluster.is_running(server_id)
        assert cluster.is_running(worker_id)

        # Kill the worker at iteration 2.
        engine.tick(1.5, {1: 2})

        assert not cluster.is_running(worker_id)
        assert cluster.is_running(server_id)

        print("Worker termination: PASS")

        # Kill the server at elapsed time 3.
        engine.tick(3.0, {1: 2})

        assert not cluster.is_running(server_id)

        print("Server termination: PASS")

        # Both events must be recorded successfully.
        assert len(engine.records) == 2

        for record in engine.records:
            assert record["outcome"].startswith("KILLED")

        print("Scenario integration: PASS")

        # Unsupported actions must report failure.
        unsupported = ScenarioEngine(
            [
                {
                    "at": {"time_s": 0},
                    "action": "add_server",
                }
            ],
            cluster.execute,
        )

        unsupported.tick(0, {})

        assert unsupported.records[0]["outcome"].startswith(
            "ERROR: NotImplementedError"
        )

        print("Unsupported action reporting: PASS")
        print("\nB4.2 cluster adapter: PASS")

    finally:
        cluster.close()


if __name__ == "__main__":
    main()
