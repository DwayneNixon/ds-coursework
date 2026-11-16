
import tempfile
import time
from pathlib import Path

from parameter_client import ParameterClient
from harness.scenarios import ScenarioEngine
from harness.faults import (
    LinkFaultController,
    FaultInjectingClient,
    LinkDroppedError,
)


def main():
    with tempfile.TemporaryDirectory() as directory:
        state_file = Path(directory) / "faults.json"

        controller = LinkFaultController(state_file)

        client = ParameterClient(
            "127.0.0.1:50051",
            worker_id=801,
        )

        faulty_client = FaultInjectingClient(
            client=client,
            worker_id=801,
            server_id="server1",
            state_file=state_file,
            seed=42,
        )

        events = [
            {
                "at": {"time_s": 0.1},
                "action": "delay_link",
                "a": 801,
                "b": "server1",
                "ms": 80,
                "jitter_ms": 0,
            },
            {
                "at": {"time_s": 0.2},
                "action": "drop_link",
                "a": 801,
                "b": "server1",
                "p": 1.0,
            },
        ]

        engine = ScenarioEngine(
            events,
            controller.execute,
        )

        try:
            # Start with the actual current server value.
            before = list(
                client.pull([9000]).wait(
                    timeout_ms=5000
                ).values
            )

            # Trigger a deterministic 80 ms delay.
            engine.tick(0.1, {801: 0})

            start = time.perf_counter()

            faulty_client.pull([9000]).wait(
                timeout_ms=5000
            )

            elapsed_ms = (
                time.perf_counter() - start
            ) * 1000

            assert elapsed_ms >= 70

            print("Link delay injection: PASS")
            print(f"Observed latency: {elapsed_ms:.2f} ms")

            # Trigger a guaranteed outgoing drop.
            engine.tick(0.2, {801: 0})

            try:
                faulty_client.push(
                    [9000],
                    [1.0, 2.0],
                    width=2,
                )
            except LinkDroppedError:
                print("Link drop injection: PASS")
            else:
                raise AssertionError(
                    "Expected the push to be dropped"
                )

            # The dropped request must not update the server.
            after = list(
                client.pull([9000]).wait(
                    timeout_ms=5000
                ).values
            )

            assert before == after

            print("Dropped push preserves state: PASS")

            assert len(engine.records) == 2

            assert all(
                record["outcome"].startswith("APPLIED")
                for record in engine.records
            )

            print("Scenario integration: PASS")
            print("\nB4.4 link fault injection: PASS")

        finally:
            faulty_client.close()


if __name__ == "__main__":
    main()
