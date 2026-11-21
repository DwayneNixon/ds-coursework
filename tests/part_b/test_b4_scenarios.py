
from harness.scenarios import ScenarioEngine


def main():
    executed = []

    def fake_execute(action, args):
        executed.append((action, args))
        return "SIMULATED"

    events = [
        {
            "at": {"time_s": 1.0},
            "action": "delay_link",
            "a": "worker1",
            "b": "server1",
            "ms": 100,
        },
        {
            "at": {"iteration": 2},
            "action": "kill_worker",
            "target": 202,
        },
        {
            "at": {"time_s": 2.0},
            "action": "drop_link",
            "a": "worker1",
            "b": "server1",
            "p": 0.25,
        },
        {
            "at": {"iteration": 3},
            "action": "add_server",
        },
    ]

    engine = ScenarioEngine(events, fake_execute)

    # Nothing should happen yet.
    engine.tick(0.5, {201: 1, 202: 1})
    assert len(executed) == 0

    # Trigger elapsed time = 1 second.
    engine.tick(1.0, {201: 2, 202: 1})
    assert len(executed) == 1
    assert executed[-1][0] == "delay_link"

    # Slowest worker reaches iteration 2.
    engine.tick(1.2, {201: 2, 202: 2})
    assert len(executed) == 2
    assert executed[-1][0] == "kill_worker"

    # Trigger elapsed time = 2 seconds.
    engine.tick(2.5, {201: 3, 202: 2})
    assert len(executed) == 3
    assert executed[-1][0] == "drop_link"

    # Slowest worker reaches iteration 3.
    engine.tick(2.6, {201: 3, 202: 3})
    assert len(executed) == 4
    assert executed[-1][0] == "add_server"

    # Repeated checks must not execute events again.
    engine.tick(10.0, {201: 10, 202: 10})
    assert len(executed) == 4

    print("Timed triggers: PASS")
    print("Slowest-worker iteration triggers: PASS")
    print("Event execution order: PASS")
    print("Exactly-once event dispatch: PASS")

    assert len(engine.records) == 4

    for record in engine.records:
        assert record["outcome"] == "SIMULATED"
        assert "ts_ns" in record
        assert "args" in record

    print("Event recording: PASS")
    print("\nB4.1 scenario scheduler: PASS")


if __name__ == "__main__":
    main()
