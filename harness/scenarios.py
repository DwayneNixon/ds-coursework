
import time


class ScenarioEngine:
    """Trigger scenario events at the configured time or iteration."""

    def __init__(self, events, execute):
        self.execute = execute
        self.events = []
        self.records = []

        for event in events:
            trigger = event.get("at", {})

            # Exactly one trigger type is required.
            has_time = "time_s" in trigger
            has_iteration = "iteration" in trigger

            if has_time == has_iteration:
                raise ValueError(
                    "Event needs either time_s or iteration"
                )

            action = event["action"]

            args = {
                key: value
                for key, value in event.items()
                if key not in ("at", "action")
            }

            self.events.append({
                "at": trigger,
                "action": action,
                "args": args,
                "fired": False,
            })

    def tick(self, elapsed_s, worker_iterations):
        """Check whether any pending events should fire."""

        slowest_iteration = min(
            worker_iterations.values(),
            default=0,
        )

        for event in self.events:
            if event["fired"]:
                continue

            trigger = event["at"]

            if "time_s" in trigger:
                due = elapsed_s >= trigger["time_s"]
            else:
                due = (
                    slowest_iteration >= trigger["iteration"]
                )

            if not due:
                continue

            try:
                outcome = self.execute(
                    event["action"],
                    event["args"],
                )

                if outcome is None:
                    outcome = "OK"

            except Exception as error:
                outcome = (
                    f"ERROR: {type(error).__name__}: {error}"
                )

            self.records.append({
                "ts_ns": time.time_ns(),
                "action": event["action"],
                "args": event["args"],
                "outcome": str(outcome),
            })

            event["fired"] = True
