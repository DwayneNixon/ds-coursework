
import math
import time


class ConvergenceObserver:
    """Read-only observer for measuring model convergence."""

    def __init__(
        self,
        client,
        targets,
        num_keys,
        width,
        batch_size=1024,
    ):
        if num_keys < 1 or width < 1 or batch_size < 1:
            raise ValueError("Invalid observer configuration")

        self.client = client
        self.targets = targets
        self.num_keys = num_keys
        self.width = width
        self.batch_size = batch_size

        self.records = []

    def sample(self, iteration):
        """Pull the model and measure distance to the target."""

        distance_sq = 0.0

        # Read the model in manageable contiguous batches.
        for start in range(0, self.num_keys, self.batch_size):
            end = min(start + self.batch_size, self.num_keys)

            task = self.client.pull(range(start, end))
            result = task.wait(timeout_ms=10000)

            weights = list(result.values)

            if len(weights) != (end - start) * self.width:
                raise ValueError("Unexpected observer response size")

            for offset, key in enumerate(range(start, end)):
                target = self.targets[key]

                for component in range(self.width):
                    position = offset * self.width + component

                    difference = (
                        weights[position] - target[component]
                    )

                    distance_sq += difference ** 2

        record = {
            "ts_ns": time.time_ns(),
            "iteration": iteration,
            "dist_to_target": math.sqrt(distance_sq),
        }

        self.records.append(record)
        return record

    def monitor(self, stop_event, get_iteration, interval_s=0.05):
        """Periodically measure convergence until stopped."""

        if interval_s <= 0:
            raise ValueError("Observer interval must be positive")

        while not stop_event.wait(interval_s):
            self.sample(get_iteration())
