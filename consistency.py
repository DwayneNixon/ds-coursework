
import threading


class ConsistencyGate:
    """Track worker progress and apply iteration delay bounds."""

    def __init__(self, workers, mode="sequential", tau=0):
        workers = list(workers)

        if not workers or len(workers) != len(set(workers)):
            raise ValueError("Workers must be unique and nonempty")

        if mode not in ("sequential", "eventual", "bounded"):
            raise ValueError("Unknown consistency mode")

        if mode == "bounded":
            if type(tau) is not int or tau < 1:
                raise ValueError("Bounded mode requires tau >= 1")
        elif tau != 0:
            raise ValueError("Tau is only used in bounded mode")

        self.mode = mode
        self.tau = tau

        # Highest contiguous completed iteration per worker.
        self.completed = {worker: 0 for worker in workers}
        self.condition = threading.Condition()

    def required_iteration(self, iteration):
        if type(iteration) is not int or iteration < 1:
            raise ValueError("Iteration must be positive")

        if self.mode == "eventual":
            return 0

        if self.mode == "sequential":
            return iteration - 1

        return max(0, iteration - self.tau - 1)

    def _allowed(self, required):
        return all(
            progress >= required
            for progress in self.completed.values()
        )

    def can_start(self, iteration):
        required = self.required_iteration(iteration)

        with self.condition:
            return self._allowed(required)

    def wait_until_allowed(self, iteration, timeout_ms=None):
        """Block until all required iterations have completed."""

        required = self.required_iteration(iteration)

        if timeout_ms is not None and timeout_ms < 0:
            raise ValueError("Timeout cannot be negative")

        timeout = (
            None if timeout_ms is None
            else timeout_ms / 1000
        )

        with self.condition:
            allowed = self.condition.wait_for(
                lambda: self._allowed(required),
                timeout=timeout,
            )

            if not allowed:
                raise TimeoutError(
                    "Consistency bound not yet satisfied"
                )

    def complete(self, worker_id, iteration):
        """Record one successfully completed worker iteration."""

        with self.condition:
            if worker_id not in self.completed:
                raise ValueError("Unknown worker")

            if (
                type(iteration) is not int
                or iteration != self.completed[worker_id] + 1
            ):
                raise ValueError(
                    "Iterations must complete in order"
                )

            required = self.required_iteration(iteration)

            if not self._allowed(required):
                raise ValueError(
                    "Consistency bound was not satisfied"
                )

            self.completed[worker_id] = iteration
            self.condition.notify_all()

    def snapshot(self):
        with self.condition:
            return dict(self.completed)
