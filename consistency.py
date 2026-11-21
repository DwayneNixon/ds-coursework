
import threading


class ConsistencyGate:
    """Track active worker progress and apply iteration delay bounds."""

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

        # Historical progress is retained even after a worker leaves.
        self.completed = {
            worker: 0
            for worker in workers
        }

        # Only active workers participate in consistency barriers.
        self.active_workers = set(workers)

        # Changes whenever a worker joins or leaves.
        self.membership_epoch = 1

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
        """Check progress of currently active workers only."""

        return all(
            self.completed[worker] >= required
            for worker in self.active_workers
        )

    def can_start(self, iteration, worker_id=None):
        """Check whether an iteration may begin.

        worker_id is optional for compatibility with Part C.
        """

        required = self.required_iteration(iteration)

        with self.condition:
            if worker_id is not None:
                if worker_id not in self.active_workers:
                    raise ValueError("Unknown or inactive worker")

                if iteration != self.completed[worker_id] + 1:
                    raise ValueError(
                        "Worker must start its next iteration"
                    )

            return self._allowed(required)

    def wait_until_allowed(self, iteration, timeout_ms=None):
        """Block until all required active workers have progressed."""

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
            if worker_id not in self.active_workers:
                raise ValueError("Unknown or inactive worker")

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

    def register_worker(self, worker_id):
        """Join a new worker at the current coordination baseline.

        Returns the first iteration the worker should execute.
        """

        if (
            type(worker_id) is not int
            or not 0 <= worker_id < (1 << 32)
        ):
            raise ValueError("Worker ID must be a uint32 integer")

        with self.condition:
            # Never reuse an ID from a departed worker.
            # Historical vector-clock entries may still refer to it.
            if worker_id in self.completed:
                raise ValueError("Worker ID has already been used")

            baseline = min(
                self.completed[worker]
                for worker in self.active_workers
            )

            self.completed[worker_id] = baseline
            self.active_workers.add(worker_id)

            self.membership_epoch += 1
            self.condition.notify_all()

            return baseline + 1

    def leave_worker(self, worker_id):
        """Remove a worker from future consistency barriers.

        The caller must ensure its outstanding Push has completed.
        """

        with self.condition:
            if worker_id not in self.active_workers:
                raise ValueError("Unknown or inactive worker")

            # Keep at least one participant in this simple model.
            if len(self.active_workers) == 1:
                raise ValueError(
                    "Cannot remove the last active worker"
                )

            last_iteration = self.completed[worker_id]

            self.active_workers.remove(worker_id)

            # Keep the historical entry in self.completed.
            self.membership_epoch += 1
            self.condition.notify_all()

            return last_iteration

    def snapshot(self):
        """Return historical progress, including departed workers."""

        with self.condition:
            return dict(self.completed)

    def active_snapshot(self):
        """Return progress of currently active workers only."""

        with self.condition:
            return {
                worker: progress
                for worker, progress in self.completed.items()
                if worker in self.active_workers
            }
