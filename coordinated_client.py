
import time
import grpc

import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc
from bisect import bisect_right
from routing import KEY_SPACE_END


def clock_covers_push(target, segments, worker_id, iteration):
    """Check whether returned clocks cover a worker's Push."""

    # Keep only intervals that have observed this iteration.
    intervals = []

    for segment in segments:
        has_update = any(
            entry.worker_id == worker_id
            and entry.counter >= iteration
            for entry in segment.entries
        )

        if has_update:
            end = (
                KEY_SPACE_END
                if segment.end_is_keyspace_end
                else segment.end
            )
            intervals.append((segment.begin, end))

    intervals.sort()

    # Combine neighbouring or overlapping covered intervals.
    merged = []

    for begin, end in intervals:
        if not merged or begin > merged[-1][1]:
            merged.append([begin, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    starts = [begin for begin, _ in merged]

    def covered(begin, end):
        index = bisect_right(starts, begin) - 1

        return (
            index >= 0
            and merged[index][1] >= end
        )

    if isinstance(target, range):
        if target.step != 1:
            raise ValueError("Range step must be 1")

        return target.start < target.stop and covered(
            target.start, target.stop
        )

    if isinstance(target, (list, tuple)):
        return bool(target) and all(
            covered(key, key + 1)
            for key in target
        )

    raise TypeError("Unsupported Push target")

class CoordinatedClient:
    """Add iteration coordination to an existing client."""

    def __init__(
        self,
        client,
        manager_address,
        worker_id,
        total_iterations,
        poll_ms=20,
        barrier_timeout_s=30,
    ):
        self.client = client
        self.worker_id = worker_id
        self.total_iterations = total_iterations
        self.iteration = 1
        self._pending_push = None
        self._pending_target = None
        self.poll_seconds = poll_ms / 1000
        self.barrier_timeout_s = barrier_timeout_s

        self.channel = grpc.insecure_channel(manager_address)
        self.manager = pb_grpc.OwnershipManagerStub(
            self.channel
        )

    def pull(self, target, deps=None):
        """Reuse the normal distributed Pull."""
        return self.client.pull(target, deps=deps)


    def push(self, target, gradients, width, deps=None):
        """Submit one Push for the current iteration."""

        if self._pending_push is not None:
            raise RuntimeError(
                "Previous iteration has not been reported"
            )

        task = self.client.push(
            target,
            gradients,
            width=width,
            deps=deps,
            iteration=self.iteration,
        )

        self._pending_push = task
        self._pending_target = (
            target if isinstance(target, range)
            else tuple(target)
        )

        return task

    def wait_for_turn(self, iteration):
        """Wait until the manager permits this iteration."""

        deadline = (
            time.monotonic() + self.barrier_timeout_s
        )

        while True:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise TimeoutError(
                    f"Consistency barrier timed out "
                    f"before iteration {iteration}"
                )

            response = self.manager.CanStart(
                pb.CanStartRequest(iteration=iteration),
                timeout=min(5.0, remaining),
            )

            if response.allowed:
                return

            time.sleep(
                min(
                    self.poll_seconds,
                    max(0.0, deadline - time.monotonic()),
                )
            )


    def iteration_finished(self, iteration):
        """Verify a successful Push before reporting progress."""

        if iteration != self.iteration:
            raise ValueError("Unexpected completed iteration")

        if self._pending_push is None:
            raise RuntimeError(
                "No Push submitted for this iteration"
            )

        # The worker has already waited for this Task.
        # A zero-timeout wait retrieves the completed result.
        result = self._pending_push.wait(timeout_ms=0)

        if not result.accepted:
            raise RuntimeError("Push was not accepted")

        if not clock_covers_push(
            self._pending_target,
            result.clocks,
            self.worker_id,
            iteration,
        ):
            raise RuntimeError(
                "CLOCK_NOT_SATISFIED: Push clock evidence "
                "does not cover the requested keys"
            )

        # Report completion only after clock verification.
        reply = self.manager.CompleteIteration(
            pb.CompleteIterationRequest(
                worker_id=self.worker_id,
                iteration=iteration,
            ),
            timeout=5.0,
        )

        if reply.completed_iteration != iteration:
            raise RuntimeError(
                "Incorrect manager completion acknowledgement"
            )

        self._pending_push = None
        self._pending_target = None
        self.iteration += 1

        if iteration < self.total_iterations:
            self.wait_for_turn(self.iteration)


    def close(self):
        self.channel.close()
        self.client.close()
