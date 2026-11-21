import time
import threading
import grpc

import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc
from bisect import bisect_right
from routing import KEY_SPACE_END
from worker_heartbeat import WorkerHeartbeatSender


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
        worker_heartbeats=False,
        heartbeat_interval_s=0.1,
        heartbeat_rpc_timeout_s=0.4,
    ):
        self.client = client
        self.worker_id = worker_id
        self.total_iterations = total_iterations
        self.iteration = 1
        self._pending_push = None
        self._pending_target = None
        self._joined_dynamically = False
        self._left = False
        self.poll_seconds = poll_ms / 1000
        self.barrier_timeout_s = barrier_timeout_s

        # F4.2: opt-in automatic worker heartbeats. No new manager RPCs or
        # changes to legacy C/E workers are needed. The sender starts on the
        # first training operation, or after a successful dynamic join.
        if type(worker_heartbeats) is not bool:
            raise ValueError("worker_heartbeats must be a boolean")
        self._heartbeat_lock = threading.Lock()
        self._heartbeat_running = False
        self._closed = False
        self._heartbeat_sender = (
            WorkerHeartbeatSender(
                manager_address,
                worker_id,
                interval_s=heartbeat_interval_s,
                rpc_timeout_s=heartbeat_rpc_timeout_s,
            ) if worker_heartbeats else None
        )

        self.channel = grpc.insecure_channel(manager_address)
        self.manager = pb_grpc.OwnershipManagerStub(
            self.channel
        )


    def _ensure_worker_heartbeat(self):
        """Start the background liveness signal at most once per worker."""
        with self._heartbeat_lock:
            if self._closed:
                raise RuntimeError("Coordinated worker is already closed")
            if self._heartbeat_sender is None or self._left:
                return
            if self._heartbeat_sender.session_excluded:
                raise RuntimeError("Worker heartbeat session was excluded")
            if not self._heartbeat_running:
                self._heartbeat_sender.start()
                self._heartbeat_running = True

    def _stop_worker_heartbeat(self):
        """Stop locally; do not modify membership at the manager."""
        with self._heartbeat_lock:
            if self._heartbeat_running:
                self._heartbeat_sender.stop()
                self._heartbeat_running = False

    def join_training(self):
        """Register a new worker and receive its starting iteration."""

        if self._closed:
            raise RuntimeError("Coordinated worker is already closed")
        if self._left or self._joined_dynamically:
            raise RuntimeError(
                "Worker has already joined or left training"
            )

        if self._pending_push is not None or self.iteration != 1:
            raise RuntimeError(
                "Cannot join after starting local training"
            )

        reply = self.manager.JoinWorker(
            pb.JoinWorkerRequest(worker_id=self.worker_id),
            timeout=5.0,
        )

        self.iteration = reply.starting_iteration
        self._joined_dynamically = True
        # Do not heartbeat before JoinWorker: the manager would reject an
        # unregistered ID and the sender would retire the session.
        self._ensure_worker_heartbeat()

        return self.iteration

    def leave_training(self):
        """Gracefully leave after the last Push was reported."""

        if self._closed:
            raise RuntimeError("Coordinated worker is already closed")
        if self._left:
            raise RuntimeError("Worker has already left training")

        if self._pending_push is not None:
            raise RuntimeError(
                "Finish and report the pending Push before leaving"
            )

        reply = self.manager.LeaveWorker(
            pb.LeaveWorkerRequest(worker_id=self.worker_id),
            timeout=5.0,
        )

        if reply.completed_iteration != self.iteration - 1:
            raise RuntimeError(
                "Manager and client iteration progress disagree"
            )

        self._left = True
        # First commit the graceful departure at the manager; only then
        # stop the sender. A failed LeaveWorker RPC keeps heartbeats active.
        self._stop_worker_heartbeat()
        return reply.completed_iteration

    def pull(self, target, deps=None):
        """Reuse the normal distributed Pull, keeping active workers alive."""
        if not self._left:
            self._ensure_worker_heartbeat()
        return self.client.pull(target, deps=deps)


    def push(self, target, gradients, width, deps=None):
        """Submit one Push for the current iteration."""

        if self._left:
            raise RuntimeError("Worker has already left training")

        if self._pending_push is not None:
            raise RuntimeError(
                "Previous iteration has not been reported"
            )

        self._ensure_worker_heartbeat()
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

        if self._left:
            raise RuntimeError("Departed worker cannot start an iteration")

        self._ensure_worker_heartbeat()
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
                pb.CanStartRequest(
                    iteration=iteration,
                    worker_id=self.worker_id,
                ),
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


    def iteration_finished(self, iteration, wait_for_next=True):
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

        if wait_for_next and iteration < self.total_iterations:
            self.wait_for_turn(self.iteration)

    def close(self):
        """Stop the sender and close channels; never imply graceful leave.

        If a worker exits without LeaveWorker, its absence must still be
        detected by the F4 manager's heartbeat timeout.
        """
        with self._heartbeat_lock:
            if self._closed:
                return
            self._closed = True

        try:
            self._stop_worker_heartbeat()
        finally:
            try:
                self.channel.close()
            finally:
                self.client.close()
