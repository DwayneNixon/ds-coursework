"""Part F1.2: background server-to-manager heartbeat sender.

No ownership or replication behaviour changes here. A heartbeat says the
process can reach its manager, not that a replica is sufficiently up-to-date.
"""

import math
import threading

import grpc
import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc


class ServerHeartbeatSender:
    """Periodically send heartbeats from an independent server process."""

    def __init__(self, manager_address, server_id,
                 interval_s=0.1, rpc_timeout_s=0.4):
        if not isinstance(manager_address, str) or not manager_address.strip():
            raise ValueError("Manager address must be nonempty")
        if not isinstance(server_id, str) or not server_id.strip():
            raise ValueError("Server ID must be nonempty")

        for name, number in (
            ("Heartbeat interval", interval_s),
            ("Heartbeat RPC timeout", rpc_timeout_s),
        ):
            if (
                isinstance(number, bool)
                or not isinstance(number, (int, float))
                or not math.isfinite(number)
                or number <= 0
            ):
                raise ValueError(f"{name} must be positive and finite")

        self.manager_address = manager_address
        self.server_id = server_id
        self.interval_s = float(interval_s)
        self.rpc_timeout_s = float(rpc_timeout_s)
        self._stop = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread = None
        self._last_rpc_error = None

    @property
    def last_rpc_error(self):
        """Diagnostic only: the most recent failed heartbeat, if any."""
        with self._lifecycle_lock:
            return self._last_rpc_error

    def _run(self):
        with grpc.insecure_channel(self.manager_address) as channel:
            stub = pb_grpc.OwnershipManagerStub(channel)
            request = pb.HeartbeatRequest(server_id=self.server_id)

            # Send immediately, then periodically. The Event.wait() call
            # makes stop() responsive without an uninterruptible sleep.
            while not self._stop.is_set():
                try:
                    stub.Heartbeat(request, timeout=self.rpc_timeout_s)
                except grpc.RpcError as error:
                    # F1.2 observes failures; losing manager connectivity
                    # must not crash the parameter server's Push/Pull path.
                    with self._lifecycle_lock:
                        self._last_rpc_error = (
                            error.code(), error.details()
                        )
                else:
                    with self._lifecycle_lock:
                        self._last_rpc_error = None
                self._stop.wait(self.interval_s)

    def start(self):
        """Launch the heartbeat sender once."""
        with self._lifecycle_lock:
            if self._thread is not None:
                raise RuntimeError("Heartbeat sender already started")
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name=f"heartbeat-sender-{self.server_id}",
            )
            self._thread.start()

    def stop(self):
        """Stop the sender and close its gRPC channel."""
        with self._lifecycle_lock:
            thread = self._thread
            if thread is None:
                return
            self._stop.set()

        if thread is threading.current_thread():
            raise RuntimeError("Cannot stop heartbeat sender from itself")
        thread.join(timeout=max(5.0, self.rpc_timeout_s + 1.0))
        if thread.is_alive():
            raise RuntimeError("Heartbeat sender did not stop")

        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None
