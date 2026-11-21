"""F4: manager-directed heartbeats from a training worker process.

A worker must keep sending heartbeats while waiting at a consistency barrier:
iteration progress is NOT evidence of liveness. Construct/start this sender
from the process that owns the worker; stop it before graceful LeaveWorker.
"""

import math
import threading

import grpc
import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc


class WorkerHeartbeatSender:
    """Independent daemon sender; no changes to the Push/Pull client API."""

    def __init__(self, manager_address, worker_id, interval_s=0.1,
                 rpc_timeout_s=0.4):
        if not isinstance(manager_address, str) or not manager_address.strip():
            raise ValueError("Manager address must be nonempty")
        if type(worker_id) is not int or not 0 <= worker_id < (1 << 32):
            raise ValueError("Worker ID must be a uint32 integer")
        for name, value in (
            ("Heartbeat interval", interval_s),
            ("Heartbeat RPC timeout", rpc_timeout_s),
        ):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite")
        self.manager_address = manager_address
        self.worker_id = worker_id
        self.interval_s = float(interval_s)
        self.rpc_timeout_s = float(rpc_timeout_s)
        self._stop = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread = None
        self._last_rpc_error = None
        self._session_excluded = False

    @property
    def last_rpc_error(self):
        with self._lifecycle_lock:
            return self._last_rpc_error

    @property
    def session_excluded(self):
        with self._lifecycle_lock:
            return self._session_excluded

    def _run(self):
        with grpc.insecure_channel(self.manager_address) as channel:
            stub = pb_grpc.OwnershipManagerStub(channel)
            request = pb.WorkerHeartbeatRequest(worker_id=self.worker_id)
            while not self._stop.is_set():
                try:
                    stub.WorkerHeartbeat(request, timeout=self.rpc_timeout_s)
                except grpc.RpcError as error:
                    with self._lifecycle_lock:
                        self._last_rpc_error = (error.code(), error.details())
                        if error.code() in (
                            grpc.StatusCode.NOT_FOUND,
                            grpc.StatusCode.FAILED_PRECONDITION,
                        ):
                            # Old worker sessions must not silently revive.
                            self._session_excluded = True
                            break
                else:
                    with self._lifecycle_lock:
                        self._last_rpc_error = None
                self._stop.wait(self.interval_s)

    def start(self):
        with self._lifecycle_lock:
            if self._thread is not None:
                raise RuntimeError("Worker heartbeat sender already started")
            self._stop.clear()
            self._last_rpc_error = None
            self._session_excluded = False
            thread = threading.Thread(
                target=self._run,
                name=f"worker-heartbeat-{self.worker_id}",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    def stop(self):
        with self._lifecycle_lock:
            thread = self._thread
            if thread is None:
                return
            self._stop.set()
        if thread is threading.current_thread():
            raise RuntimeError("Cannot stop worker heartbeat from its own thread")
        thread.join(timeout=max(5.0, self.rpc_timeout_s + 1.0))
        if thread.is_alive():
            raise RuntimeError("Worker heartbeat thread did not stop")
        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None
