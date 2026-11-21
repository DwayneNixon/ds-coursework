"""F3.3a: automatic orchestration of *confirmed crash-stop* failover.

This deliberately does not equate heartbeat suspicion with a dead process.
The caller supplies is_confirmed_dead(server_id), e.g. a test harness's
Popen.poll() check. A surviving, reachable manager and a seeded replica
are required. This is not fencing for arbitrary network partitions.

The controller reuses F3.1 promotion and F3.2 repair; it does not change
their commit or retry protocols. It runs separately from the F1 monitor.
"""

import math
import threading
import time


class AutomaticFailoverController:
    """Drive suspected -> confirmed -> promoted -> replication repaired.

    A single controller serializes its own recovery attempts. Failed
    promotion/repair RPCs are retried with the manager's existing
    idempotent pending-operation state; errors are surfaced in snapshot().
    """

    def __init__(
        self, manager, servers, is_confirmed_dead,
        *, scan_interval_s=0.05, retry_interval_s=0.15,
    ):
        if not isinstance(servers, dict) or not servers:
            raise ValueError("Servers must be a nonempty ID-to-peer dictionary")
        if not callable(is_confirmed_dead):
            raise TypeError("is_confirmed_dead must be callable")
        for name, value in (("scan_interval_s", scan_interval_s),
                            ("retry_interval_s", retry_interval_s)):
            if (type(value) not in (float, int) or
                    not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite")

        self.manager = manager
        self.servers = dict(servers)
        self.is_confirmed_dead = is_confirmed_dead
        self.scan_interval_s = float(scan_interval_s)
        self.retry_interval_s = float(retry_interval_s)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._inflight = None
        self._phase = None
        self._handled = set()
        self._recoveries = []
        self._last_error = None
        self._attempts = 0

    def start(self):
        """Start the background orchestrator; manager monitor is separate."""
        with self._lock:
            if self._thread is not None:
                raise RuntimeError("Automatic failover controller already started")
            self._stop.clear()
            thread = threading.Thread(
                target=self._run, name="automatic-failover-controller", daemon=True,
            )
            self._thread = thread
            thread.start()

    def stop(self, timeout_s=10.0):
        """Stop the controller; idempotent and safe between retries."""
        with self._lock:
            thread = self._thread
            self._stop.set()
        if thread is None:
            return
        if thread is threading.current_thread():
            raise RuntimeError("Cannot join the controller from itself")
        thread.join(timeout=timeout_s)
        if thread.is_alive():
            raise RuntimeError("Automatic failover controller did not stop")
        with self._lock:
            if self._thread is thread:
                self._thread = None

    def snapshot(self):
        """Return diagnostics without exposing mutable internal structures."""
        with self._lock:
            return {
                "running": self._thread is not None and self._thread.is_alive(),
                "inflight_server": self._inflight["server_id"] if self._inflight else None,
                "phase": self._phase,
                "attempts": self._attempts,
                "last_error": self._last_error,
                "handled_servers": tuple(sorted(self._handled)),
                "recoveries": [dict(record) for record in self._recoveries],
            }

    def _choose_failure(self):
        """Find a SUSPECTED primary whose process is *confirmed* stopped."""
        health = self.manager.heartbeat_snapshot()
        owners = {p.owner for p in self.manager.get_map().partitions}
        with self._lock:
            handled = set(self._handled)
        for server_id in sorted(owners):
            if server_id in handled:
                continue
            entry = health.get(server_id)
            if entry is None or entry["state"] != "SUSPECTED":
                continue
            if not self.is_confirmed_dead(server_id):
                continue
            return {
                "server_id": server_id,
                "detected_at_s": entry["suspected_at_s"],
                "confirmed_at_s": time.monotonic(),
                "promoted_at_s": None,
                "ready_at_s": None,
            }
        return None

    def _run(self):
        while not self._stop.is_set():
            try:
                with self._lock:
                    inflight = self._inflight
                if inflight is None:
                    candidate = self._choose_failure()
                    if candidate is None:
                        self._stop.wait(self.scan_interval_s)
                        continue
                    with self._lock:
                        self._inflight = candidate
                        self._phase = "PROMOTING"
                        self._last_error = None
                        inflight = candidate

                with self._lock:
                    phase = self._phase
                    self._attempts += 1
                if phase == "PROMOTING":
                    self.manager.promote_confirmed_crash(
                        inflight["server_id"], self.servers,
                        confirmed_dead=True,
                    )
                    with self._lock:
                        inflight["promoted_at_s"] = time.monotonic()
                        self._phase = "REPAIRING"

                # This call may safely be retried in F3.2's stored phase.
                self.manager.restore_replication_after_failover(self.servers)
                with self._lock:
                    inflight["ready_at_s"] = time.monotonic()
                    inflight["detection_to_promotion_s"] = (
                        inflight["promoted_at_s"] - inflight["detected_at_s"]
                    )
                    inflight["promotion_to_ready_s"] = (
                        inflight["ready_at_s"] - inflight["promoted_at_s"]
                    )
                    self._recoveries.append(dict(inflight))
                    self._handled.add(inflight["server_id"])
                    self._inflight = None
                    self._phase = None
                    self._last_error = None
            except Exception as error:
                # A failed health-confirmation callback, an unreachable peer,
                # or a lost RPC reply must not kill the monitoring thread.
                with self._lock:
                    self._last_error = f"{type(error).__name__}: {error}"
                self._stop.wait(self.retry_interval_s)
