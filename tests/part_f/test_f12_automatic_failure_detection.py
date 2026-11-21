"""F1.2: automatic manager timeouts and independent process death.

Run from the project root:
    python test_f12_automatic_failure_detection.py

Requires manager.proto bindings generated for F1.1, plus the existing
Part A-E server dependencies. No other process needs to be running.
"""

import socket
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


class FakeClock:
    def __init__(self):
        self._time = 100.0
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            return self._time

    def advance(self, amount):
        with self._lock:
            self._time += amount


def wait_for(predicate, timeout=5.0, poll=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll)
    return bool(predicate())


def unused_local_address():
    """Reserve port 0, then release it for a separate subprocess."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"127.0.0.1:{port}"


def start_manager_rpc(manager):
    import manager_pb2_grpc as pb_grpc
    from manager_rpc import ManagerService

    service = grpc.server(ThreadPoolExecutor(max_workers=6))
    pb_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager), service,
    )
    port = service.add_insecure_port("127.0.0.1:0")
    if not port:
        raise RuntimeError("Could not bind test manager")
    service.start()
    return service, f"127.0.0.1:{port}"


class AutomaticMonitorTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, "s1")],
            heartbeat_timeout_s=2.0,
            clock=self.clock,
        )

    def tearDown(self):
        self.manager.stop_heartbeat_monitor()

    def test_detects_silent_server_without_manual_polling(self):
        self.manager.start_heartbeat_monitor(interval_s=0.01)
        self.clock.advance(2.1)
        self.assertTrue(wait_for(
            lambda: self.manager.heartbeat_snapshot()["s1"]["state"]
            == "SUSPECTED",
            timeout=1.0,
        ))
        self.assertEqual(self.manager.get_map().epoch, 1)
        self.assertEqual(self.manager.get_map().partitions[0].owner, "s1")

    def test_late_heartbeat_restores_alive_before_failover(self):
        self.manager.start_heartbeat_monitor(interval_s=0.01)
        self.manager.record_heartbeat("s1")
        self.clock.advance(2.1)
        self.assertTrue(wait_for(
            lambda: self.manager.heartbeat_snapshot()["s1"]["state"]
            == "SUSPECTED",
            timeout=1.0,
        ))
        self.manager.record_heartbeat("s1")
        self.assertEqual(
            self.manager.heartbeat_snapshot()["s1"]["state"], "ALIVE"
        )
        self.assertIsNone(
            self.manager.heartbeat_snapshot()["s1"]["suspected_at_s"]
        )

    def test_monitor_start_stop_and_invalid_interval(self):
        for invalid in (0, -1, True, float("nan"), float("inf"), "1"):
            with self.assertRaises(ValueError):
                self.manager.start_heartbeat_monitor(invalid)

        self.manager.start_heartbeat_monitor(interval_s=0.01)
        with self.assertRaises(RuntimeError):
            self.manager.start_heartbeat_monitor()
        self.manager.stop_heartbeat_monitor()
        self.manager.stop_heartbeat_monitor()

        self.clock.advance(100.0)
        time.sleep(0.04)
        self.assertEqual(
            self.manager.heartbeat_snapshot()["s1"]["state"],
            "REGISTERED",
        )
        self.manager.start_heartbeat_monitor(interval_s=0.01)
        self.assertTrue(wait_for(
            lambda: self.manager.heartbeat_snapshot()["s1"]["state"]
            == "SUSPECTED",
            timeout=1.0,
        ))

    def test_background_monitor_is_safe_with_concurrent_heartbeats(self):
        self.manager.start_heartbeat_monitor(interval_s=0.002)
        self.manager.record_heartbeat("s1")
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(self.manager.record_heartbeat, ["s1"] * 100))
        self.assertEqual(
            self.manager.heartbeat_snapshot()["s1"]["state"], "ALIVE"
        )
        self.assertEqual(self.manager.get_map().epoch, 1)


class HeartbeatTransportTests(unittest.TestCase):
    def test_sender_configuration_validation(self):
        from server_heartbeat import ServerHeartbeatSender

        for settings in (
            {"manager_address": "", "server_id": "s1"},
            {"manager_address": "manager", "server_id": ""},
            {"manager_address": "manager", "server_id": "s1", "interval_s": 0},
            {"manager_address": "manager", "server_id": "s1", "rpc_timeout_s": -1},
            {"manager_address": "manager", "server_id": "s1", "interval_s": float("inf")},
        ):
            with self.assertRaises(ValueError):
                ServerHeartbeatSender(**settings)

    def test_real_grpc_heartbeats_and_suspicion_after_sender_stops(self):
        from server_heartbeat import ServerHeartbeatSender

        manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, "s1")],
            heartbeat_timeout_s=0.55,
        )
        service, address = start_manager_rpc(manager)
        manager.start_heartbeat_monitor(interval_s=0.02)
        sender = ServerHeartbeatSender(
            manager_address=address,
            server_id="s1",
            interval_s=0.05,
            rpc_timeout_s=0.2,
        )

        try:
            sender.start()
            self.assertTrue(wait_for(
                lambda: manager.heartbeat_snapshot()["s1"]["state"]
                == "ALIVE",
                timeout=5.0,
            ), "Live sender did not deliver its first heartbeat")

            time.sleep(0.7)  # Longer than the timeout: ongoing pings keep it alive.
            self.assertEqual(
                manager.heartbeat_snapshot()["s1"]["state"], "ALIVE"
            )
            self.assertIsNone(sender.last_rpc_error)
            sender.stop()

            self.assertTrue(wait_for(
                lambda: manager.heartbeat_snapshot()["s1"]["state"]
                == "SUSPECTED",
                timeout=3.0,
            ), "Manager did not notice stopped heartbeat sender")
            self.assertEqual(manager.get_map().epoch, 1)
        finally:
            sender.stop()
            manager.stop_heartbeat_monitor()
            service.stop(0).wait()

    def test_killed_parameter_server_process_is_detected(self):
        # This test runs the *actual* server.py in an independent process.
        server_id = unused_local_address()
        manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, server_id)],
            heartbeat_timeout_s=0.6,
        )
        service, manager_address = start_manager_rpc(manager)
        manager.start_heartbeat_monitor(interval_s=0.02)

        script = Path(__file__).resolve().parents[2] / "server.py"
        self.assertTrue(script.is_file(), f"Missing server.py at {script}")
        process = subprocess.Popen(
            [
                sys.executable, "-u", str(script),
                "--listen", server_id,
                "--manager", manager_address,
                "--heartbeat-interval", "0.05",
                "--heartbeat-rpc-timeout", "0.2",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        try:
            # gRPC server really started (not just the sender thread).
            with grpc.insecure_channel(server_id) as channel:
                grpc.channel_ready_future(channel).result(timeout=8.0)

            self.assertTrue(wait_for(
                lambda: manager.heartbeat_snapshot()[server_id]["state"]
                == "ALIVE",
                timeout=5.0,
            ), "Server process never heartbeated")

            # Keep it running beyond the timeout and verify no false alarm.
            time.sleep(0.8)
            self.assertIsNone(process.poll(), "Server process exited early")
            self.assertEqual(
                manager.heartbeat_snapshot()[server_id]["state"], "ALIVE"
            )

            # Crash the process ungracefully (works on Windows/Linux).
            killed_at = time.monotonic()
            process.kill()
            process.wait(timeout=5.0)

            self.assertTrue(wait_for(
                lambda: manager.heartbeat_snapshot()[server_id]["state"]
                == "SUSPECTED",
                timeout=3.0,
            ), "Manager did not detect the killed process")

            state = manager.heartbeat_snapshot()[server_id]
            detected_after_s = state["suspected_at_s"] - killed_at
            print(
                f"F1.2 observed process-kill detection latency: "
                f"{detected_after_s:.3f}s"
            )
            self.assertGreaterEqual(detected_after_s, 0)
            self.assertEqual(manager.get_map().epoch, 1)
            self.assertEqual(manager.get_map().partitions[0].owner, server_id)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5.0)
            manager.stop_heartbeat_monitor()
            service.stop(0).wait()


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F1.2 automatic failure detection: PASS")
    else:
        raise SystemExit(1)
