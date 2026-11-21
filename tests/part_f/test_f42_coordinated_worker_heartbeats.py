"""F4.2 — automatic heartbeat lifecycle of normal coordinated workers.

Run from the repository root after passing F4.1. No additional Protobuf
regeneration is needed; this uses the F4.1 WorkerHeartbeat RPC.
"""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import time
import unittest

import grpc
import manager_pb2_grpc as manager_grpc
import parameter_server_pb2_grpc as parameter_grpc

from coordinated_client import CoordinatedClient
from distributed_client import DistributedParameterClient
from manager_rpc import ManagerService
from routing import KEY_SPACE_END, Partition
from server import ParameterServer
from server_manager import ServerManager


def until(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.015)
    return predicate()


class DummyDataClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class CoordinatedHeartbeatTests(unittest.TestCase):
    def setUp(self):
        # A real manager gRPC endpoint, but no data server is needed for
        # these control-plane tests.
        self.manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, "127.0.0.1:65000")],
            heartbeat_timeout_s=100.0,
        )
        self.manager.configure_consistency([11, 22], mode="sequential")
        self.manager.configure_worker_failure_detection(timeout_s=0.35)
        self.server = grpc.server(ThreadPoolExecutor(max_workers=8))
        manager_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.server
        )
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        self.server.start()
        self.address = f"127.0.0.1:{port}"
        self.manager.start_heartbeat_monitor(interval_s=0.025)
        self.workers = []

    def tearDown(self):
        for worker in self.workers:
            worker.close()
        self.manager.stop_heartbeat_monitor()
        self.server.stop(0).wait()

    def make_worker(self, worker_id, **kwargs):
        worker = CoordinatedClient(
            DummyDataClient(), self.address, worker_id, total_iterations=3,
            worker_heartbeats=True, heartbeat_interval_s=0.04,
            heartbeat_rpc_timeout_s=0.15, **kwargs,
        )
        self.workers.append(worker)
        return worker

    def test_initial_worker_lazily_starts_on_first_barrier(self):
        worker = self.make_worker(11)
        self.assertEqual(
            self.manager.worker_health_snapshot()[11]["state"], "REGISTERED"
        )
        worker.wait_for_turn(1)
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[11]["state"] == "ALIVE"))
        time.sleep(0.52)  # Longer than worker timeout, no progress required.
        self.assertEqual(
            self.manager.worker_health_snapshot()[11]["state"], "ALIVE"
        )
        self.assertEqual(self.manager.consistency_gate.snapshot()[11], 0)

    def test_first_pull_or_push_starts_heartbeat_without_wait_for_turn(self):
        class SimpleDataClient(DummyDataClient):
            def pull(self, target, deps=None):
                return (target, deps)
            def push(self, *args, **kwargs):
                return object()

        data = SimpleDataClient()
        worker = CoordinatedClient(
            data, self.address, 11, 3,
            worker_heartbeats=True, heartbeat_interval_s=0.04,
            heartbeat_rpc_timeout_s=0.15,
        )
        self.workers.append(worker)
        self.assertEqual(worker.pull([3]), ([3], None))
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[11]["state"] == "ALIVE"))
        self.assertIsNotNone(worker.push([3], [1.0, 1.0], width=2))
        self.assertEqual(self.manager.consistency_gate.snapshot()[11], 0)

    def test_heartbeats_continue_while_sequential_barrier_blocks(self):
        waiting_peer = self.make_worker(22)
        slow_peer = self.make_worker(11)
        slow_peer.wait_for_turn(1)
        waiting_peer.wait_for_turn(1)
        self.manager.consistency_gate.complete(22, 1)

        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(waiting_peer.wait_for_turn, 2)
            time.sleep(0.55)
            self.assertFalse(waiting.done())
            self.assertEqual(
                self.manager.worker_health_snapshot()[11]["state"], "ALIVE"
            )
            self.assertEqual(
                self.manager.worker_health_snapshot()[22]["state"], "ALIVE"
            )
            self.manager.consistency_gate.exclude_failed_worker(11)
            self.assertIsNone(waiting.result(timeout=2.0))
        self.assertEqual(self.manager.consistency_gate.snapshot()[11], 0)

    def test_dynamic_join_starts_only_after_manager_registration(self):
        new_worker = self.make_worker(33)
        self.assertNotIn(33, self.manager.worker_health_snapshot())
        start = new_worker.join_training()
        self.assertEqual(start, 1)
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[33]["state"] == "ALIVE"))
        self.assertEqual(new_worker.leave_training(), 0)
        self.assertEqual(
            self.manager.worker_health_snapshot()[33]["reason"],
            "GRACEFUL_LEAVE",
        )
        self.assertFalse(new_worker._heartbeat_running)
        with self.assertRaises(RuntimeError):
            new_worker.push([10], [1.0, 1.0], width=2)

    def test_failed_leave_keeps_heartbeat_active_for_retry(self):
        # Keep another participant live: graceful leave cannot remove the
        # very last active worker under the original E3 membership policy.
        peer = self.make_worker(22)
        peer.wait_for_turn(1)
        worker = self.make_worker(11)
        worker.wait_for_turn(1)
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[11]["state"] == "ALIVE"))
        original = worker.manager

        class FailingStub:
            def LeaveWorker(self, request, timeout=None):
                raise RuntimeError("Simulated lost leave RPC")

        worker.manager = FailingStub()
        try:
            with self.assertRaisesRegex(RuntimeError, "Simulated"):
                worker.leave_training()
        finally:
            worker.manager = original
        time.sleep(0.47)
        self.assertEqual(
            self.manager.worker_health_snapshot()[11]["state"], "ALIVE"
        )
        self.assertFalse(worker._left)
        self.assertTrue(worker._heartbeat_running)
        self.assertEqual(worker.leave_training(), 0)

    def test_close_stops_sender_without_graceful_leave(self):
        worker = self.make_worker(11)
        peer = self.make_worker(22)
        worker.wait_for_turn(1)
        peer.wait_for_turn(1)
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[11]["state"] == "ALIVE"))
        worker.close()
        worker.close()  # Safe to call repeatedly.
        self.assertTrue(until(lambda:
            self.manager.worker_health_snapshot()[11]["state"] == "EXCLUDED"))
        self.assertEqual(
            self.manager.worker_health_snapshot()[11]["reason"],
            "HEARTBEAT_TIMEOUT",
        )
        self.assertEqual(
            self.manager.worker_health_snapshot()[22]["state"], "ALIVE"
        )
        self.assertEqual(self.manager.consistency_gate.snapshot()[11], 0)

    def test_validation_and_legacy_default(self):
        with self.assertRaises(ValueError):
            CoordinatedClient(DummyDataClient(), self.address, 11, 3,
                              worker_heartbeats="yes")
        with self.assertRaises(ValueError):
            CoordinatedClient(DummyDataClient(), self.address, 11, 3,
                              worker_heartbeats=True,
                              heartbeat_interval_s=0)
        # No sender is created in legacy mode, and existing calls continue.
        legacy = CoordinatedClient(DummyDataClient(), self.address, 11, 3)
        self.workers.append(legacy)
        self.assertIsNone(legacy._heartbeat_sender)
        legacy.wait_for_turn(1)
        self.assertEqual(
            self.manager.worker_health_snapshot()[11]["state"], "REGISTERED"
        )


class RealCoordinatedCrashTests(unittest.TestCase):
    def test_killed_coordinated_worker_unblocks_training_and_preserves_weights(self):
        data = ParameterServer(value_width=2)
        data_grpc = grpc.server(ThreadPoolExecutor(max_workers=8))
        parameter_grpc.add_ParameterServerServicer_to_server(data, data_grpc)
        data_port = data_grpc.add_insecure_port("127.0.0.1:0")
        self.assertGreater(data_port, 0)
        data_addr = f"127.0.0.1:{data_port}"

        manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, data_addr)],
            heartbeat_timeout_s=100.0,
        )
        manager.configure_consistency([11, 22], mode="sequential")
        manager.configure_worker_failure_detection(timeout_s=1.0)
        data.set_ownership(data_addr, manager.get_map())
        data_grpc.start()

        control = grpc.server(ThreadPoolExecutor(max_workers=8))
        manager_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(manager), control
        )
        port = control.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        control.start()
        address = f"127.0.0.1:{port}"
        manager.start_heartbeat_monitor(interval_s=0.025)
        process = None
        worker = None
        try:
            code = """
import sys, time
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient
address = sys.argv[1]
client = DistributedParameterClient.from_manager(address, worker_id=11)
worker = CoordinatedClient(client, address, 11, 3,
                           worker_heartbeats=True,
                           heartbeat_interval_s=0.05,
                           heartbeat_rpc_timeout_s=0.2)
worker.wait_for_turn(1)
time.sleep(60)  # Simulated stalled computation; the sender remains alive.
"""
            process = subprocess.Popen(
                [sys.executable, "-c", code, address],
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.assertTrue(until(lambda:
                manager.worker_health_snapshot()[11]["state"] == "ALIVE",
                timeout=4.0), "The child worker did not start heartbeat")

            client = DistributedParameterClient.from_manager(address, worker_id=22)
            worker = CoordinatedClient(
                client, address, 22, total_iterations=3,
                worker_heartbeats=True,
                heartbeat_interval_s=0.05, heartbeat_rpc_timeout_s=0.2,
            )
            worker.wait_for_turn(1)
            task = worker.push([10], [1.0, 1.0], width=2)
            self.assertTrue(task.wait(timeout_ms=5000).accepted)
            worker.iteration_finished(1, wait_for_next=False)

            with ThreadPoolExecutor(max_workers=1) as executor:
                waiting = executor.submit(worker.wait_for_turn, 2)
                time.sleep(1.25)  # Greater than worker timeout.
                self.assertFalse(waiting.done())
                self.assertEqual(
                    manager.worker_health_snapshot()[11]["state"], "ALIVE"
                )
                self.assertEqual(
                    manager.worker_health_snapshot()[22]["state"], "ALIVE"
                )
                killed_at = time.monotonic()
                process.kill()
                process.wait(timeout=5)
                self.assertTrue(until(lambda:
                    manager.worker_health_snapshot()[11]["state"] == "EXCLUDED",
                    timeout=4.0))
                self.assertIsNone(waiting.result(timeout=2.0))
                print("F4.2 coordinated-worker crash exclusion latency: "
                      f"{time.monotonic() - killed_at:.3f}s")

            task = worker.push([10], [1.0, 1.0], width=2)
            self.assertTrue(task.wait(timeout_ms=5000).accepted)
            worker.iteration_finished(2, wait_for_next=False)
            result = worker.pull([10]).wait(timeout_ms=5000)
            self.assertEqual(len(result.values), 2)
            for value in result.values:
                self.assertAlmostEqual(value, -0.2, places=9)
            self.assertEqual(manager.consistency_gate.snapshot(), {11: 0, 22: 2})
            self.assertEqual(manager.consistency_gate.active_snapshot(), {22: 2})
            self.assertEqual(data.range_clocks["p0"].clock_for_key(10), {22: 2})
        finally:
            if worker is not None:
                worker.close()
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
            manager.stop_heartbeat_monitor()
            control.stop(0).wait()
            data_grpc.stop(0).wait()


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F4.2 automatic coordinated-worker heartbeats: PASS")
    else:
        raise SystemExit(1)
