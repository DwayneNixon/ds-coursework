"""F4.1: worker heartbeat detection, crash exclusion, and barrier recovery.

Run from the repository root after regenerating manager_pb2[_grpc].py.
The real process test uses a separate Python worker heartbeat process.
"""
import importlib.util
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

from consistency import ConsistencyGate
from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


class FakeClock:
    def __init__(self):
        self.value = 100.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, duration):
        with self.lock:
            self.value += duration


def make_manager(clock=None, workers=(11, 22), mode="sequential", tau=0,
                 worker_timeout=1.0):
    manager = ServerManager(
        [Partition("p0", 0, KEY_SPACE_END, "server1")],
        clock=clock,
        heartbeat_timeout_s=100.0,
    )
    manager.configure_consistency(workers, mode=mode, tau=tau)
    if worker_timeout is not None:
        manager.configure_worker_failure_detection(worker_timeout)
    return manager


def wait_for(predicate, timeout=4.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class WorkerFailureUnitTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.manager = make_manager(self.clock)

    def test_opt_in_preserves_legacy_consistency(self):
        legacy = make_manager(self.clock, worker_timeout=None)
        self.assertEqual(legacy.worker_health_snapshot(), {})
        self.assertEqual(legacy.check_worker_timeouts(), [])
        with self.assertRaises(RuntimeError):
            legacy.record_worker_heartbeat(11)
        self.clock.advance(100)
        self.assertEqual(legacy.consistency_gate.active_snapshot(), {11: 0, 22: 0})

    def test_invalid_configuration_and_ids(self):
        for value in (0, -1, True, float("inf"), float("nan"), "1"):
            with self.assertRaises(ValueError):
                make_manager(self.clock, worker_timeout=value)
        fresh = ServerManager([Partition("p0", 0, KEY_SPACE_END, "server1")])
        with self.assertRaises(RuntimeError):
            fresh.configure_worker_failure_detection()
        with self.assertRaises(RuntimeError):
            self.manager.configure_worker_failure_detection()
        for worker_id in (None, -1, 2**32, 2.5, True):
            with self.assertRaises(ValueError):
                self.manager.record_worker_heartbeat(worker_id)

    def test_heartbeats_prevent_false_failure_when_worker_makes_no_progress(self):
        # Heartbeats are independent of CompleteIteration and CanStart.
        for _ in range(8):
            self.manager.record_worker_heartbeat(11)
            self.manager.record_worker_heartbeat(22)
            self.clock.advance(.4)
            self.assertEqual(self.manager.check_worker_timeouts(), [])
        self.assertEqual(self.manager.consistency_gate.snapshot(), {11: 0, 22: 0})
        self.assertEqual(self.manager.consistency_gate.active_snapshot(), {11: 0, 22: 0})

    def test_worker_registration_heartbeat_and_timeout_boundary(self):
        registered = self.manager.worker_health_snapshot()[11]
        self.assertEqual(registered["state"], "REGISTERED")
        self.assertIsNone(registered["last_seen_s"])
        self.manager.record_worker_heartbeat(11)
        self.manager.record_worker_heartbeat(22)
        self.clock.advance(.99)
        self.assertEqual(self.manager.check_worker_timeouts(), [])
        self.clock.advance(.01)
        self.assertEqual(self.manager.check_worker_timeouts(), [11, 22])
        self.assertEqual(self.manager.check_worker_timeouts(), [])
        self.assertEqual(self.manager.consistency_gate.membership_epoch, 3)

    def test_alive_peer_unblocks_sequential_waiter_and_history_survives(self):
        gate = self.manager.consistency_gate
        self.manager.record_worker_heartbeat(11)
        self.manager.record_worker_heartbeat(22)
        gate.complete(22, 1)
        self.assertFalse(gate.can_start(2, worker_id=22))
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiter = executor.submit(gate.wait_until_allowed, 2, 2000, 22)
            self.clock.advance(1.01)
            self.manager.record_worker_heartbeat(22)
            self.assertEqual(self.manager.check_worker_timeouts(), [11])
            self.assertIsNone(waiter.result(timeout=1.0))
        self.assertTrue(gate.can_start(2, worker_id=22))
        self.assertEqual(gate.snapshot(), {11: 0, 22: 1})
        self.assertEqual(gate.active_snapshot(), {22: 1})
        gate.complete(22, 2)
        self.assertEqual(gate.snapshot()[22], 2)

    def test_bounded_delay_unblocks_on_worker_crash(self):
        manager = make_manager(self.clock, mode="bounded", tau=1)
        manager.record_worker_heartbeat(11)
        manager.record_worker_heartbeat(22)
        gate = manager.consistency_gate
        gate.complete(22, 1)
        gate.complete(22, 2)
        self.assertFalse(gate.can_start(3, worker_id=22))
        self.clock.advance(1.01)
        manager.record_worker_heartbeat(22)
        self.assertEqual(manager.check_worker_timeouts(), [11])
        self.assertTrue(gate.can_start(3, worker_id=22))
        gate.complete(22, 3)
        self.assertEqual(gate.snapshot()[11], 0)

    def test_excluded_worker_cannot_heartbeat_or_report_completion(self):
        gate = self.manager.consistency_gate
        self.clock.advance(1.01)
        self.manager.record_worker_heartbeat(22)
        self.assertEqual(self.manager.check_worker_timeouts(), [11])
        with self.assertRaises(RuntimeError):
            self.manager.record_worker_heartbeat(11)
        with self.assertRaises(ValueError):
            gate.can_start(1, worker_id=11)
        with self.assertRaises(ValueError):
            gate.complete(11, 1)
        with self.assertRaises(ValueError):
            gate.wait_until_allowed(1, timeout_ms=100, worker_id=11)
        self.assertEqual(self.manager.worker_health_snapshot()[11]["reason"],
                         "HEARTBEAT_TIMEOUT")

    def test_last_worker_can_be_excluded_without_vacuous_barrier(self):
        manager = make_manager(self.clock, workers=(11,))
        gate = manager.consistency_gate
        gate.complete(11, 1)
        self.clock.advance(1.1)
        self.assertEqual(manager.check_worker_timeouts(), [11])
        self.assertEqual(gate.active_snapshot(), {})
        self.assertFalse(gate.can_start(2))
        with self.assertRaises(TimeoutError):
            gate.wait_until_allowed(2, timeout_ms=0)
        with self.assertRaises(ValueError):
            manager.join_training_worker(11)
        starting, epoch = manager.join_training_worker(33)
        self.assertEqual((starting, epoch), (2, 3))
        manager.record_worker_heartbeat(33)
        self.assertTrue(gate.can_start(2, worker_id=33))
        self.assertEqual(gate.snapshot(), {11: 1, 33: 1})

    def test_waiting_excluded_worker_is_awakened_and_rejected(self):
        gate = self.manager.consistency_gate
        gate.complete(11, 1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(gate.wait_until_allowed, 2, 2000, 11)
            self.clock.advance(1.01)
            self.manager.record_worker_heartbeat(22)
            self.assertEqual(self.manager.check_worker_timeouts(), [11])
            with self.assertRaises(ValueError):
                waiting.result(timeout=1.0)

    def test_graceful_leave_and_join_update_worker_health(self):
        starting, epoch = self.manager.join_training_worker(33)
        self.assertEqual((starting, epoch), (1, 2))
        self.manager.record_worker_heartbeat(33)
        completed, epoch = self.manager.leave_training_worker(33)
        self.assertEqual((completed, epoch), (0, 3))
        self.assertEqual(self.manager.worker_health_snapshot()[33]["reason"],
                         "GRACEFUL_LEAVE")
        self.assertEqual(self.manager.consistency_gate.snapshot()[33], 0)
        with self.assertRaises(RuntimeError):
            self.manager.record_worker_heartbeat(33)
        with self.assertRaises(ValueError):
            self.manager.join_training_worker(33)

    def test_no_old_session_revival_and_snapshots_are_independent(self):
        self.clock.advance(1.01)
        self.manager.record_worker_heartbeat(22)
        self.manager.check_worker_timeouts()
        snapshot = self.manager.worker_health_snapshot()
        snapshot[11]["state"] = "ALIVE"
        self.assertEqual(self.manager.worker_health_snapshot()[11]["state"],
                         "EXCLUDED")
        self.assertEqual(self.manager.consistency_gate.membership_epoch, 2)
        self.assertEqual(self.manager.get_map().epoch, 1)

    def test_concurrent_worker_heartbeats_are_safe(self):
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = list(executor.map(self.manager.record_worker_heartbeat,
                                        [11] * 120))
        self.assertEqual(results, [100.0] * 120)
        self.assertEqual(self.manager.worker_health_snapshot()[11]["state"], "ALIVE")

    def test_old_direct_membership_changes_are_reconciled(self):
        gate = self.manager.consistency_gate
        gate.leave_worker(11)
        self.manager.check_worker_timeouts()
        self.assertEqual(self.manager.worker_health_snapshot()[11]["reason"],
                         "MEMBERSHIP_CHANGED")
        with self.assertRaises(RuntimeError):
            self.manager.record_worker_heartbeat(11)

    def test_background_monitor_excludes_silent_worker(self):
        # Real monotonic time; no fake-clock sleeping assumptions.
        manager = make_manager(workers=(11, 22), worker_timeout=0.18)
        manager.start_heartbeat_monitor(interval_s=0.01)
        try:
            self.assertTrue(wait_for(lambda:
                manager.worker_health_snapshot()[11]["state"] == "EXCLUDED",
                timeout=2.0))
            self.assertEqual(manager.consistency_gate.active_snapshot(), {})
        finally:
            manager.stop_heartbeat_monitor()


HAS_STUBS = (
    importlib.util.find_spec("manager_pb2") is not None
    and importlib.util.find_spec("manager_pb2_grpc") is not None
)


@unittest.skipUnless(HAS_STUBS, "Generate manager Protobuf bindings first")
class WorkerFailureRpcTests(unittest.TestCase):
    def start_rpc(self, manager):
        import manager_pb2_grpc as pb_grpc
        from manager_rpc import ManagerService
        server = grpc.server(ThreadPoolExecutor(max_workers=8))
        pb_grpc.add_OwnershipManagerServicer_to_server(ManagerService(manager), server)
        port = server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        server.start()
        return server, f"127.0.0.1:{port}"

    def test_real_heartbeat_rpc_join_leave_and_old_getmap(self):
        import manager_pb2 as pb
        import manager_pb2_grpc as pb_grpc
        manager = make_manager(workers=(11, 22))
        server, address = self.start_rpc(manager)
        try:
            with grpc.insecure_channel(address) as channel:
                stub = pb_grpc.OwnershipManagerStub(channel)
                response = stub.WorkerHeartbeat(pb.WorkerHeartbeatRequest(worker_id=11), timeout=2)
                self.assertTrue(response.accepted)
                self.assertEqual(response.membership_epoch, 1)
                self.assertEqual(stub.GetMap(pb.GetMapRequest(), timeout=2).epoch, 1)
                joined = stub.JoinWorker(pb.JoinWorkerRequest(worker_id=33), timeout=2)
                self.assertEqual(joined.starting_iteration, 1)
                self.assertTrue(stub.WorkerHeartbeat(
                    pb.WorkerHeartbeatRequest(worker_id=33), timeout=2).accepted)
                departed = stub.LeaveWorker(pb.LeaveWorkerRequest(worker_id=33), timeout=2)
                self.assertEqual(departed.membership_epoch, 3)
                with self.assertRaises(grpc.RpcError) as rejected:
                    stub.WorkerHeartbeat(pb.WorkerHeartbeatRequest(worker_id=33), timeout=2)
                self.assertEqual(rejected.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
                with self.assertRaises(grpc.RpcError) as missing:
                    stub.WorkerHeartbeat(pb.WorkerHeartbeatRequest(worker_id=999), timeout=2)
                self.assertEqual(missing.exception.code(), grpc.StatusCode.NOT_FOUND)
        finally:
            server.stop(0).wait()

    def test_real_worker_process_kill_releases_sequential_barrier(self):
        import manager_pb2 as pb
        import manager_pb2_grpc as pb_grpc
        manager = make_manager(workers=(11, 22), worker_timeout=0.5)
        gate = manager.consistency_gate
        manager.record_worker_heartbeat(22)
        gate.complete(22, 1)
        self.assertFalse(gate.can_start(2, worker_id=22))
        server, address = self.start_rpc(manager)
        manager.start_heartbeat_monitor(interval_s=0.02)
        script = (
            "import sys,time; "
            "from worker_heartbeat import WorkerHeartbeatSender; "
            "w=WorkerHeartbeatSender(sys.argv[1],11,interval_s=0.06,rpc_timeout_s=0.25); "
            "w.start(); time.sleep(30)"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, address],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.assertTrue(wait_for(lambda:
                manager.worker_health_snapshot()[11]["state"] == "ALIVE",
                timeout=3.0), "Worker process failed to begin heartbeating")
            started = time.monotonic()
            process.kill()
            process.wait(timeout=5)
            deadline = time.monotonic() + 4.0
            while (time.monotonic() < deadline
                   and manager.worker_health_snapshot()[11]["state"] != "EXCLUDED"):
                manager.record_worker_heartbeat(22)
                time.sleep(0.035)
            self.assertEqual(manager.worker_health_snapshot()[11]["state"], "EXCLUDED")
            self.assertEqual(manager.worker_health_snapshot()[22]["state"], "ALIVE")
            self.assertTrue(gate.can_start(2, worker_id=22))
            gate.complete(22, 2)
            with grpc.insecure_channel(address) as channel:
                stub = pb_grpc.OwnershipManagerStub(channel)
                can_start = stub.CanStart(pb.CanStartRequest(worker_id=22, iteration=3), timeout=2)
                self.assertTrue(can_start.allowed)
                with self.assertRaises(grpc.RpcError) as rejected:
                    stub.CompleteIteration(pb.CompleteIterationRequest(worker_id=11, iteration=1), timeout=2)
                self.assertEqual(rejected.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
            print(f"F4 observed worker-crash exclusion latency: {time.monotonic()-started:.3f}s")
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            manager.stop_heartbeat_monitor()
            server.stop(0).wait()


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F4.1 worker failure detection and barrier recovery: PASS")
    else:
        raise SystemExit(1)
