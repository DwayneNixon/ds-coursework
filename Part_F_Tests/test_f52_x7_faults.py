"""F5 / X7: real-process worker crash and deterministic message faults.

Place beside the existing F3.4 and F4.2 test files in the repository root.
Run independently with: python -m unittest -v test_f52_x7_faults

All gRPC servers are real. FaultInjectedPushLink is an explicitly test-only
RPC-boundary shim: it drops a request BEFORE forwarding it, delays forwarding,
or loses the response AFTER the remote RPC has returned successfully. These
are controlled faults, not an assertion that arbitrary TCP partitions are safe.
"""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import grpc
import manager_pb2_grpc as manager_grpc
import parameter_server_pb2 as data_pb
import parameter_server_pb2_grpc as data_grpc

from coordinated_client import CoordinatedClient
from distributed_client import DistributedParameterClient
from manager_rpc import ManagerService
from routing import KEY_SPACE_END, Partition
from server import ParameterServer
from server_manager import ServerManager
import Part_F_Tests.test_f34_migration_crash_recovery as migration_tests


def until(predicate, timeout_s=5.0):
    until_s = time.monotonic() + timeout_s
    while time.monotonic() < until_s:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def save_case_metrics(**metrics):
    """Write only observations made by this test, never a predicted outcome."""
    root = os.environ.get("DS_X7_CASE_DIR")
    case_id = os.environ.get("DS_X7_CASE_ID")
    if root and case_id:
        output = Path(root)
        output.mkdir(parents=True, exist_ok=True)
        (output / f"{case_id}.json").write_text(
            json.dumps(metrics, indent=2, sort_keys=True, allow_nan=False),
            encoding="utf-8",
        )


class DroppedMessage(ConnectionError):
    """An intentionally discarded request/response at the client RPC boundary."""


class FaultInjectedPushLink:
    """Transmit via a real gRPC stub, with a deterministic one-shot fault.

    `drop_request`: do not send the first request; simulate an unavailable link.
    `drop_reply`: execute the first RPC on the server, discard its successful ACK.
    `delay`: sleep BEFORE forwarding the first RPC, measuring the added latency.
    Only the test harness retries. Task IDs remain unchanged across attempts.
    """

    def __init__(self, stub, *, mode, delay_s=0.25):
        if mode not in ("drop_request", "drop_reply", "delay"):
            raise ValueError("Unsupported message-fault mode")
        if delay_s < 0:
            raise ValueError("Negative delay")
        self.stub = stub
        self.mode = mode
        self.delay_s = delay_s
        self.attempts = 0
        self.forwarded = 0

    def push(self, request):
        self.attempts += 1
        first = self.attempts == 1
        if first and self.mode == "drop_request":
            raise DroppedMessage("injected request drop before network send")
        if first and self.mode == "delay":
            time.sleep(self.delay_s)
        response = self.stub.Push(request, timeout=4)
        self.forwarded += 1
        if first and self.mode == "drop_reply":
            raise DroppedMessage("injected reply loss after remote acceptance")
        return response


class MessageFaultCases(unittest.TestCase):
    """Use the established three-PROCESS F3.4 fixture; do not fake replication."""

    def setUp(self):
        self.cluster = migration_tests.MigrationCrashTests(
            "test_successful_full_range_cutover_and_replica_reseed"
        )
        self.cluster.setUp()

    def tearDown(self):
        self.cluster.tearDown()

    def assert_primary_and_shadow(self, updates):
        c = self.cluster
        c.assert_weights(c.s1, c.a, updates)
        shadow = c.peers[c.s2].get_replica_snapshot(c.s1, "p0")
        self.assertEqual(shadow["sequence"], updates)
        self.assertEqual(len(shadow["completed_pushes"]), updates)
        return shadow

    def test_delayed_push_rpc_preserves_acknowledged_order(self):
        c = self.cluster
        with grpc.insecure_channel(c.s1) as channel:
            link = FaultInjectedPushLink(data_grpc.ParameterServerStub(channel),
                                         mode="delay", delay_s=0.25)
            first = migration_tests.push_request(c.a, 401)
            started = time.monotonic()
            self.assertTrue(link.push(first).accepted)
            elapsed = time.monotonic() - started
            self.assertGreaterEqual(elapsed, 0.24)
            self.assertTrue(link.push(migration_tests.push_request(c.a, 402)).accepted)
        self.assert_primary_and_shadow(2)
        save_case_metrics(injection="delay_first_push_before_grpc_send",
                          injected_delay_s=0.25, measured_first_rpc_s=elapsed,
                          rpc_attempts=2, forwarded=2, acknowledged_unique_pushes=2,
                          final_first_component=-0.2, replica_sequence=2)

    def test_dropped_push_request_retries_without_extra_application(self):
        c = self.cluster
        with grpc.insecure_channel(c.s1) as channel:
            link = FaultInjectedPushLink(data_grpc.ParameterServerStub(channel),
                                         mode="drop_request")
            request = migration_tests.push_request(c.a, 501)
            with self.assertRaisesRegex(DroppedMessage, "request drop"):
                link.push(request)
            self.assertEqual(link.forwarded, 0)
            self.assert_primary_and_shadow(0)
            self.assertTrue(link.push(request).accepted)
        self.assert_primary_and_shadow(1)
        save_case_metrics(injection="drop_first_push_before_grpc_send",
                          rpc_attempts=2, forwarded=1, acknowledged_unique_pushes=1,
                          final_first_component=-0.1, replica_sequence=1)

    def test_lost_push_reply_replay_same_task_id_exactly_once(self):
        c = self.cluster
        with grpc.insecure_channel(c.s1) as channel:
            link = FaultInjectedPushLink(data_grpc.ParameterServerStub(channel),
                                         mode="drop_reply")
            request = migration_tests.push_request(c.a, 601)
            with self.assertRaisesRegex(DroppedMessage, "reply loss"):
                link.push(request)
            # The server committed and replicated BEFORE its ACK was discarded.
            self.assert_primary_and_shadow(1)
            self.assertTrue(link.push(request).accepted)
        self.assert_primary_and_shadow(1)
        save_case_metrics(injection="drop_first_successful_push_reply",
                          rpc_attempts=2, forwarded=2, acknowledged_unique_pushes=1,
                          final_first_component=-0.1, replica_sequence=1,
                          replayed_task_id=[71, 601])

    def test_dropped_transfer_staging_rpc_aborts_before_publication(self):
        c = self.cluster
        self.assertTrue(c.push(c.s1, c.a, 701).accepted)
        original_epoch = c.manager.get_map().epoch
        with patch.object(c.peers[c.s2], "stage_incoming_transfer",
                          side_effect=ConnectionError("injected dropped staging RPC")):
            with self.assertRaisesRegex(ConnectionError, "dropped staging RPC"):
                c.migrate(0, migration_tests.THIRD)
        self.assertEqual(c.manager.get_map().epoch, original_epoch)
        self.assertEqual(c.manager.get_map().partitions[0].owner, c.s1)
        self.assertTrue(c.push(c.s1, c.a, 702).accepted)
        self.assert_primary_and_shadow(2)
        c.assert_repaired(original_epoch)
        save_case_metrics(injection="drop_manager_to_destination_stage_rpc",
                          ownership_epoch_before=original_epoch,
                          ownership_epoch_after=c.manager.get_map().epoch,
                          unpublished_transfer_aborted=True,
                          acknowledged_unique_pushes=2,
                          replica_sequence=2)


class BoundedWorkerCrashCase(unittest.TestCase):
    def test_real_worker_kill_unblocks_bounded_delay_and_keeps_weights(self):
        """Crash an actual coordinated worker subprocess while tau=1 blocks peer."""
        data = ParameterServer(value_width=2)
        data_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        data_grpc.add_ParameterServerServicer_to_server(data, data_server)
        data_port = data_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(data_port, 0)
        data_address = f"127.0.0.1:{data_port}"
        manager = ServerManager(
            [Partition("p0", 0, KEY_SPACE_END, data_address)],
            heartbeat_timeout_s=100,
        )
        manager.configure_consistency([11, 22], mode="bounded", tau=1)
        manager.configure_worker_failure_detection(timeout_s=0.65)
        data.set_ownership(data_address, manager.get_map())
        data_server.start()
        manager_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        manager_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(manager), manager_server)
        port = manager_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        manager_server.start()
        manager_address = f"127.0.0.1:{port}"
        # Begin timeout monitoring only after both Python worker processes
        # have registered: interpreter startup is not a worker crash.
        process = None
        worker = None
        try:
            child = '''
import sys, time
from distributed_client import DistributedParameterClient
from coordinated_client import CoordinatedClient
address = sys.argv[1]
client = DistributedParameterClient.from_manager(address, worker_id=11)
worker = CoordinatedClient(client, address, worker_id=11,
                           total_iterations=4, worker_heartbeats=True,
                           heartbeat_interval_s=0.05,
                           heartbeat_rpc_timeout_s=0.2)
worker.wait_for_turn(1)
time.sleep(45)  # Alive but intentionally stalled between iterations.
'''
            process = subprocess.Popen(
                [sys.executable, "-c", child, manager_address],
                cwd=str(Path(__file__).resolve().parent),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            self.assertTrue(until(lambda:
                manager.worker_health_snapshot()[11]["state"] == "ALIVE", 5.0),
                "The worker process did not begin heartbeating")
            distributed = DistributedParameterClient.from_manager(
                manager_address, worker_id=22, value_width=2,
            )
            worker = CoordinatedClient(
                distributed, manager_address, 22, total_iterations=4,
                worker_heartbeats=True, heartbeat_interval_s=0.05,
                heartbeat_rpc_timeout_s=0.2,
            )
            worker.wait_for_turn(1)
            self.assertTrue(until(lambda:
                manager.worker_health_snapshot()[22]["state"] == "ALIVE", 3.0))
            manager.start_heartbeat_monitor(interval_s=0.02)
            for iteration in (1, 2):
                worker.wait_for_turn(iteration)
                task = worker.push([10], [1.0, 1.0], width=2)
                self.assertTrue(task.wait(timeout_ms=5000).accepted)
                worker.iteration_finished(iteration, wait_for_next=False)
            self.assertFalse(manager.consistency_gate.can_start(3, worker_id=22))
            with ThreadPoolExecutor(max_workers=1) as pool:
                waiting = pool.submit(worker.wait_for_turn, 3)
                time.sleep(0.9)  # Longer than timeout: both heartbeats keep peers alive.
                self.assertFalse(waiting.done())
                self.assertEqual(manager.worker_health_snapshot()[11]["state"], "ALIVE")
                self.assertEqual(manager.worker_health_snapshot()[22]["state"], "ALIVE")
                killed_at = time.monotonic()
                process.kill()
                process.wait(timeout=5)
                self.assertTrue(until(lambda:
                    manager.worker_health_snapshot()[11]["state"] == "EXCLUDED", 5.0))
                waiting.result(timeout=3)
                latency = time.monotonic() - killed_at
            task = worker.push([10], [1.0, 1.0], width=2)
            self.assertTrue(task.wait(timeout_ms=5000).accepted)
            worker.iteration_finished(3, wait_for_next=False)
            weights = list(worker.pull([10]).wait(timeout_ms=5000).values)
            self.assertEqual(len(weights), 2)
            for value in weights:
                self.assertAlmostEqual(value, -0.3, places=9)
            self.assertEqual(manager.consistency_gate.snapshot(), {11: 0, 22: 3})
            self.assertEqual(manager.consistency_gate.active_snapshot(), {22: 3})
            self.assertEqual(data.range_clocks["p0"].clock_for_key(10), {22: 3})
            save_case_metrics(injection="kill_real_coordinated_worker_process",
                              consistency="bounded", tau=1,
                              crash_exclusion_latency_s=latency,
                              completed_survivor_pushes=3,
                              final_weights=weights,
                              historical_progress={"11": 0, "22": 3},
                              active_worker_ids=[22])
            print("X7 bounded worker crash exclusion latency: %.3fs" % latency)
        finally:
            if worker is not None:
                worker.close()
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
            manager.stop_heartbeat_monitor()
            manager_server.stop(0).wait()
            data_server.stop(0).wait()


if __name__ == "__main__":
    unittest.main(verbosity=2)
