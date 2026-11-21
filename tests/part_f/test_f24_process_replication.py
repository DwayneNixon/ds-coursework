"""F2.4: real independent server processes and gRPC backup traffic.

Run: python test_f24_process_replication.py
No change to manager.proto/manager_pb2 is required for F2.4.
"""
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

import grpc
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc
import manager_pb2_grpc as manager_grpc

from manager_rpc import ManagerService
from replica_transport import ReplicaPeer, pack, unpack
from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def wait_until(predicate, seconds=6.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def push_request(sequence, key=7):
    request = pb.PushRequest(keys=[key], gradients=[1.0, 2.0], width=2)
    request.task_id.worker_id = 19
    request.task_id.sequence = sequence
    return request


class CodecTests(unittest.TestCase):
    def test_codec_preserves_exact_u64_keys_clock_tuples_and_fingerprints(self):
        huge = (1 << 64) - 1
        record = {
            "weights": {huge: [1.2, -3.4]},
            "clock_segments": [(huge - 1, 1 << 64, {19: 500})],
            "completed_pushes": {(19, huge): (hashlib.sha256(b"x").digest(), (huge, 1 << 64))},
            "bytes": b"\x00\xff",
        }
        self.assertEqual(unpack(pack(record)), record)

    def test_codec_rejects_non_finite_and_unsupported_values(self):
        with self.assertRaises(ValueError):
            pack({"a": float("nan")})
        with self.assertRaises(TypeError):
            pack({"a": object()})

    def test_invalid_peer_configuration_is_rejected(self):
        with self.assertRaises(ValueError):
            ReplicaPeer("")
        with self.assertRaises(ValueError):
            ReplicaPeer("localhost:1234", timeout_s=0)


class ProcessReplicationTests(unittest.TestCase):
    def setUp(self):
        self.primary = free_address()
        self.backup = free_address()
        self.manager = ServerManager([
            Partition("p0", 0, KEY_SPACE_END // 2, self.primary),
            Partition("p1", KEY_SPACE_END // 2, KEY_SPACE_END, self.backup),
        ], heartbeat_timeout_s=0.6)
        self.manager_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        manager_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.manager_server
        )
        manager_port = self.manager_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(manager_port, 0)
        self.manager_address = f"127.0.0.1:{manager_port}"
        self.manager_server.start()
        self.manager.start_heartbeat_monitor(interval_s=0.025)

        self.processes = {}
        self.peers = {}
        server_file = Path(__file__).resolve().parents[2] / "server.py"
        self.assertTrue(server_file.is_file())
        for address in (self.primary, self.backup):
            process = subprocess.Popen(
                [sys.executable, "-u", str(server_file), "--listen", address,
                 "--manager", self.manager_address, "--bootstrap-ownership",
                 "--heartbeat-interval", "0.05", "--heartbeat-rpc-timeout", "0.2"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.processes[address] = process
            self.peers[address] = ReplicaPeer(address, timeout_s=1.0)

        for address in (self.primary, self.backup):
            process = self.processes[address]
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=8.0)
            except Exception as error:
                output = ""
                if process.poll() is not None:
                    output = process.communicate(timeout=3)[0]
                self.fail(f"Server {address} failed to start: {error}\n{output}")
        self.assertTrue(wait_until(lambda: all(
            entry["state"] == "ALIVE"
            for entry in self.manager.heartbeat_snapshot().values()
        )), "Server heartbeats did not arrive")

    def tearDown(self):
        for process in getattr(self, "processes", {}).values():
            if process.poll() is None:
                process.kill()
        for process in getattr(self, "processes", {}).values():
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=3)
        for peer in getattr(self, "peers", {}).values():
            peer.close()
        if hasattr(self, "manager"):
            self.manager.stop_heartbeat_monitor()
        if hasattr(self, "manager_server"):
            self.manager_server.stop(0).wait()

    def activate(self, batch_size):
        self.manager.configure_replication([self.primary, self.backup], k=1)
        seeded = self.manager.seed_replicas(self.peers)
        self.assertEqual(len(seeded), 2)
        self.manager.activate_live_replication(
            self.peers, batch_size=batch_size, batch_timeout_s=0.08,
        )

    def worker_push(self, request):
        with grpc.insecure_channel(self.primary) as channel:
            return pb_grpc.ParameterServerStub(channel).Push(request, timeout=4.0)

    def check_primary_kill(self, expected_sequence, expected_weight):
        # A shadow backup must hold the completed state, independently.
        replica = self.peers[self.backup].get_replica_snapshot(self.primary, "p0")
        self.assertEqual(replica["sequence"], expected_sequence)
        self.assertEqual(len(replica["completed_pushes"]), expected_sequence)
        for actual, expected in zip(replica["weights"][7], expected_weight):
            self.assertAlmostEqual(actual, expected)

        # It remains a shadow replica, not worker-visible primary ownership.
        with grpc.insecure_channel(self.backup) as channel:
            with self.assertRaises(grpc.RpcError) as error:
                pb_grpc.ParameterServerStub(channel).Pull(
                    pb.PullRequest(keys=[7]), timeout=2.0
                )
            self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)

        crashed_at = time.monotonic()
        process = self.processes[self.primary]
        process.kill()
        process.wait(timeout=5.0)
        self.assertTrue(wait_until(lambda:
            self.manager.heartbeat_snapshot()[self.primary]["state"] == "SUSPECTED",
            seconds=3.0,
        ), "Manager did not detect primary process crash")
        latency = self.manager.heartbeat_snapshot()[self.primary]["suspected_at_s"] - crashed_at
        print(f"F2.4 primary-process kill detection: {latency:.3f}s", flush=True)
        self.assertIsNone(self.processes[self.backup].poll(), "Backup process died with primary")

        surviving = self.peers[self.backup].get_replica_snapshot(self.primary, "p0")
        self.assertEqual(surviving, replica)
        # Failover is F3: the manager ownership map remains unchanged here.
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.primary)

    def test_per_push_cross_process_replication_survives_primary_kill(self):
        self.activate(batch_size=1)
        reply = self.worker_push(push_request(1))
        self.assertTrue(reply.accepted)
        self.check_primary_kill(1, (-0.1, -0.2))

    def test_batched_cross_process_replication_survives_primary_kill(self):
        self.activate(batch_size=3)
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(self.worker_push, [
                push_request(1), push_request(2), push_request(3)
            ]))
        self.assertTrue(all(reply.accepted for reply in results))
        self.check_primary_kill(3, (-0.3, -0.6))

    def test_backup_kill_prevents_primary_ack_and_fences(self):
        self.activate(batch_size=1)
        backup_process = self.processes[self.backup]
        backup_process.kill()
        backup_process.wait(timeout=5.0)
        with self.assertRaises(grpc.RpcError) as error:
            self.worker_push(push_request(1))
        self.assertEqual(error.exception.code(), grpc.StatusCode.UNAVAILABLE)
        state = self.peers[self.primary].live_replication_snapshot()
        self.assertIn("p0", state["faulted_partitions"])
        self.assertEqual(state["sequences"]["p0"], 0)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F2.4 cross-process replication: PASS")
    else:
        raise SystemExit(1)
