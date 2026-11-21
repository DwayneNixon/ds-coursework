"""F3.1 — manager-mediated promotion after a CONFIRMED process crash.

Run: python test_f31_replica_promotion.py

This milestone intentionally promotes a single failed partition READ-ONLY.
F3.2 will restore replica protection and new-Push progress. It does not
claim split-brain safety for unconfirmed failures or network partitions.
"""
import copy
import hashlib
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import grpc
import manager_pb2 as mpb
import manager_pb2_grpc as mgrpc
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pgrpc

from manager_rpc import ManagerService
from replica_transport import ReplicaPeer
from routing import KEY_SPACE_END, Partition, RoutingTable
from server_manager import ServerManager
from range_clocks import RangeClocks


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def wait_until(predicate, timeout_s=4.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def request(sequence, gradient=(1.0, 2.0)):
    req = pb.PushRequest(keys=[7], gradients=list(gradient), width=2)
    req.task_id.worker_id = 19
    req.task_id.sequence = sequence
    return req


class ReplicaPromotionTests(unittest.TestCase):
    def setUp(self):
        self.failed = free_address()
        self.backup = free_address()
        self.manager = ServerManager([
            Partition("p0", 0, KEY_SPACE_END // 2, self.failed),
            Partition("p1", KEY_SPACE_END // 2, KEY_SPACE_END, self.backup),
        ], heartbeat_timeout_s=0.55)

        self.manager_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        mgrpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.manager_server,
        )
        port = self.manager_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        self.manager_address = f"127.0.0.1:{port}"
        self.manager_server.start()
        self.manager.start_heartbeat_monitor(interval_s=0.025)

        self.processes = {}
        self.peers = {}
        file = Path(__file__).resolve().with_name("server.py")
        for address in (self.failed, self.backup):
            process = subprocess.Popen(
                [sys.executable, "-u", str(file), "--listen", address,
                 "--manager", self.manager_address, "--bootstrap-ownership",
                 "--heartbeat-interval", "0.05", "--heartbeat-rpc-timeout", "0.2"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            self.processes[address] = process
            self.peers[address] = ReplicaPeer(address, timeout_s=1.0)

        for address in (self.failed, self.backup):
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=8.0)
            except Exception as error:
                process = self.processes[address]
                output = process.communicate(timeout=2)[0] if process.poll() is not None else ""
                self.fail(f"Server {address} did not start: {error}\n{output}")
        self.assertTrue(wait_until(lambda: all(
            x["state"] == "ALIVE" for x in self.manager.heartbeat_snapshot().values()
        )), "Missing initial server heartbeats")

        self.manager.configure_replication([self.failed, self.backup], k=1)
        self.manager.seed_replicas(self.peers)

    def tearDown(self):
        for process in getattr(self, "processes", {}).values():
            if process.poll() is None:
                process.kill()
        for process in getattr(self, "processes", {}).values():
            try:
                process.communicate(timeout=3.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=3.0)
        for peer in getattr(self, "peers", {}).values():
            peer.close()
        if hasattr(self, "manager"):
            self.manager.stop_heartbeat_monitor()
        if hasattr(self, "manager_server"):
            self.manager_server.stop(0).wait()

    def activate(self, batch_size=1):
        self.manager.activate_live_replication(
            self.peers, batch_size=batch_size, batch_timeout_s=0.06,
        )

    def push(self, address, req):
        with grpc.insecure_channel(address) as channel:
            return pgrpc.ParameterServerStub(channel).Push(req, timeout=5.0)

    def pull(self, address):
        with grpc.insecure_channel(address) as channel:
            return pgrpc.ParameterServerStub(channel).Pull(
                pb.PullRequest(keys=[7]), timeout=3.0,
            )

    def kill_primary(self):
        process = self.processes[self.failed]
        process.kill()
        process.wait(timeout=5.0)
        self.assertTrue(wait_until(lambda:
            self.manager.heartbeat_snapshot()[self.failed]["state"] == "SUSPECTED",
            timeout_s=3.0,
        ))
        self.assertIsNone(self.processes[self.backup].poll())

    def promote(self):
        return self.manager.promote_confirmed_crash(
            self.failed, self.peers, confirmed_dead=True,
        )

    def test_requires_confirmed_crash_not_heartbeat_suspicion(self):
        self.activate()
        with self.assertRaisesRegex(RuntimeError, "confirmed crash-stop"):
            self.manager.promote_confirmed_crash(self.failed, self.peers)
        with self.assertRaisesRegex(RuntimeError, "SUSPECTED"):
            self.promote()  # Server is still alive.
        self.assertEqual(self.manager.get_map().epoch, 1)
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.failed)

    def test_prepared_replica_is_not_published_or_served(self):
        self.activate()
        current = self.manager.get_map()
        new_table = RoutingTable([
            Partition(p.partition_id, p.begin, p.end,
                      self.backup if p.partition_id == "p0" else p.owner)
            for p in current.partitions
        ], epoch=current.epoch + 1)
        peer = self.peers[self.backup]
        result = peer.prepare_replica_promotion(self.failed, "p0", new_table)
        self.assertTrue(result["prepared"])
        self.assertEqual(self.manager.get_map().epoch, 1)
        with self.assertRaises(grpc.RpcError) as caught:
            self.pull(self.backup)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
        self.assertTrue(peer.cancel_replica_promotion(self.failed, "p0", 2))
        self.assertFalse(peer.cancel_replica_promotion(self.failed, "p0", 2))
        self.assertEqual(self.manager.get_map().epoch, 1)

    def test_killed_primary_promotes_weights_clocks_and_dedup(self):
        self.activate()
        original = request(1)
        self.assertTrue(self.push(self.failed, original).accepted)
        self.assertTrue(self.push(self.failed, request(2)).accepted)
        before = self.peers[self.backup].get_replica_snapshot(self.failed, "p0")
        self.assertEqual(before["sequence"], 2)
        self.kill_primary()
        new_table = self.promote()
        self.assertEqual(new_table.epoch, 2)
        self.assertEqual(new_table.partitions[0].owner, self.backup)
        self.assertEqual(list(self.pull(self.backup).values), [-0.2, -0.4])

        # An uncertain duplicate is recognized from replicated Task IDs.
        self.assertTrue(self.push(self.backup, original).accepted)
        self.assertEqual(list(self.pull(self.backup).values), [-0.2, -0.4])
        with self.assertRaises(grpc.RpcError) as conflict:
            self.push(self.backup, request(1, gradient=(9.0, 9.0)))
        self.assertEqual(conflict.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)

        # New writes must NOT be acknowledged before F3.2 replica repair.
        with self.assertRaises(grpc.RpcError) as blocked:
            self.push(self.backup, request(3))
        self.assertEqual(blocked.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(list(self.pull(self.backup).values), [-0.2, -0.4])
        self.assertEqual(self.promote().epoch, 2)

        with self.assertRaises(grpc.RpcError) as stale:
            self.peers[self.backup].get_replica_snapshot(self.failed, "p0")
        self.assertEqual(stale.exception.code(), grpc.StatusCode.NOT_FOUND)

        # Heartbeats cannot change FAILED back to ALIVE after publication.
        with grpc.insecure_channel(self.manager_address) as channel:
            stub = mgrpc.OwnershipManagerStub(channel)
            map_reply = stub.GetMap(mpb.GetMapRequest(), timeout=3)
            self.assertEqual(map_reply.epoch, 2)
            with self.assertRaises(grpc.RpcError) as heartbeat:
                stub.Heartbeat(mpb.HeartbeatRequest(server_id=self.failed), timeout=3)
            self.assertEqual(
                heartbeat.exception.code(), grpc.StatusCode.FAILED_PRECONDITION,
            )
        self.manager.check_server_timeouts()
        self.assertEqual(self.manager.heartbeat_snapshot()[self.failed]["state"], "FAILED")

    def test_batched_primary_kill_promotes_all_completed_tasks(self):
        self.activate(batch_size=3)
        with ThreadPoolExecutor(max_workers=3) as pool:
            responses = list(pool.map(
                lambda req: self.push(self.failed, req),
                [request(1), request(2), request(3)],
            ))
        self.assertTrue(all(x.accepted for x in responses))
        self.kill_primary()
        self.assertEqual(self.promote().epoch, 2)
        for actual, expected in zip(self.pull(self.backup).values, (-0.3, -0.6)):
            self.assertAlmostEqual(actual, expected)
        for sequence in (1, 2, 3):
            self.assertTrue(self.push(self.backup, request(sequence)).accepted)
        for actual, expected in zip(self.pull(self.backup).values, (-0.3, -0.6)):
            self.assertAlmostEqual(actual, expected)
        with self.assertRaises(grpc.RpcError) as blocked:
            self.push(self.backup, request(4))
        self.assertEqual(blocked.exception.code(), grpc.StatusCode.UNAVAILABLE)

    def test_commit_response_loss_retries_same_epoch(self):
        self.activate()
        self.assertTrue(self.push(self.failed, request(1)).accepted)
        self.kill_primary()
        peer = self.peers[self.backup]
        original = peer.commit_replica_promotion

        def commit_then_lose_reply(*args):
            original(*args)
            raise ConnectionError("simulated lost promotion reply")

        with patch.object(peer, "commit_replica_promotion", side_effect=commit_then_lose_reply):
            with self.assertRaises(ConnectionError):
                self.promote()
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.backup)
        self.assertEqual(self.promote().epoch, 2)
        self.assertEqual(list(self.pull(self.backup).values), [-0.1, -0.2])
        self.assertTrue(self.push(self.backup, request(1)).accepted)

    def test_uncertain_push_already_on_backup_is_not_applied_twice(self):
        self.activate()
        op = request(1)
        clocks = RangeClocks(0, KEY_SPACE_END // 2)
        clocks.record(7, 8, 19, 1)
        record = {
            "primary_id": self.failed,
            "partition_id": "p0",
            "epoch": 1,
            "sequence": 1,
            "task_id": (19, 1),
            "fingerprint": hashlib.sha256(
                op.SerializeToString(deterministic=True)
            ).digest(),
            "bounds": (7, 8),
            "weights": {7: [-0.1, -0.2]},
            "clock_segments": clocks.snapshot(),
        }
        # Simulate backup accepting the record immediately before primary
        # process dies, without a worker acknowledgement or local commit.
        self.assertTrue(self.peers[self.backup].apply_replica_update(record))
        self.kill_primary()
        self.promote()
        self.assertEqual(list(self.pull(self.backup).values), [-0.1, -0.2])
        self.assertTrue(self.push(self.backup, op).accepted)
        self.assertEqual(list(self.pull(self.backup).values), [-0.1, -0.2])

    def test_no_healthy_replica_does_not_change_map(self):
        self.activate()
        self.kill_primary()
        backup_proc = self.processes[self.backup]
        backup_proc.kill()
        backup_proc.wait(timeout=4.0)
        self.assertTrue(wait_until(lambda:
            self.manager.heartbeat_snapshot()[self.backup]["state"] == "SUSPECTED",
            timeout_s=3.0,
        ))
        with self.assertRaisesRegex(RuntimeError, "No healthy"):
            self.promote()
        self.assertEqual(self.manager.get_map().epoch, 1)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F3.1 confirmed-crash replica promotion: PASS")
    else:
        raise SystemExit(1)
