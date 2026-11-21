"""F3.2: restore backup protection after a confirmed primary crash.

Run from the project root: python test_f32_replication_repair.py

Three independent server processes, three primary ranges, k=1. F3.1
promotes the failed range; F3.2 re-seeds the new epoch before allowing new
Pushes on any repaired owner. Uses the existing worker-facing gRPC protocol.
"""
import socket
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import grpc
import manager_pb2_grpc as mgrpc
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pgrpc

from manager_rpc import ManagerService
from replica_transport import ReplicaPeer
from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


THIRD = KEY_SPACE_END // 3


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def wait_until(predicate, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.025)
    return bool(predicate())


def request(key, sequence, gradient=(1.0, 2.0)):
    req = pb.PushRequest(keys=[key], gradients=list(gradient), width=2)
    req.task_id.worker_id = 19
    req.task_id.sequence = sequence
    return req


class RepairProcessTests(unittest.TestCase):
    def setUp(self):
        self.s1, self.s2, self.s3 = (free_address() for _ in range(3))
        self.keys = (7, THIRD + 7, 2 * THIRD + 7)
        self.manager = ServerManager([
            Partition("p0", 0, THIRD, self.s1),
            Partition("p1", THIRD, 2 * THIRD, self.s2),
            Partition("p2", 2 * THIRD, KEY_SPACE_END, self.s3),
        ], heartbeat_timeout_s=0.5)
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
        script = Path(__file__).resolve().with_name("server.py")
        for address in (self.s1, self.s2, self.s3):
            process = subprocess.Popen(
                [sys.executable, "-u", str(script), "--listen", address,
                 "--manager", self.manager_address, "--bootstrap-ownership",
                 "--heartbeat-interval", "0.05", "--heartbeat-rpc-timeout", "0.2"],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True,
            )
            self.processes[address] = process
            self.peers[address] = ReplicaPeer(address, timeout_s=2.0)
        for address in (self.s1, self.s2, self.s3):
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=8.0)
            except Exception as error:
                proc = self.processes[address]
                output = proc.communicate(timeout=2)[0] if proc.poll() is not None else ""
                self.fail(f"Server failed to start: {error}\n{output}")
        self.assertTrue(wait_until(lambda: all(
            entry["state"] == "ALIVE"
            for entry in self.manager.heartbeat_snapshot().values()
        )), "Server heartbeat missing")
        self.manager.configure_replication([self.s1, self.s2, self.s3], k=1)
        self.manager.seed_replicas(self.peers)

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

    def push(self, server_id, req):
        with grpc.insecure_channel(server_id) as channel:
            return pgrpc.ParameterServerStub(channel).Push(req, timeout=5)

    def pull(self, server_id, key):
        with grpc.insecure_channel(server_id) as channel:
            return pgrpc.ParameterServerStub(channel).Pull(
                pb.PullRequest(keys=[key]), timeout=3,
            )

    def failover(self, *, batch_size=1):
        self.manager.activate_live_replication(
            self.peers, batch_size=batch_size, batch_timeout_s=0.06,
        )
        self.assertTrue(self.push(self.s1, request(self.keys[0], 1)).accepted)
        self.assertTrue(self.push(self.s2, request(self.keys[1], 2)).accepted)
        self.assertTrue(self.push(self.s3, request(self.keys[2], 3)).accepted)
        process = self.processes[self.s1]
        process.kill()
        process.wait(timeout=5)
        self.assertTrue(wait_until(lambda:
            self.manager.heartbeat_snapshot()[self.s1]["state"] == "SUSPECTED"
        ), "Failed primary was not detected")
        table = self.manager.promote_confirmed_crash(
            self.s1, self.peers, confirmed_dead=True,
        )
        self.assertEqual(table.epoch, 2)
        self.assertEqual(table.partitions[0].owner, self.s2)
        with self.assertRaises(grpc.RpcError) as caught:
            self.push(self.s2, request(self.keys[0], 4))
        self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)

    def repaired_plan(self, batch_size=1):
        return self.manager.restore_replication_after_failover(
            self.peers, batch_size=batch_size, batch_timeout_s=0.06,
        )

    def assert_values(self, server_id, key, want):
        actual = list(self.pull(server_id, key).values)
        for got, expected in zip(actual, want):
            self.assertAlmostEqual(got, expected)

    def test_repair_restores_all_three_ranges_and_new_pushes(self):
        self.failover()
        plan = self.repaired_plan()
        self.assertEqual(plan, {
            "p0": (self.s3,), "p1": (self.s3,), "p2": (self.s2,),
        })
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertEqual(self.manager.get_replication_plan(), plan)
        self.assertEqual(self.manager.restore_replication_after_failover(
            self.peers, batch_size=1, batch_timeout_s=0.06,
        ), plan)
        for owner in (self.s2, self.s3):
            live = self.peers[owner].live_replication_snapshot()
            self.assertFalse(live["repair_write_fenced"])
            self.assertFalse(live["faulted_partitions"])
        # Previously completed Push is not applied again after promotion/repair.
        self.assertTrue(self.push(self.s2, request(self.keys[0], 1)).accepted)
        self.assert_values(self.s2, self.keys[0], [-0.1, -0.2])
        self.assertTrue(self.push(self.s2, request(self.keys[0], 4)).accepted)
        self.assertTrue(self.push(self.s2, request(self.keys[1], 5)).accepted)
        self.assertTrue(self.push(self.s3, request(self.keys[2], 6)).accepted)
        for sid, key in ((self.s2, self.keys[0]),
                         (self.s2, self.keys[1]),
                         (self.s3, self.keys[2])):
            self.assert_values(sid, key, [-0.2, -0.4])
        for primary, pid, backup in ((self.s2, "p0", self.s3),
                                     (self.s2, "p1", self.s3),
                                     (self.s3, "p2", self.s2)):
            shadow = self.peers[backup].get_replica_snapshot(primary, pid)
            self.assertEqual(shadow["epoch"], 2)
            self.assertEqual(shadow["sequence"], 1)
            self.assertEqual(len(shadow["completed_pushes"]), 2)
        self.assertIsNone(self.processes[self.s2].poll())
        self.assertIsNone(self.processes[self.s3].poll())

    def test_failed_seed_stays_fenced_and_retry_works(self):
        self.failover()
        peer = self.peers[self.s3]
        original = peer.install_repair_replica_snapshot
        calls = 0

        def fail_once(snapshot):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("test repair-seed outage")
            return original(snapshot)

        with patch.object(peer, "install_repair_replica_snapshot", side_effect=fail_once):
            with self.assertRaises(ConnectionError):
                self.repaired_plan()
        self.assertEqual(self.manager.get_map().epoch, 2)
        for sid, key, seq in ((self.s2, self.keys[0], 4),
                              (self.s3, self.keys[2], 5)):
            with self.assertRaises(grpc.RpcError) as caught:
                self.push(sid, request(key, seq))
            self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.repaired_plan()["p0"], (self.s3,))
        self.assertTrue(self.push(self.s2, request(self.keys[0], 4)).accepted)
        self.assert_values(self.s2, self.keys[0], [-0.2, -0.4])

    def test_lost_release_ack_retry_does_not_reseed_after_new_write(self):
        self.failover()
        peer = self.peers[self.s3]
        original = peer.release_repaired_replication

        def release_then_lose_reply(epoch):
            original(epoch)
            raise ConnectionError("lost final release acknowledgement")

        with patch.object(peer, "release_repaired_replication", side_effect=release_then_lose_reply):
            with self.assertRaises(ConnectionError):
                self.repaired_plan()
        self.assertEqual(self.manager.get_map().epoch, 2)
        # The earlier S2 release is already safe because every new shadow
        # was prepared BEFORE either owner was allowed to write.
        self.assertTrue(self.push(self.s2, request(self.keys[0], 4)).accepted)
        shadow_before = self.peers[self.s3].get_replica_snapshot(self.s2, "p0")
        self.assertEqual(shadow_before["sequence"], 1)
        self.repaired_plan()  # Only release operations, NO re-seeding.
        shadow_after = self.peers[self.s3].get_replica_snapshot(self.s2, "p0")
        self.assertEqual(shadow_after["sequence"], 1)
        self.assert_values(self.s2, self.keys[0], [-0.2, -0.4])
        self.assertTrue(self.push(self.s3, request(self.keys[2], 5)).accepted)

    def test_lost_prepare_ack_can_be_retried_without_early_writes(self):
        self.failover()
        peer = self.peers[self.s2]
        original = peer.prepare_repaired_replication

        def prepare_then_lose_reply(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionError("lost prepare acknowledgement")

        with patch.object(peer, "prepare_repaired_replication",
                          side_effect=prepare_then_lose_reply):
            with self.assertRaises(ConnectionError):
                self.repaired_plan()
        for sid, key in ((self.s2, self.keys[0]),
                         (self.s3, self.keys[2])):
            with self.assertRaises(grpc.RpcError) as caught:
                self.push(sid, request(key, 4))
            self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.repaired_plan()["p0"], (self.s3,))
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertTrue(self.push(self.s2, request(self.keys[0], 4)).accepted)

    def test_unsafe_replication_mode_change_is_rejected_before_repair(self):
        self.failover(batch_size=3)
        with self.assertRaisesRegex(ValueError, "active batch settings"):
            self.repaired_plan(batch_size=1)
        # Wrong settings must not create a sticky pending repair.
        self.assertEqual(self.repaired_plan(batch_size=3)["p0"], (self.s3,))
        self.assertTrue(self.push(self.s2, request(self.keys[0], 4)).accepted)

    def test_batched_pushes_resume_after_repair(self):
        self.failover(batch_size=3)
        self.repaired_plan(batch_size=3)
        reqs = [request(self.keys[0], i) for i in (4, 5, 6)]
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda req: self.push(self.s2, req), reqs))
        self.assertTrue(all(r.accepted for r in results))
        self.assert_values(self.s2, self.keys[0], [-0.4, -0.8])
        shadow = self.peers[self.s3].get_replica_snapshot(self.s2, "p0")
        self.assertEqual(shadow["sequence"], 3)
        self.assertEqual(len(shadow["completed_pushes"]), 4)

    def test_insufficient_survivors_refuses_repair_and_keeps_fence(self):
        self.failover()
        # Two crashes exceed the k=1 tolerance; without a second live
        # server, the manager must not silently remove the write fence.
        process = self.processes[self.s3]
        process.kill()
        process.wait(timeout=5)
        self.assertTrue(wait_until(lambda:
            self.manager.heartbeat_snapshot()[self.s3]["state"] == "SUSPECTED"
        ))
        with self.assertRaisesRegex(RuntimeError, "Insufficient surviving"):
            self.repaired_plan()
        self.assertEqual(self.manager.get_map().epoch, 2)
        with self.assertRaises(grpc.RpcError) as caught:
            self.push(self.s2, request(self.keys[0], 4))
        self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F3.2 replication repair and training resumption: PASS")
    else:
        raise SystemExit(1)
