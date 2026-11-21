"""F3.4 — fenced replicated E1 migration and crash recovery.

Runs three separate parameter-server processes and a real manager gRPC
endpoint. No manually launched servers or new .proto generation is needed.
Run: python test_f34_migration_crash_recovery.py
"""
import socket
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import manager_pb2_grpc as manager_grpc
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from manager_rpc import ManagerService
from replica_transport import ReplicaPeer
from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager

THIRD = KEY_SPACE_END // 3


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return "127.0.0.1:" + str(sock.getsockname()[1])


def eventually(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def push_request(key, seq, grad=(1.0, 2.0)):
    request = pb.PushRequest(keys=[key], gradients=list(grad), width=2)
    request.task_id.worker_id = 71
    request.task_id.sequence = seq
    return request


class MigrationCrashTests(unittest.TestCase):
    def setUp(self):
        self.s1, self.s2, self.s3 = (free_address() for _ in range(3))
        self.ids = (self.s1, self.s2, self.s3)
        self.manager = ServerManager([
            Partition("p0", 0, THIRD, self.s1),
            Partition("p1", THIRD, 2 * THIRD, self.s2),
            Partition("p2", 2 * THIRD, KEY_SPACE_END, self.s3),
        ], heartbeat_timeout_s=0.4)
        self.grpc_manager = grpc.server(ThreadPoolExecutor(max_workers=8))
        manager_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.grpc_manager,
        )
        port = self.grpc_manager.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        self.manager_address = f"127.0.0.1:{port}"
        self.grpc_manager.start()
        self.manager.start_heartbeat_monitor(interval_s=0.02)
        self.processes = {}
        self.peers = {}
        program = Path(__file__).resolve().with_name("server.py")
        for address in self.ids:
            proc = subprocess.Popen([
                sys.executable, "-u", str(program),
                "--listen", address,
                "--manager", self.manager_address,
                "--bootstrap-ownership",
                "--heartbeat-interval", "0.05",
                "--heartbeat-rpc-timeout", "0.2",
            ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True)
            self.processes[address] = proc
            self.peers[address] = ReplicaPeer(address, timeout_s=1.5)
        for address in self.ids:
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=8)
            except Exception as error:
                proc = self.processes[address]
                output = proc.communicate(timeout=2)[0] if proc.poll() is not None else ""
                self.fail(f"Server {address} startup failed: {error}\n{output}")
        self.assertTrue(eventually(lambda: all(
            health["state"] == "ALIVE"
            for health in self.manager.heartbeat_snapshot().values()
        )), "One or more server heartbeats were not received")
        self.manager.configure_replication(self.ids, k=1)
        self.manager.seed_replicas(self.peers)
        self.manager.activate_live_replication(
            self.peers, batch_size=1, batch_timeout_s=0.05,
        )
        self.a, self.b, self.c = 9, THIRD + 9, 2 * THIRD + 9

    def tearDown(self):
        for proc in getattr(self, "processes", {}).values():
            if proc.poll() is None:
                proc.kill()
        for proc in getattr(self, "processes", {}).values():
            try:
                proc.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=3)
        for peer in getattr(self, "peers", {}).values():
            peer.close()
        if hasattr(self, "manager"):
            self.manager.stop_heartbeat_monitor()
        if hasattr(self, "grpc_manager"):
            self.grpc_manager.stop(0).wait()

    def push(self, address, key, sequence):
        with grpc.insecure_channel(address) as channel:
            return pb_grpc.ParameterServerStub(channel).Push(
                push_request(key, sequence), timeout=3,
            )

    def pull(self, address, key):
        with grpc.insecure_channel(address) as channel:
            return list(pb_grpc.ParameterServerStub(channel).Pull(
                pb.PullRequest(keys=[key]), timeout=3,
            ).values)

    def kill(self, address):
        proc = self.processes[address]
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    def migrate(self, begin, end, *, checkpoint=None):
        return self.manager.move_range_replicated(
            self.s1, self.s2, begin, end, self.peers,
            checkpoint=checkpoint,
            is_confirmed_dead=lambda sid: self.processes[sid].poll() is not None,
        )

    def assert_weights(self, address, key, updates):
        values = self.pull(address, key)
        self.assertAlmostEqual(values[0], -0.1 * updates)
        self.assertAlmostEqual(values[1], -0.2 * updates)

    def assert_repaired(self, expected_epoch):
        self.assertEqual(self.manager.get_map().epoch, expected_epoch)
        self.assertEqual(self.manager.get_replication_plan(), {
            p.partition_id: tuple(self.manager.get_replication_plan()[p.partition_id])
            for p in self.manager.get_map().partitions
        })
        for sid, proc in self.processes.items():
            if proc.poll() is None:
                info = self.peers[sid].live_replication_snapshot()
                self.assertFalse(info["repair_write_fenced"])
                self.assertFalse(info["migration_fenced"])
                self.assertFalse(info["faulted_partitions"])

    def test_successful_full_range_cutover_and_replica_reseed(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        self.assertTrue(self.push(self.s2, self.b, 2).accepted)
        new = self.migrate(0, THIRD)
        self.assertEqual(new.epoch, 2)
        moved = new.split_keys([self.a])[0]
        self.assertEqual(moved.owner, self.s2)
        self.assert_weights(self.s2, self.a, 1)
        # Completed Push survives cutover; replay must not double apply.
        self.assertTrue(self.push(self.s2, self.a, 1).accepted)
        self.assert_weights(self.s2, self.a, 1)
        self.assertTrue(self.push(self.s2, self.a, 3).accepted)
        self.assert_weights(self.s2, self.a, 2)
        shadow = self.peers[self.s3].get_replica_snapshot(self.s2, moved.partition_id)
        self.assertEqual(shadow["epoch"], 2)
        self.assertEqual(shadow["sequence"], 1)
        self.assertEqual(len(shadow["completed_pushes"]), 2)
        self.assert_repaired(2)

    def test_successful_partial_range_preserves_retained_clocks(self):
        other = 13
        self.assertTrue(self.push(self.s1, other, 1).accepted)
        self.assertTrue(self.push(self.s1, self.a, 2).accepted)
        new = self.migrate(0, 12)
        self.assertEqual(len(new.partitions), 4)
        self.assertEqual(new.split_keys([self.a])[0].owner, self.s2)
        self.assertEqual(new.split_keys([other])[0].owner, self.s1)
        self.assert_weights(self.s1, other, 1)
        self.assert_weights(self.s2, self.a, 1)
        self.assertTrue(self.push(self.s1, other, 3).accepted)
        self.assertTrue(self.push(self.s2, self.a, 4).accepted)
        self.assert_weights(self.s1, other, 2)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_source_killed_after_staging_recovers_from_old_replica(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        phases = []
        def inject(phase):
            phases.append(phase)
            if phase == "staged":
                self.kill(self.s1)
        table = self.migrate(0, THIRD, checkpoint=inject)
        self.assertEqual(phases, ["staged"])
        self.assertEqual(table.epoch, 2)
        self.assertEqual(table.partitions[0].partition_id, "p0")
        self.assertEqual(table.partitions[0].owner, self.s2)
        self.assert_weights(self.s2, self.a, 1)
        self.assertTrue(self.push(self.s2, self.a, 1).accepted)
        self.assert_weights(self.s2, self.a, 1)
        self.assertTrue(self.push(self.s2, self.a, 3).accepted)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_source_killed_after_destination_install_rolls_it_back(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        self.assertTrue(self.push(self.s2, self.b, 2).accepted)
        def inject(phase):
            if phase == "installed":
                self.kill(self.s1)
        table = self.migrate(0, THIRD, checkpoint=inject)
        self.assertEqual(table.epoch, 2)
        # Publication was NOT completed: ownership/ID remain the original
        # p0. The speculative p0_moved_e2 was discarded before promotion.
        self.assertEqual(table.partitions[0].partition_id, "p0")
        self.assertEqual(table.partitions[0].owner, self.s2)
        self.assert_weights(self.s2, self.a, 1)
        self.assert_weights(self.s2, self.b, 1)
        self.assertTrue(self.push(self.s2, self.a, 3).accepted)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_destination_killed_while_staged_source_recovers(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        self.assertTrue(self.push(self.s2, self.b, 2).accepted)
        def inject(phase):
            if phase == "staged":
                self.kill(self.s2)
        table = self.migrate(0, THIRD, checkpoint=inject)
        self.assertEqual(table.epoch, 2)
        self.assertEqual(table.split_keys([self.a])[0].owner, self.s1)
        self.assertEqual(table.split_keys([self.b])[0].owner, self.s3)
        self.assert_weights(self.s1, self.a, 1)
        self.assert_weights(self.s3, self.b, 1)
        self.assertTrue(self.push(self.s1, self.a, 3).accepted)
        self.assertTrue(self.push(self.s3, self.b, 4).accepted)
        self.assert_repaired(2)

    def test_source_killed_after_full_range_publication(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        def inject(phase):
            if phase == "published":
                self.kill(self.s1)
        table = self.migrate(0, THIRD, checkpoint=inject)
        self.assertEqual(table.epoch, 2)
        self.assertEqual(table.split_keys([self.a])[0].owner, self.s2)
        self.assert_weights(self.s2, self.a, 1)
        self.assertTrue(self.push(self.s2, self.a, 2).accepted)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_partial_postpublication_source_crash_stays_fail_closed(self):
        # F3.4 does not yet reconstruct retained subranges if the source
        # disappears AFTER its split-range ownership was published.
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        self.assertTrue(self.push(self.s1, 13, 2).accepted)

        def inject(phase):
            if phase == "published":
                self.kill(self.s1)

        with self.assertRaisesRegex(RuntimeError, "retained ranges"):
            self.migrate(0, 12, checkpoint=inject)
        self.assertEqual(self.manager.get_map().epoch, 2)
        with self.assertRaises(grpc.RpcError):
            self.push(self.s2, self.a, 3)
        with self.assertRaisesRegex(RuntimeError, "retained ranges"):
            self.manager.resume_published_migration(self.peers)
        self.assertEqual(self.manager.get_map().epoch, 2)

    def test_partial_copy_source_crash_recovers_original_whole_range(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        self.assertTrue(self.push(self.s1, 13, 2).accepted)

        def inject(phase):
            if phase == "staged":
                self.kill(self.s1)

        table = self.migrate(0, 12, checkpoint=inject)
        self.assertEqual(table.epoch, 2)
        self.assertEqual(len(table.partitions), 3)
        self.assertEqual(table.split_keys([self.a, 13])[0].owner, self.s2)
        self.assert_weights(self.s2, self.a, 1)
        self.assert_weights(self.s2, 13, 1)
        self.assertTrue(self.push(self.s2, 13, 3).accepted)
        self.assert_weights(self.s2, 13, 2)
        self.assert_repaired(2)

    def test_lost_source_finalization_reply_resumes_same_epoch(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        original = self.peers[self.s1].finish_outgoing_transfer
        calls = 0

        def lost_ack(table):
            nonlocal calls
            calls += 1
            result = original(table)
            if calls == 1:
                raise ConnectionError("injected source-finalize ACK loss")
            return result

        self.peers[self.s1].finish_outgoing_transfer = lost_ack
        with self.assertRaisesRegex(ConnectionError, "ACK loss"):
            self.migrate(0, THIRD)
        self.assertEqual(self.manager.get_map().epoch, 2)
        # The first RPC actually completed on source. A retry must not
        # finalize twice or publish epoch 3.
        self.manager.resume_published_migration(self.peers)
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertTrue(self.push(self.s2, self.a, 2).accepted)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_lost_release_reply_cannot_overwrite_new_acknowledged_push(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        original = self.peers[self.s3].release_repaired_replication
        calls = 0

        def lost_ack(epoch):
            nonlocal calls
            calls += 1
            result = original(epoch)
            if calls == 1:
                raise ConnectionError("injected release ACK loss")
            return result

        self.peers[self.s3].release_repaired_replication = lost_ack
        with self.assertRaisesRegex(ConnectionError, "ACK loss"):
            self.migrate(0, THIRD)
        self.assertEqual(self.manager.get_map().epoch, 2)
        # The server did release before the simulated ACK was lost.
        self.assertTrue(self.push(self.s2, self.a, 2).accepted)
        moved_id = self.manager.get_map().split_keys([self.a])[0].partition_id
        self.manager.resume_published_migration(self.peers)
        shadow = self.peers[self.s3].get_replica_snapshot(self.s2, moved_id)
        self.assertEqual(shadow["sequence"], 1)
        self.assert_weights(self.s2, self.a, 2)
        self.assert_repaired(2)

    def test_lost_postpublication_callback_resumes_without_republication(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)

        def fail_after_publish(phase):
            if phase == "published":
                raise RuntimeError("injected control interruption")

        with self.assertRaisesRegex(RuntimeError, "control interruption"):
            self.migrate(0, THIRD, checkpoint=fail_after_publish)
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.manager.resume_published_migration(self.peers)
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertTrue(self.push(self.s2, self.a, 2).accepted)
        self.assert_weights(self.s2, self.a, 2)

    def test_exception_without_crash_aborts_and_releases_fence(self):
        self.assertTrue(self.push(self.s1, self.a, 1).accepted)
        def inject(phase):
            if phase == "installed":
                raise RuntimeError("test-only interruption before publication")
        with self.assertRaisesRegex(RuntimeError, "test-only"):
            self.migrate(0, THIRD, checkpoint=inject)
        self.assertEqual(self.manager.get_map().epoch, 1)
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.s1)
        self.assertTrue(self.push(self.s1, self.a, 2).accepted)
        self.assert_weights(self.s1, self.a, 2)
        self.assert_repaired(1)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F3.4 fenced migration and crash recovery: PASS")
    else:
        raise SystemExit(1)
