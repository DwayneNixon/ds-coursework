"""F2.2 — ordered live replication with acknowledgement-after-backup.

Run from the project root:
    python test_f22_live_replication.py

Uses actual server.py, RangeClocks and generated parameter_server_pb2.
F2.2 intentionally uses in-process backup objects, like Part E migration.
"""

import copy
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import grpc
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from routing import KEY_SPACE_END, Partition
from server import ParameterServer
from server_manager import ServerManager


class AbortError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.status_code = code


class FakeContext:
    def __init__(self):
        self._code = None
        self._details = ""

    def abort(self, code, details):
        self._code = code
        self._details = details
        raise AbortError(code, details)

    def code(self):
        return self._code

    def details(self):
        return self._details


def request(keys, gradients, worker=1, sequence=1, task=True):
    req = pb.PushRequest(keys=keys, gradients=gradients, width=2)
    if task:
        req.task_id.worker_id = worker
        req.task_id.sequence = sequence
    return req


def make_cluster(k=1):
    third = KEY_SPACE_END // 3
    manager = ServerManager([
        Partition("p0", 0, third, "s1"),
        Partition("p1", third, 2 * third, "s2"),
        Partition("p2", 2 * third, KEY_SPACE_END, "s3"),
    ])
    table = manager.get_map()
    servers = {sid: ParameterServer() for sid in ("s1", "s2", "s3")}
    for sid, server in servers.items():
        server.set_ownership(sid, table)
    manager.configure_replication(["s1", "s2", "s3"], k=k)
    manager.seed_replicas(servers)
    manager.activate_live_replication(servers)
    return manager, servers


class LiveReplicationTests(unittest.TestCase):
    def test_backup_has_complete_record_before_primary_commit(self):
        _, servers = make_cluster()
        primary, backup = servers["s1"], servers["s2"]
        original = backup.apply_replica_update
        saw_precommit = []

        def inspect_then_apply(record):
            saw_precommit.append((
                7 not in primary.weights,
                (1, 1) not in primary.completed_pushes,
                record["sequence"] == 1,
            ))
            return original(record)

        with patch.object(backup, "apply_replica_update", side_effect=inspect_then_apply):
            reply = primary.Push(request([7], [2.0, -3.0]), FakeContext())

        self.assertTrue(reply.accepted)
        self.assertEqual(saw_precommit, [(True, True, True)])
        self.assertEqual(primary.weights[7], [-0.2, 0.30000000000000004])
        replica = backup.get_replica_snapshot("s1", "p0")
        self.assertEqual(replica["weights"][7], primary.weights[7])
        self.assertEqual(replica["sequence"], 1)
        self.assertEqual(
            replica["clock_segments"],
            primary.range_clocks["p0"].snapshot(),
        )
        self.assertEqual(
            replica["completed_pushes"][(1, 1)],
            (primary.completed_pushes[(1, 1)], (7, 8)),
        )
        self.assertNotIn(7, backup.weights)
        self.assertNotIn("p0", backup.range_clocks)

    def test_two_pushes_ordered_and_exact_retry_is_deduplicated(self):
        _, servers = make_cluster()
        primary, backup = servers["s1"], servers["s2"]
        first = request([5], [1.0, 2.0], sequence=10)
        second = request([5], [3.0, 4.0], sequence=11)
        primary.Push(first, FakeContext())
        primary.Push(second, FakeContext())
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 2)
        self.assertTrue(primary.Push(first, FakeContext()).accepted)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 2)
        self.assertEqual(primary.weights[5], [-0.4, -0.6000000000000001])
        with self.assertRaises(AbortError) as mismatch:
            primary.Push(request([5], [6.0, 7.0], sequence=10), FakeContext())
        self.assertEqual(mismatch.exception.status_code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 2)

    def test_missing_task_id_is_rejected_while_replication_enabled(self):
        _, servers = make_cluster()
        with self.assertRaises(AbortError) as missing:
            servers["s1"].Push(
                request([5], [1.0, 1.0], task=False), FakeContext(),
            )
        self.assertEqual(missing.exception.status_code, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertNotIn(5, servers["s1"].weights)
        self.assertEqual(
            servers["s2"].get_replica_snapshot("s1", "p0")["sequence"], 0,
        )

    def test_backup_failure_prevents_ack_and_fences_partition(self):
        _, servers = make_cluster()
        primary, backup = servers["s1"], servers["s2"]
        with patch.object(backup, "apply_replica_update", side_effect=OSError("backup offline")):
            with self.assertRaises(AbortError) as error:
                primary.Push(request([7], [1.0, 2.0]), FakeContext())
        self.assertEqual(error.exception.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertNotIn(7, primary.weights)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 0)
        self.assertEqual(primary.live_replication_snapshot()["faulted_partitions"], ("p0",))

        with self.assertRaises(AbortError) as subsequent:
            primary.Push(request([7], [1.0, 2.0], sequence=2), FakeContext())
        self.assertEqual(subsequent.exception.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 0)

    def test_uncertain_delivery_cannot_be_silently_retried(self):
        _, servers = make_cluster()
        primary, backup = servers["s1"], servers["s2"]
        original = backup.apply_replica_update

        def apply_then_lose_ack(record):
            original(record)
            raise ConnectionError("acknowledgement lost")

        with patch.object(backup, "apply_replica_update", side_effect=apply_then_lose_ack):
            with self.assertRaises(AbortError) as error:
                primary.Push(request([9], [1.0, 2.0]), FakeContext())
        self.assertEqual(error.exception.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertNotIn(9, primary.weights)
        replica = backup.get_replica_snapshot("s1", "p0")
        self.assertEqual(replica["sequence"], 1)
        self.assertIn((1, 1), replica["completed_pushes"])
        self.assertEqual(primary.live_replication_snapshot()["faulted_partitions"], ("p0",))
        with self.assertRaises(AbortError):
            primary.Push(request([9], [1.0, 2.0]), FakeContext())

    def test_replica_replay_order_conflict_and_epoch_fencing(self):
        _, servers = make_cluster()
        servers["s1"].Push(request([1], [1.0, 1.0]), FakeContext())
        backup = servers["s2"]
        record = backup.get_replica_snapshot("s1", "p0")["last_record"]
        self.assertFalse(backup.apply_replica_update(copy.deepcopy(record)))
        for change in (
            {"sequence": 3},
            {"epoch": 99},
            {"weights": {1: [99.0, 99.0]}},
            {"weights": {-1: [0.0, 0.0]}},
        ):
            with self.subTest(change=change):
                bad = copy.deepcopy(record)
                bad.update(change)
                with self.assertRaises(ValueError):
                    backup.apply_replica_update(bad)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 1)

    def test_real_grpc_push_replicates_before_ack(self):
        """Exercise the existing worker-facing RPC, not just the servicer."""
        if not hasattr(pb_grpc, "ParameterServerStub"):
            self.skipTest("Generated gRPC stubs unavailable in test sandbox")

        _, servers = make_cluster()
        primary, backup = servers["s1"], servers["s2"]
        rpc_server = grpc.server(ThreadPoolExecutor(max_workers=4))
        pb_grpc.add_ParameterServerServicer_to_server(primary, rpc_server)
        port = rpc_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        rpc_server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = pb_grpc.ParameterServerStub(channel)
                operation = request([7], [1.0, 2.0], worker=55, sequence=1)
                self.assertTrue(stub.Push(operation, timeout=5.0).accepted)
                self.assertTrue(stub.Push(operation, timeout=5.0).accepted)
            replica = backup.get_replica_snapshot("s1", "p0")
            self.assertEqual(replica["weights"][7], primary.weights[7])
            self.assertEqual(replica["sequence"], 1)
            self.assertIn((55, 1), replica["completed_pushes"])
        finally:
            rpc_server.stop(grace=0).wait()

    def test_two_backups_both_receive_each_accepted_push(self):
        _, servers = make_cluster(k=2)
        primary = servers["s1"]
        for seq in (1, 2, 3):
            primary.Push(request([10], [1.0, 1.0], sequence=seq), FakeContext())
        for backup in (servers["s2"], servers["s3"]):
            replica = backup.get_replica_snapshot("s1", "p0")
            self.assertEqual(replica["sequence"], 3)
            self.assertEqual(replica["weights"][10], primary.weights[10])
            self.assertEqual(len(replica["completed_pushes"]), 3)

    def test_mutual_backup_concurrent_pushes_do_not_deadlock(self):
        manager = ServerManager([
            Partition("left", 0, KEY_SPACE_END // 2, "s1"),
            Partition("right", KEY_SPACE_END // 2, KEY_SPACE_END, "s2"),
        ])
        servers = {sid: ParameterServer() for sid in ("s1", "s2")}
        table = manager.get_map()
        for sid, server in servers.items():
            server.set_ownership(sid, table)
        manager.configure_replication(["s1", "s2"], k=1)
        manager.seed_replicas(servers)
        manager.activate_live_replication(servers)
        right_key = KEY_SPACE_END // 2 + 4

        def push_one(primary, key, worker):
            for seq in range(1, 31):
                primary.Push(
                    request([key], [1.0, 1.0], worker=worker, sequence=seq),
                    FakeContext(),
                )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(push_one, servers["s1"], 4, 1),
                pool.submit(push_one, servers["s2"], right_key, 2),
            ]
            for future in futures:
                future.result(timeout=8.0)

        self.assertEqual(servers["s1"].weights[4], [-3.0000000000000013] * 2)
        self.assertEqual(
            servers["s2"].get_replica_snapshot("s1", "left")["sequence"], 30,
        )
        self.assertEqual(
            servers["s1"].get_replica_snapshot("s2", "right")["sequence"], 30,
        )

    def test_cross_partition_physical_push_is_rejected(self):
        manager = ServerManager([
            Partition("left", 0, KEY_SPACE_END // 2, "s1"),
            Partition("right", KEY_SPACE_END // 2, KEY_SPACE_END, "s1"),
        ])
        manager.register_server("s2")
        servers = {sid: ParameterServer() for sid in ("s1", "s2")}
        for sid, server in servers.items():
            server.set_ownership(sid, manager.get_map())
        manager.configure_replication(["s1", "s2"])
        manager.seed_replicas(servers)
        manager.activate_live_replication(servers)
        with self.assertRaises(AbortError) as error:
            servers["s1"].Push(
                request([1, KEY_SPACE_END // 2], [1.0, 1.0, 1.0, 1.0]),
                FakeContext(),
            )
        self.assertEqual(error.exception.status_code, grpc.StatusCode.FAILED_PRECONDITION)
        self.assertFalse(servers["s1"].weights)

    def test_legacy_unreplicated_push_remains_unchanged(self):
        primary = ParameterServer()
        reply = primary.Push(request([3], [1.0, 2.0], task=False), FakeContext())
        self.assertTrue(reply.accepted)
        self.assertEqual(primary.weights[3], [-0.1, -0.2])
        self.assertFalse(primary.live_replication_snapshot()["enabled"])

    def test_live_replication_rejects_migration_until_coordinated(self):
        _, servers = make_cluster()
        with self.assertRaisesRegex(RuntimeError, "F3 coordination"):
            servers["s1"].begin_range_transfer(0, 100)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F2.2 live replication: PASS")
    else:
        raise SystemExit(1)
