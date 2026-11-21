"""F2.3: acknowledgement-after-replication batching, timer flush, atomic backups.

Run: python test_f23_batched_replication.py
Run again with F1/F2.1/F2.2 suites to check backwards compatibility.
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


def request(key, gradients=(1.0, 2.0), worker=1, sequence=1):
    req = pb.PushRequest(keys=[key], gradients=list(gradients), width=2)
    req.task_id.worker_id = worker
    req.task_id.sequence = sequence
    return req


def make_cluster(batch_size=3, timeout=0.10, k=1):
    third = KEY_SPACE_END // 3
    manager = ServerManager([
        Partition("p0", 0, third, "s1"),
        Partition("p1", third, 2 * third, "s2"),
        Partition("p2", 2 * third, KEY_SPACE_END, "s3"),
    ])
    servers = {sid: ParameterServer() for sid in ("s1", "s2", "s3")}
    table = manager.get_map()
    for sid, server in servers.items():
        server.set_ownership(sid, table)
    manager.configure_replication(["s1", "s2", "s3"], k=k)
    manager.seed_replicas(servers)
    manager.activate_live_replication(
        servers, batch_size=batch_size, batch_timeout_s=timeout,
    )
    return manager, servers


class BatchedReplicationTests(unittest.TestCase):
    def cluster(self, **kwargs):
        manager, servers = make_cluster(**kwargs)
        for server in servers.values():
            self.addCleanup(server.stop_replication_batcher)
        return manager, servers

    def test_batches_three_pushes_in_one_backup_call_before_ack(self):
        _, servers = self.cluster(batch_size=3, timeout=1.0)
        primary, backup = servers["s1"], servers["s2"]
        observed = []
        original = backup.apply_replica_batch

        def inspect(batch):
            observed.append((
                len(batch), [r["sequence"] for r in batch],
                7 not in primary.weights,
                all((1, i) not in primary.completed_pushes for i in (1, 2, 3)),
            ))
            return original(batch)

        with patch.object(backup, "apply_replica_batch", side_effect=inspect):
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [
                    pool.submit(primary.Push, request(7, sequence=i), FakeContext())
                    for i in (1, 2, 3)
                ]
                self.assertTrue(all(f.result(timeout=3).accepted for f in futures))

        self.assertEqual(observed, [(3, [1, 2, 3], True, True)])
        self.assertEqual(primary.weights[7], [-0.30000000000000004, -0.6000000000000001])
        replica = backup.get_replica_snapshot("s1", "p0")
        self.assertEqual(replica["weights"][7], primary.weights[7])
        self.assertEqual(replica["sequence"], 3)
        self.assertEqual(len(replica["completed_pushes"]), 3)
        snapshot = primary.live_replication_snapshot()
        self.assertEqual(snapshot["batch_size"], 3)
        self.assertEqual(snapshot["batch_flushes"], 1)
        self.assertEqual(snapshot["batch_committed_pushes"], 3)

    def test_timeout_flushes_partial_batch(self):
        _, servers = self.cluster(batch_size=5, timeout=0.05)
        primary = servers["s1"]
        start = time.monotonic()
        self.assertTrue(primary.Push(request(3), FakeContext()).accepted)
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.025)
        self.assertLess(elapsed, 1.5)
        snapshot = primary.live_replication_snapshot()
        self.assertEqual(snapshot["batch_flushes"], 1)
        self.assertEqual(snapshot["batch_committed_pushes"], 1)

    def test_same_task_in_one_batch_is_not_double_applied(self):
        _, servers = self.cluster(batch_size=3, timeout=0.20)
        primary = servers["s1"]
        duplicate = request(9, worker=8, sequence=10)
        with ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(primary.Push, duplicate, FakeContext())
            second = pool.submit(primary.Push, duplicate, FakeContext())
            third = pool.submit(
                primary.Push, request(9, worker=8, sequence=11), FakeContext(),
            )
            self.assertTrue(all(f.result(timeout=2).accepted for f in (first, second, third)))
        self.assertEqual(primary.weights[9], [-0.2, -0.4])
        self.assertEqual(servers["s2"].get_replica_snapshot("s1", "p0")["sequence"], 2)
        self.assertTrue(primary.Push(duplicate, FakeContext()).accepted)
        self.assertEqual(primary.weights[9], [-0.2, -0.4])

    def test_mismatched_duplicate_is_rejected_without_poisoning_valid_push(self):
        _, servers = self.cluster(batch_size=3, timeout=0.15)
        primary = servers["s1"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            okay = pool.submit(primary.Push, request(9), FakeContext())
            mismatch = pool.submit(
                primary.Push,
                request(9, gradients=(9.0, 9.0)), FakeContext(),
            )
            another = pool.submit(primary.Push, request(9, sequence=2), FakeContext())
            self.assertTrue(okay.result(timeout=3).accepted)
            with self.assertRaises(AbortError) as err:
                mismatch.result(timeout=3)
            self.assertEqual(err.exception.status_code, grpc.StatusCode.INVALID_ARGUMENT)
            self.assertTrue(another.result(timeout=3).accepted)
        self.assertEqual(primary.weights[9], [-0.2, -0.4])

    def test_batch_is_invisible_to_pull_until_backup_ack(self):
        _, servers = self.cluster(batch_size=2, timeout=0.5)
        primary, backup = servers["s1"], servers["s2"]
        entered = threading.Event()
        release = threading.Event()
        original = backup.apply_replica_batch

        def hold_backup(batch):
            entered.set()
            if not release.wait(timeout=3):
                raise TimeoutError("test backup timed out")
            return original(batch)

        with patch.object(backup, "apply_replica_batch", side_effect=hold_backup):
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(primary.Push, request(2), FakeContext())
                b = pool.submit(primary.Push, request(2, sequence=2), FakeContext())
                try:
                    self.assertTrue(entered.wait(timeout=2))
                    self.assertNotIn(2, primary.weights)
                    self.assertNotIn((1, 1), primary.completed_pushes)
                    self.assertFalse(a.done())
                    self.assertFalse(b.done())
                    self.assertEqual(
                        backup.get_replica_snapshot("s1", "p0")["sequence"], 0,
                    )
                finally:
                    release.set()
                self.assertTrue(a.result(timeout=3).accepted)
                self.assertTrue(b.result(timeout=3).accepted)
        self.assertEqual(primary.weights[2], [-0.2, -0.4])

    def test_rejected_replica_batch_is_atomic(self):
        _, servers = self.cluster(batch_size=2, timeout=0.20)
        primary, backup = servers["s1"], servers["s2"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(primary.Push, request(6), FakeContext())
            b = pool.submit(primary.Push, request(6, sequence=2), FakeContext())
            self.assertTrue(a.result(timeout=3).accepted)
            self.assertTrue(b.result(timeout=3).accepted)
        replica = backup.get_replica_snapshot("s1", "p0")
        before = copy.deepcopy(replica)
        first = copy.deepcopy(replica["last_record"])
        first.update(sequence=3, task_id=(1, 3))
        second = copy.deepcopy(first)
        second.update(sequence=4, task_id=(1, 4), weights={-1: [1.0, 1.0]})
        with self.assertRaises(ValueError):
            backup.apply_replica_batch([first, second])
        self.assertEqual(backup.get_replica_snapshot("s1", "p0"), before)

    def test_backup_failure_fences_primary_and_prevents_ack(self):
        _, servers = self.cluster(batch_size=2, timeout=0.15)
        primary, backup = servers["s1"], servers["s2"]
        with patch.object(backup, "apply_replica_batch", side_effect=OSError("backup down")):
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(primary.Push, request(7), FakeContext())
                b = pool.submit(primary.Push, request(7, sequence=2), FakeContext())
                for result in (a, b):
                    with self.assertRaises(AbortError) as error:
                        result.result(timeout=3)
                    self.assertEqual(error.exception.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertNotIn(7, primary.weights)
        self.assertEqual(backup.get_replica_snapshot("s1", "p0")["sequence"], 0)
        self.assertEqual(primary.live_replication_snapshot()["faulted_partitions"], ("p0",))
        with self.assertRaises(AbortError):
            primary.Push(request(7, sequence=3), FakeContext())

    def test_lost_batch_ack_fences_instead_of_unsafe_retry(self):
        _, servers = self.cluster(batch_size=2, timeout=0.15)
        primary, backup = servers["s1"], servers["s2"]
        original = backup.apply_replica_batch

        def deliver_then_lose_ack(batch):
            original(batch)
            raise ConnectionError("ack lost")

        with patch.object(backup, "apply_replica_batch", side_effect=deliver_then_lose_ack):
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(primary.Push, request(7), FakeContext())
                b = pool.submit(primary.Push, request(7, sequence=2), FakeContext())
                for result in (a, b):
                    with self.assertRaises(AbortError):
                        result.result(timeout=3)
        self.assertNotIn(7, primary.weights)
        replica = backup.get_replica_snapshot("s1", "p0")
        self.assertEqual(replica["sequence"], 2)
        self.assertEqual(primary.live_replication_snapshot()["faulted_partitions"], ("p0",))

    def test_two_required_backups_receive_batch(self):
        _, servers = self.cluster(batch_size=3, timeout=0.15, k=2)
        primary = servers["s1"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(primary.Push, request(13, sequence=i), FakeContext())
                       for i in (1, 2, 3)]
            self.assertTrue(all(f.result(timeout=3).accepted for f in futures))
        for sid in ("s2", "s3"):
            replica = servers[sid].get_replica_snapshot("s1", "p0")
            self.assertEqual(replica["sequence"], 3)
            self.assertEqual(replica["weights"][13], primary.weights[13])

    def test_reciprocal_batched_primaries_do_not_deadlock(self):
        manager = ServerManager([
            Partition("left", 0, KEY_SPACE_END // 2, "s1"),
            Partition("right", KEY_SPACE_END // 2, KEY_SPACE_END, "s2"),
        ])
        servers = {sid: ParameterServer() for sid in ("s1", "s2")}
        for sid, server in servers.items():
            server.set_ownership(sid, manager.get_map())
            self.addCleanup(server.stop_replication_batcher)
        manager.configure_replication(["s1", "s2"])
        manager.seed_replicas(servers)
        manager.activate_live_replication(servers, batch_size=2, batch_timeout_s=0.08)
        right = KEY_SPACE_END // 2 + 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(servers["s1"].Push, request(1, worker=1, sequence=1), FakeContext()),
                pool.submit(servers["s1"].Push, request(1, worker=1, sequence=2), FakeContext()),
                pool.submit(servers["s2"].Push, request(right, worker=2, sequence=1), FakeContext()),
                pool.submit(servers["s2"].Push, request(right, worker=2, sequence=2), FakeContext()),
            ]
            self.assertTrue(all(f.result(timeout=5).accepted for f in futures))
        self.assertEqual(servers["s1"].weights[1], [-0.2, -0.4])
        self.assertEqual(servers["s2"].weights[right], [-0.2, -0.4])

    def test_unsupported_batch_configurations_are_rejected(self):
        for batch_size in (0, -1, True, 2.5, "2"):
            manager, servers = make_unactivated_cluster()
            with self.subTest(batch_size=batch_size):
                with self.assertRaises(ValueError):
                    manager.activate_live_replication(servers, batch_size=batch_size)
        for timeout in (0, -1, float("nan"), float("inf"), True, "0.1"):
            manager, servers = make_unactivated_cluster()
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    manager.activate_live_replication(servers, batch_timeout_s=timeout)

    def test_one_batch_can_cover_two_distinct_primary_partitions(self):
        # Primary s1 owns both left and right, with one shadow backup on s2.
        middle = KEY_SPACE_END // 2
        manager = ServerManager([
            Partition("left", 0, middle, "s1"),
            Partition("right", middle, KEY_SPACE_END, "s1"),
        ])
        manager.register_server("s2")
        servers = {sid: ParameterServer() for sid in ("s1", "s2")}
        for sid, server in servers.items():
            server.set_ownership(sid, manager.get_map())
            self.addCleanup(server.stop_replication_batcher)
        manager.configure_replication(["s1", "s2"])
        manager.seed_replicas(servers)
        manager.activate_live_replication(
            servers, batch_size=2, batch_timeout_s=0.15,
        )
        primary, backup = servers["s1"], servers["s2"]
        observed = []
        original = backup.apply_replica_batch

        def spy(records):
            observed.append([r["partition_id"] for r in records])
            return original(records)

        with patch.object(backup, "apply_replica_batch", side_effect=spy):
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(primary.Push, request(4), FakeContext())
                b = pool.submit(primary.Push, request(middle + 4, sequence=2), FakeContext())
                self.assertTrue(a.result(timeout=3).accepted)
                self.assertTrue(b.result(timeout=3).accepted)
        self.assertEqual(len(observed), 1)
        self.assertEqual(set(observed[0]), {"left", "right"})
        self.assertEqual(backup.get_replica_snapshot("s1", "left")["sequence"], 1)
        self.assertEqual(backup.get_replica_snapshot("s1", "right")["sequence"], 1)

    def test_second_backup_failure_does_not_approve_partial_delivery(self):
        _, servers = self.cluster(batch_size=2, timeout=0.15, k=2)
        primary, first, second = servers["s1"], servers["s2"], servers["s3"]
        with patch.object(second, "apply_replica_batch", side_effect=OSError("offline")):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(primary.Push, request(5, sequence=i), FakeContext())
                    for i in (1, 2)
                ]
                for future in futures:
                    with self.assertRaises(AbortError) as error:
                        future.result(timeout=3)
                    self.assertEqual(error.exception.status_code, grpc.StatusCode.UNAVAILABLE)
        self.assertNotIn(5, primary.weights)
        self.assertEqual(first.get_replica_snapshot("s1", "p0")["sequence"], 2)
        self.assertEqual(second.get_replica_snapshot("s1", "p0")["sequence"], 0)
        self.assertEqual(primary.live_replication_snapshot()["faulted_partitions"], ("p0",))

    def test_orderly_stop_flushes_underfilled_batch(self):
        _, servers = self.cluster(batch_size=5, timeout=2.0)
        primary = servers["s1"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(primary.Push, request(3), FakeContext())
            deadline = time.monotonic() + 2
            while not primary._batch_pending and time.monotonic() < deadline:
                time.sleep(0.002)
            self.assertTrue(primary._batch_pending)
            primary.stop_replication_batcher()
            self.assertTrue(future.result(timeout=3).accepted)
            with self.assertRaises(AbortError) as err:
                primary.Push(request(3, sequence=2), FakeContext())
            self.assertEqual(err.exception.status_code, grpc.StatusCode.UNAVAILABLE)

    def test_real_grpc_concurrent_pushes_are_batched(self):
        if not hasattr(pb_grpc, "ParameterServerStub"):
            self.skipTest("Generated gRPC stubs unavailable")
        _, servers = self.cluster(batch_size=3, timeout=0.2)
        primary, backup = servers["s1"], servers["s2"]
        rpc_server = grpc.server(ThreadPoolExecutor(max_workers=6))
        pb_grpc.add_ParameterServerServicer_to_server(primary, rpc_server)
        port = rpc_server.add_insecure_port("127.0.0.1:0")
        rpc_server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = pb_grpc.ParameterServerStub(channel)
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures = [
                        pool.submit(stub.Push, request(4, sequence=i), timeout=3)
                        for i in (1, 2, 3)
                    ]
                    self.assertTrue(all(f.result(timeout=4).accepted for f in futures))
            replica = backup.get_replica_snapshot("s1", "p0")
            self.assertEqual(replica["sequence"], 3)
            self.assertEqual(primary.live_replication_snapshot()["batch_flushes"], 1)
        finally:
            rpc_server.stop(0).wait()


def make_unactivated_cluster():
    third = KEY_SPACE_END // 3
    manager = ServerManager([
        Partition("p0", 0, third, "s1"),
        Partition("p1", third, 2 * third, "s2"),
        Partition("p2", 2 * third, KEY_SPACE_END, "s3"),
    ])
    servers = {sid: ParameterServer() for sid in ("s1", "s2", "s3")}
    for sid, server in servers.items():
        server.set_ownership(sid, manager.get_map())
    manager.configure_replication(["s1", "s2", "s3"])
    manager.seed_replicas(servers)
    return manager, servers


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F2.3 batched replication: PASS")
    else:
        raise SystemExit(1)
