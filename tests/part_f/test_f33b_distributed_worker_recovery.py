"""F3.3b: ordinary distributed workers recover without changing Push identities.

Run: python test_f33b_distributed_worker_recovery.py
Unit tests run with local compatibility stubs if generated Protobuf is absent.
The process tests require the complete coursework repository and real gRPC stubs.
"""
import importlib.util
import socket
import subprocess
import sys
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import grpc

HAS_REAL_STUBS = all(importlib.util.find_spec(name) is not None for name in (
    "manager_pb2", "manager_pb2_grpc", "parameter_server_pb2",
    "parameter_server_pb2_grpc", "range_clocks",
))

# These light fakes are for source-level unit testing when working outside
# the student's generated-code environment. They are never used on the user's
# computer when the real project bindings are available.
if not HAS_REAL_STUBS:
    class DummyRequest:
        def __init__(self, **kwargs):
            self.data = b""
            self.task_id = SimpleNamespace(worker_id=0, sequence=0)
            self._has_id = False
        def ParseFromString(self, data):
            self.data = bytes(data)
            if self.data.startswith(b"PUSH:"):
                _, worker, seq, _ = self.data.split(b":", 3)
                self.task_id = SimpleNamespace(
                    worker_id=int(worker), sequence=int(seq),
                )
                self._has_id = True
        def HasField(self, name):
            return name == "task_id" and self._has_id
        def SerializeToString(self, deterministic=True):
            return self.data
    pb_stub = types.ModuleType("parameter_server_pb2")
    pb_stub.PushRequest = DummyRequest
    pb_stub.PullRequest = DummyRequest
    rpc_stub = types.ModuleType("parameter_server_pb2_grpc")
    rpc_stub.ParameterServerStub = lambda channel: SimpleNamespace(
        Push="push_stub", Pull="pull_stub",
    )
    sys.modules.setdefault("parameter_server_pb2", pb_stub)
    sys.modules.setdefault("parameter_server_pb2_grpc", rpc_stub)
    compression_stub = types.ModuleType("value_compression")
    compression_stub.compress_doubles = lambda values: b""
    compression_stub.decompress_doubles = lambda blob, count: [0.0] * count
    sys.modules.setdefault("value_compression", compression_stub)
    manager_stub = types.ModuleType("manager_rpc")
    manager_stub.fetch_ownership_map = lambda address: None
    sys.modules.setdefault("manager_rpc", manager_stub)

from distributed_client import DistributedTask, DistributedParameterClient
from parameter_client import ParameterClient, Task
from routing import KEY_SPACE_END, Partition, RoutingTable


class RetryableFailure(grpc.RpcError):
    def __init__(self, code=grpc.StatusCode.UNAVAILABLE):
        self._code = code
    def code(self):
        return self._code
    def details(self):
        return "old primary unavailable"


class BadArgument(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.INVALID_ARGUMENT
    def details(self):
        return "invalid request"


def ready_task(result=None, error=None, task_id=(41, 1), operation="push"):
    task = Task(task_id=task_id)
    if HAS_REAL_STUBS:
        import parameter_server_pb2 as pb
        push = pb.PushRequest(keys=[7], gradients=[1.0, 2.0], width=2)
        push.task_id.worker_id = 41
        push.task_id.sequence = 1
        task._push_request_bytes = push.SerializeToString(deterministic=True)
        pull = pb.PullRequest(keys=[7])
        task._pull_request_bytes = pull.SerializeToString(deterministic=True)
    else:
        task._push_request_bytes = b"PUSH:41:1:fixed-original-bytes"
        task._pull_request_bytes = b"PULL:fixed-original-bytes"
    if error is not None:
        task._future.set_exception(error)
    else:
        task._future.set_result(result or SimpleNamespace(
            accepted=True, clocks=[], values=[-0.1, -0.2],
        ))
    return task


class FakePhysical:
    def __init__(self, address, failures=0):
        self.address = address
        self.failures = failures
        self.calls = []
    def replay_physical_request(self, original, operation):
        self.calls.append((original.task_id, original._push_request_bytes
                           if operation == "push" else original._pull_request_bytes))
        if self.failures:
            self.failures -= 1
            return ready_task(error=RetryableFailure(),
                              task_id=original.task_id, operation=operation)
        return ready_task(task_id=original.task_id, operation=operation)


class FakeDistributed:
    _failover_retries = True
    _failover_timeout_s = 1.0
    _failover_retry_interval_s = .002
    _failover_attempt_wait_ms = 100
    manager_address = "mock-manager"
    def __init__(self, maps, failures=0):
        self.maps = list(maps)
        self.table = self.maps[0]
        self.clients = {"s1": FakePhysical("s1", failures=100000),
                        "s2": FakePhysical("s2", failures=failures)}
        self.refresh_count = 0
    def refresh_map(self):
        self.refresh_count += 1
        self.table = self.maps[min(self.refresh_count, len(self.maps)-1)]
        return True
    def _subtarget(self, route):
        return list(route.keys) if hasattr(route, "keys") else range(route.begin, route.end)
    def _routes(self, target):
        return (self.table.split_range(target.start, target.stop)
                if isinstance(target, range) else self.table.split_keys(target))


class DistributedRetryUnitTests(unittest.TestCase):
    @staticmethod
    def table(owner="s1", epoch=1):
        return RoutingTable([Partition("p0", 0, KEY_SPACE_END, owner)], epoch=epoch)

    def make_task(self, manager, error=None, operation="push"):
        old = self.table()
        route = old.split_keys([7])[0]
        original = ready_task(error=error or RetryableFailure())
        logical = DistributedTask(
            operation, [7], [route], [(route, original)], 2,
            client=manager,
            pinned_partitions={route: ("p0", 0, KEY_SPACE_END)},
        )
        return logical, original

    def test_retry_same_task_and_serialized_bytes_after_new_owner(self):
        c = FakeDistributed([self.table(), self.table("s2", 2)])
        task, original = self.make_task(c)
        self.assertTrue(task.wait(timeout_ms=1500).accepted)
        self.assertEqual(c.clients["s2"].calls, [
            ((41, 1), original._push_request_bytes)
        ])
        self.assertEqual(len(task.tasks), 2)
        self.assertEqual(original.task_id, (41, 1))

    def test_waits_for_new_map_and_recovers_after_transient_retries(self):
        c = FakeDistributed([
            self.table(), self.table(), self.table("s2", 2),
        ], failures=2)
        task, _ = self.make_task(c)
        self.assertTrue(task.wait(timeout_ms=1500).accepted)
        self.assertGreaterEqual(c.refresh_count, 4)
        self.assertEqual(len(c.clients["s2"].calls), 3)

    def test_pull_recovers_and_preserves_value_order(self):
        c = FakeDistributed([self.table(), self.table("s2", 2)])
        task, _ = self.make_task(c, operation="pull")
        reply = task.wait(timeout_ms=1500)
        self.assertEqual(reply.values, [-0.1, -0.2])
        self.assertEqual(len(c.clients["s2"].calls), 1)

    def test_repartitioned_uncertain_push_is_rejected_without_replay(self):
        new = RoutingTable([
            Partition("p0_left", 0, KEY_SPACE_END//2, "s2"),
            Partition("p0_right", KEY_SPACE_END//2, KEY_SPACE_END, "s2"),
        ], epoch=2)
        c = FakeDistributed([self.table(), new])
        task, _ = self.make_task(c)
        with self.assertRaisesRegex(RuntimeError, "Unsafe failover replay"):
            task.wait(timeout_ms=1500)
        self.assertEqual(c.clients["s2"].calls, [])

    def test_permanent_error_is_not_retried(self):
        c = FakeDistributed([self.table(), self.table("s2", 2)])
        task, _ = self.make_task(c, error=BadArgument())
        with self.assertRaises(BadArgument):
            task.wait(timeout_ms=1500)
        self.assertEqual(c.clients["s2"].calls, [])

    def test_deadline_exhaustion_preserves_uncertainty(self):
        c = FakeDistributed([self.table()], failures=10000)
        c._failover_timeout_s = .025
        task, _ = self.make_task(c)
        with self.assertRaisesRegex(TimeoutError, "outcome remains uncertain"):
            task.wait(timeout_ms=1500)

    def test_parameter_client_replay_uses_original_task_and_d1_metadata(self):
        # Patch only scheduling: check the exact API inputs without an RPC.
        client = object.__new__(ParameterClient)
        client.stub = SimpleNamespace(Push="PUSH_STUB", Pull="PULL_STUB")
        old = ready_task(error=RetryableFailure())
        old._cache_id = b"x" * 16
        old._cache_keys = (7, 9)
        captured = []
        def schedule(method, req, deps, task_id, cache_id, cache_keys):
            captured.append((method, req.SerializeToString(deterministic=True),
                             deps, task_id, cache_id, cache_keys))
            return ready_task(task_id=task_id)
        with patch.object(client, "_schedule_with_cache", schedule):
            next_task = client.replay_physical_request(old, "push")
        self.assertEqual(captured, [
            ("PUSH_STUB", old._push_request_bytes, None, (41, 1),
             b"x" * 16, (7, 9)),
        ])
        self.assertEqual(next_task._push_request_bytes, old._push_request_bytes)

    def test_replay_rejects_unstable_or_missing_identity(self):
        client = object.__new__(ParameterClient)
        client.stub = SimpleNamespace(Push="PUSH_STUB")
        old = ready_task(error=RetryableFailure())
        old.task_id = (41, 999)
        with self.assertRaisesRegex(ValueError, "does not match"):
            client.replay_physical_request(old, "push")
        old.task_id = None
        with self.assertRaisesRegex(ValueError, "Task ID"):
            client.replay_physical_request(old, "push")

    def test_task_wait_preserves_completed_timeout_error(self):
        from parameter_client import TaskWaitTimeout
        pending = Task()
        with self.assertRaises(TaskWaitTimeout):
            pending.wait(timeout_ms=1)
        finished = Task()
        finished._future.set_exception(TimeoutError("recovery outcome uncertain"))
        with self.assertRaisesRegex(TimeoutError, "recovery outcome uncertain"):
            finished.wait(timeout_ms=100)

    def test_successful_shards_are_not_repeated(self):
        third = KEY_SPACE_END // 2
        old = RoutingTable([
            Partition("p0", 0, third, "s1"),
            Partition("p1", third, KEY_SPACE_END, "s2"),
        ], epoch=1)
        new = RoutingTable([
            Partition("p0", 0, third, "s3"),
            Partition("p1", third, KEY_SPACE_END, "s2"),
        ], epoch=2)
        c = FakeDistributed([old, new])
        c.clients["s3"] = FakePhysical("s3")
        r0, r1 = old.split_keys([7, third + 7])
        t0 = ready_task(error=RetryableFailure(), task_id=(41, 1))
        t1 = ready_task(task_id=(41, 2))
        logical = DistributedTask(
            "push", [7, third + 7], [r0, r1],
            [(r0, t0), (r1, t1)], 2, client=c,
            pinned_partitions={
                r0: ("p0", 0, third),
                r1: ("p1", third, KEY_SPACE_END),
            },
        )
        self.assertTrue(logical.wait(timeout_ms=1500).accepted)
        self.assertEqual(c.clients["s3"].calls[0][0], (41, 1))
        self.assertEqual(c.clients["s2"].calls, [])

    def test_invalid_recovery_configuration_is_rejected(self):
        table = self.table()
        with self.assertRaises(ValueError):
            DistributedParameterClient(table, None, failover_retries=True)
        with self.assertRaises(ValueError):
            DistributedParameterClient(table, 11, failover_timeout_s=0)
        with self.assertRaises(TypeError):
            DistributedParameterClient(table, 11, failover_retries="yes")
        with self.assertRaises(ValueError):
            ParameterClient("s1", rpc_timeout_s=0)


@unittest.skipUnless(HAS_REAL_STUBS,
                     "Requires real manager/parameter Protobuf and RangeClocks")
class DistributedWorkerProcessTests(unittest.TestCase):
    @staticmethod
    def free_addr():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return f"127.0.0.1:{sock.getsockname()[1]}"

    @staticmethod
    def wait_until(predicate, timeout_s=8):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(.025)
        return bool(predicate())

    def setUp(self):
        import manager_pb2_grpc as mgrpc
        from manager_rpc import ManagerService
        from server_manager import ServerManager
        from replica_transport import ReplicaPeer
        from failover_controller import AutomaticFailoverController

        self.addresses = [self.free_addr() for _ in range(3)]
        self.s1, self.s2, self.s3 = self.addresses
        third = KEY_SPACE_END // 3
        self.keys = [7, third + 7, third * 2 + 7]
        self.manager = ServerManager([
            Partition("p0", 0, third, self.s1),
            Partition("p1", third, 2*third, self.s2),
            Partition("p2", 2*third, KEY_SPACE_END, self.s3),
        ], heartbeat_timeout_s=.45)
        self.manager_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        mgrpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.manager_server,
        )
        port = self.manager_server.add_insecure_port("127.0.0.1:0")
        self.manager_address = f"127.0.0.1:{port}"
        self.manager_server.start()
        self.manager.start_heartbeat_monitor(interval_s=.025)
        self.processes = {}
        self.peers = {}
        server_file = Path(__file__).resolve().parents[1] / "server.py"
        for address in self.addresses:
            self.processes[address] = subprocess.Popen([
                sys.executable, "-u", str(server_file),
                "--listen", address, "--manager", self.manager_address,
                "--bootstrap-ownership", "--heartbeat-interval", ".05",
                "--heartbeat-rpc-timeout", ".2",
            ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True)
            self.peers[address] = ReplicaPeer(address, timeout_s=2.0)
        for address in self.addresses:
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=8)
        self.assertTrue(self.wait_until(lambda: all(
            entry["state"] == "ALIVE"
            for entry in self.manager.heartbeat_snapshot().values()
        )))
        self.manager.configure_replication(self.addresses, k=1)
        self.manager.seed_replicas(self.peers)
        self.manager.activate_live_replication(self.peers)
        self.controller = AutomaticFailoverController(
            self.manager, self.peers,
            lambda sid: self.processes[sid].poll() is not None,
            scan_interval_s=.025, retry_interval_s=.05,
        )
        self.worker = DistributedParameterClient.from_manager(
            self.manager_address, worker_id=83,
            failover_retries=True, failover_timeout_s=9.0,
            failover_attempt_timeout_s=.4,
            failover_retry_interval_s=.025,
        )

    def tearDown(self):
        if hasattr(self, "worker"):
            self.worker.close()
        if hasattr(self, "controller"):
            self.controller.stop()
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

    def kill_s1(self):
        self.controller.start()
        self.processes[self.s1].kill()
        self.processes[self.s1].wait(timeout=3)

    def test_worker_push_pull_and_partial_shards_survive_failure(self):
        key0, key1 = self.keys[:2]
        first = self.worker.push([key0, key1], [1., 2., 1., 2.], width=2)
        self.assertTrue(first.wait(timeout_ms=5000).accepted)
        self.kill_s1()
        prior = self.worker._push_sequence
        pending = self.worker.push([key0, key1], [1., 2., 1., 2.], width=2)
        self.assertTrue(pending.wait(timeout_ms=10000).accepted)
        # Two physical subtasks were allocated, but failover replay must not
        # allocate a third Task ID for the failed p0 shard.
        self.assertEqual(self.worker._push_sequence, prior+2)
        values = self.worker.pull([key0, key1]).wait(timeout_ms=10000).values
        self.assertEqual(values, [-.2, -.4, -.2, -.4])
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.s2)

    def test_cached_sparse_push_replayed_after_failover(self):
        self.worker.close()
        self.worker = DistributedParameterClient.from_manager(
            self.manager_address, worker_id=84, key_caching=True,
            value_compression=True, failover_retries=True,
            failover_timeout_s=9., failover_attempt_timeout_s=.4,
            failover_retry_interval_s=.025,
        )
        key = self.keys[0]
        self.assertTrue(self.worker.push([key], [1.,2.], width=2)
                        .wait(timeout_ms=5000).accepted)
        second = self.worker.push([key], [1.,2.], width=2)
        self.assertTrue(second.wait(timeout_ms=5000).accepted)
        original_task = second.tasks[0][1]
        self.assertTrue(original_task._cache_id)
        self.assertTrue(original_task._cache_keys)
        self.kill_s1()
        prior = self.worker._push_sequence
        third = self.worker.push([key], [1.,2.], width=2)
        self.assertTrue(third.wait(timeout_ms=10000).accepted)
        self.assertEqual(self.worker._push_sequence, prior+1)
        values = self.worker.pull([key]).wait(timeout_ms=10000).values
        self.assertAlmostEqual(values[0], -.3, places=10)
        self.assertAlmostEqual(values[1], -.6, places=10)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F3.3b integrated distributed worker recovery: PASS")
    else:
        raise SystemExit(1)
