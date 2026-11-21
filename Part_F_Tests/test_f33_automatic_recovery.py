"""F3.3a: automatic confirmed-crash recovery + byte-identical physical Push retry.

Run: python test_f33_automatic_recovery.py

Requires current F3.2 files and generated manager/parameter-server protobuf
stubs for the two independent-process integration tests. No README needed.
"""

import importlib.util
import copy
import types
from unittest.mock import patch
import socket
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from failover_controller import AutomaticFailoverController
from failover_retry import FailoverPushClient


def wait_until(predicate, timeout_s=8.0):
    limit = time.monotonic() + timeout_s
    while time.monotonic() < limit:
        if predicate():
            return True
        time.sleep(0.025)
    return bool(predicate())


class FakeManager:
    def __init__(self):
        self.owner = "s1"
        self.epoch = 1
        self.healthy = "SUSPECTED"
        self.promote_attempts = 0
        self.published_count = 0
        self.repair_attempts = 0
        self.fail_promotion_reply = False
        self.fail_repair_once = False
        self.repaired = False
        self.suspected_at_s = time.monotonic()

    def heartbeat_snapshot(self):
        return {"s1": {"state": self.healthy,
                       "suspected_at_s": self.suspected_at_s}}

    def get_map(self):
        return SimpleNamespace(epoch=self.epoch,
                               partitions=[SimpleNamespace(owner=self.owner)])

    def promote_confirmed_crash(self, server_id, servers, *, confirmed_dead):
        assert confirmed_dead and server_id == "s1"
        self.promote_attempts += 1
        if self.epoch == 1:
            self.epoch = 2
            self.owner = "s2"
            self.healthy = "FAILED"
            self.published_count += 1
        if self.fail_promotion_reply and self.promote_attempts == 1:
            raise ConnectionError("final promotion reply lost")
        return self.get_map()

    def restore_replication_after_failover(self, servers):
        self.repair_attempts += 1
        if self.fail_repair_once and self.repair_attempts == 1:
            raise ConnectionError("replica seed failed")
        self.repaired = True
        return {"p0": ("s3",)}


class AutoRecoveryUnitTests(unittest.TestCase):
    def make_controller(self, manager, is_dead=lambda _: True):
        return AutomaticFailoverController(
            manager, {"s2": object(), "s3": object()}, is_dead,
            scan_interval_s=0.005, retry_interval_s=0.015,
        )

    def test_suspicion_alone_never_promotes(self):
        manager = FakeManager()
        controller = self.make_controller(manager, lambda _: False)
        controller.start()
        try:
            self.assertFalse(wait_until(
                lambda: manager.promote_attempts > 0, timeout_s=0.2))
            self.assertEqual(manager.epoch, 1)
            self.assertFalse(controller.snapshot()["recoveries"])
        finally:
            controller.stop()

    def test_confirmed_crash_promotes_and_repairs_automatically(self):
        manager = FakeManager()
        controller = self.make_controller(manager)
        controller.start()
        try:
            self.assertTrue(wait_until(lambda:
                len(controller.snapshot()["recoveries"]) == 1))
            snap = controller.snapshot()
            self.assertEqual(manager.epoch, 2)
            self.assertEqual(manager.published_count, 1)
            self.assertTrue(manager.repaired)
            self.assertEqual(snap["handled_servers"], ("s1",))
            self.assertIsNone(snap["last_error"])
            self.assertGreaterEqual(snap["recoveries"][0]["ready_at_s"],
                                    snap["recoveries"][0]["promoted_at_s"])
            self.assertGreaterEqual(snap["recoveries"][0]["promoted_at_s"],
                                    snap["recoveries"][0]["detected_at_s"])
            snap["recoveries"][0]["server_id"] = "corrupt"
            self.assertEqual(controller.snapshot()["recoveries"][0]["server_id"], "s1")
        finally:
            controller.stop()
        self.assertFalse(controller.snapshot()["running"])

    def test_lost_promotion_reply_retries_same_epoch(self):
        manager = FakeManager()
        manager.fail_promotion_reply = True
        controller = self.make_controller(manager)
        controller.start()
        try:
            self.assertTrue(wait_until(lambda:
                len(controller.snapshot()["recoveries"]) == 1))
            self.assertEqual(manager.promote_attempts, 2)
            self.assertEqual(manager.published_count, 1)
            self.assertEqual(manager.epoch, 2)
            self.assertEqual(manager.repair_attempts, 1)
        finally:
            controller.stop()

    def test_failed_repair_retries_without_repromotion(self):
        manager = FakeManager()
        manager.fail_repair_once = True
        controller = self.make_controller(manager)
        controller.start()
        try:
            self.assertTrue(wait_until(lambda:
                len(controller.snapshot()["recoveries"]) == 1))
            self.assertEqual(manager.promote_attempts, 1)
            self.assertEqual(manager.published_count, 1)
            self.assertEqual(manager.repair_attempts, 2)
        finally:
            controller.stop()

    def test_controller_configuration_and_lifecycle(self):
        for bad in (0, -1, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                AutomaticFailoverController(FakeManager(), {"s2": object()},
                                            lambda _: True, scan_interval_s=bad)
        with self.assertRaises(TypeError):
            AutomaticFailoverController(FakeManager(), {"s2": object()}, None)
        ctrl = self.make_controller(FakeManager(), lambda _: False)
        ctrl.stop()
        ctrl.start()
        with self.assertRaises(RuntimeError):
            ctrl.start()
        ctrl.stop()
        ctrl.stop()

    def test_retry_helper_configuration_and_one_partition_constraint(self):
        from routing import KEY_SPACE_END, Partition, RoutingTable
        table = RoutingTable([
            Partition("p0", 0, KEY_SPACE_END // 2, "s1"),
            Partition("p1", KEY_SPACE_END // 2, KEY_SPACE_END, "s2"),
        ])

        class Request:
            use_cached_keys = False
            key_cache_id = b""
            key_range = None
            def __init__(self, keys):
                self.keys = keys
            def HasField(self, name):
                return False

        self.assertEqual(FailoverPushClient._target(table, Request([7])).owner, "s1")
        with self.assertRaises(ValueError):
            FailoverPushClient._target(table, Request([7, KEY_SPACE_END // 2 + 7]))
        cached = Request([7])
        cached.use_cached_keys = True
        with self.assertRaises(ValueError):
            FailoverPushClient._target(table, cached)
        with self.assertRaises(ValueError):
            FailoverPushClient("", overall_timeout_s=2)
        with self.assertRaises(ValueError):
            FailoverPushClient("manager", retry_interval_s=0)


    def test_physical_push_replay_preserves_id_and_bytes_after_owner_change(self):
        import grpc
        from routing import KEY_SPACE_END, Partition, RoutingTable

        old = RoutingTable([Partition("p0", 0, KEY_SPACE_END, "s1")], epoch=1)
        new = RoutingTable([Partition("p0", 0, KEY_SPACE_END, "s2")], epoch=2)
        lookups = iter((old, new))
        calls = []
        retries = []

        class Request:
            def __init__(self):
                self.keys = [7]
                self.key_cache_id = b""
                self.use_cached_keys = False
                self.task_id = SimpleNamespace(worker_id=19, sequence=42)
                self.payload = b"fixed-original-request"
            def HasField(self, field):
                return field == "task_id"
            def CopyFrom(self, other):
                self.__dict__ = copy.deepcopy(other.__dict__)

        class LostReply(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE
            def details(self):
                return "primary crashed before ACK"

        class Channel:
            def __init__(self, address):
                self.address = address
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass

        class Stub:
            def __init__(self, channel):
                self.address = channel.address
            def Push(self, request, timeout):
                calls.append((self.address, request.task_id.worker_id,
                              request.task_id.sequence, request.payload))
                if self.address == "s1":
                    raise LostReply()
                return SimpleNamespace(accepted=True)

        grpc_module = types.ModuleType("parameter_server_pb2_grpc")
        grpc_module.ParameterServerStub = Stub
        manager_module = types.ModuleType("manager_rpc")
        manager_module.fetch_ownership_map = lambda _: next(lookups)

        client = FailoverPushClient("manager", attempt_timeout_s=0.1,
                                    retry_interval_s=0.001,
                                    on_retry=lambda *args: retries.append(args))
        original = Request()
        with patch.dict(sys.modules, {
            "parameter_server_pb2_grpc": grpc_module,
            "manager_rpc": manager_module,
        }), patch.object(grpc, "insecure_channel", Channel):
            self.assertTrue(client.push(original).accepted)

        self.assertEqual(calls, [
            ("s1", 19, 42, b"fixed-original-request"),
            ("s2", 19, 42, b"fixed-original-request"),
        ])
        self.assertEqual(len(retries), 1)
        self.assertEqual(original.task_id.sequence, 42)

    def test_physical_retry_rejects_partition_boundary_change(self):
        import grpc
        from routing import KEY_SPACE_END, Partition, RoutingTable
        old = RoutingTable([Partition("p0", 0, KEY_SPACE_END, "s1")], epoch=1)
        changed = RoutingTable([
            Partition("p0", 0, KEY_SPACE_END // 2, "s2"),
            Partition("p1", KEY_SPACE_END // 2, KEY_SPACE_END, "s3"),
        ], epoch=2)
        lookups = iter((old, changed))
        class Request:
            def __init__(self):
                self.keys = [7]
                self.key_cache_id = b""
                self.use_cached_keys = False
                self.task_id = SimpleNamespace(worker_id=19, sequence=42)
            def HasField(self, name):
                return name == "task_id"
            def CopyFrom(self, other):
                self.__dict__ = copy.deepcopy(other.__dict__)
        class LostReply(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE
            def details(self):
                return "unavailable"
        class Channel:
            def __init__(self, address):
                self.address = address
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
        class Stub:
            def __init__(self, channel):
                pass
            def Push(self, request, timeout):
                raise LostReply()
        rpc_stub = types.ModuleType("parameter_server_pb2_grpc")
        rpc_stub.ParameterServerStub = Stub
        map_mod = types.ModuleType("manager_rpc")
        map_mod.fetch_ownership_map = lambda _: next(lookups)
        client = FailoverPushClient("manager", retry_interval_s=0.001)
        with patch.dict(sys.modules, {
            "parameter_server_pb2_grpc": rpc_stub, "manager_rpc": map_mod,
        }), patch.object(grpc, "insecure_channel", Channel):
            with self.assertRaisesRegex(RuntimeError, "Partition shape changed"):
                client.push(Request())


HAS_STUBS = all(importlib.util.find_spec(module) is not None for module in (
    "manager_pb2", "manager_pb2_grpc", "parameter_server_pb2",
    "parameter_server_pb2_grpc", "range_clocks",
))


@unittest.skipUnless(HAS_STUBS, "Requires actual generated gRPC stubs and RangeClocks")
class AutoRecoveryProcessTests(unittest.TestCase):
    def setUp(self):
        import grpc
        import manager_pb2_grpc as mgrpc
        import parameter_server_pb2 as pb
        import parameter_server_pb2_grpc as pgrpc
        from manager_rpc import ManagerService
        from replica_transport import ReplicaPeer
        from routing import KEY_SPACE_END, Partition
        from server_manager import ServerManager

        self.grpc = grpc
        self.pb = pb
        self.pgrpc = pgrpc
        self.third = KEY_SPACE_END // 3
        self.keys = (7, self.third + 7, 2 * self.third + 7)
        self.addresses = tuple(self._free_address() for _ in range(3))
        self.s1, self.s2, self.s3 = self.addresses
        self.manager = ServerManager([
            Partition("p0", 0, self.third, self.s1),
            Partition("p1", self.third, 2 * self.third, self.s2),
            Partition("p2", 2 * self.third, KEY_SPACE_END, self.s3),
        ], heartbeat_timeout_s=0.50)
        self.manager_server = grpc.server(ThreadPoolExecutor(max_workers=8))
        mgrpc.add_OwnershipManagerServicer_to_server(
            ManagerService(self.manager), self.manager_server)
        port = self.manager_server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        self.manager_address = f"127.0.0.1:{port}"
        self.manager_server.start()
        self.manager.start_heartbeat_monitor(interval_s=0.025)
        self.processes = {}
        self.peers = {}
        server_file = Path(__file__).resolve().parents[1] / "server.py"
        for address in self.addresses:
            process = subprocess.Popen([
                sys.executable, "-u", str(server_file), "--listen", address,
                "--manager", self.manager_address, "--bootstrap-ownership",
                "--heartbeat-interval", "0.05", "--heartbeat-rpc-timeout", "0.2",
            ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True)
            self.processes[address] = process
            self.peers[address] = ReplicaPeer(address, timeout_s=2.0)
        for address in self.addresses:
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=8)
        self.assertTrue(wait_until(lambda: all(
            entry["state"] == "ALIVE"
            for entry in self.manager.heartbeat_snapshot().values()
        )), "Server heartbeats did not initialize")
        self.manager.configure_replication(self.addresses, k=1)
        self.manager.seed_replicas(self.peers)
        self.controller = None
        self.retries = []
        self.client = FailoverPushClient(
            self.manager_address, attempt_timeout_s=0.18,
            retry_interval_s=0.025, overall_timeout_s=9.0,
            on_retry=lambda attempt, owner, status:
                self.retries.append((attempt, owner, status)),
        )

    @staticmethod
    def _free_address():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return f"127.0.0.1:{sock.getsockname()[1]}"

    def tearDown(self):
        if getattr(self, "controller", None) is not None:
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

    def request(self, key, sequence, *, worker_id=19):
        req = self.pb.PushRequest(keys=[key], gradients=[1.0, 2.0], width=2)
        req.task_id.worker_id = worker_id
        req.task_id.sequence = sequence
        return req

    def value(self, address, key):
        with self.grpc.insecure_channel(address) as channel:
            return list(self.pgrpc.ParameterServerStub(channel).Pull(
                self.pb.PullRequest(keys=[key]), timeout=3,
            ).values)

    def start_controller(self):
        self.controller = AutomaticFailoverController(
            self.manager, self.peers,
            lambda server_id: (server_id in self.processes
                               and self.processes[server_id].poll() is not None),
            scan_interval_s=0.025, retry_interval_s=0.06,
        )
        self.controller.start()

    def test_real_crash_automatically_promotes_repairs_and_retries_worker(self):
        self.manager.activate_live_replication(self.peers)
        first = self.request(self.keys[0], 1)
        self.assertTrue(self.client.push(first).accepted)
        self.start_controller()
        t_fault = time.monotonic()
        self.processes[self.s1].kill()
        self.processes[self.s1].wait(timeout=3)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.client.push, self.request(self.keys[0], 2))
            self.assertTrue(future.result(timeout=10).accepted)
        self.assertTrue(wait_until(lambda:
            len(self.controller.snapshot()["recoveries"]) == 1))
        recovery = self.controller.snapshot()["recoveries"][0]
        self.assertEqual(self.manager.get_map().epoch, 2)
        self.assertEqual(self.manager.get_map().partitions[0].owner, self.s2)
        self.assertTrue(self.retries, "Worker should retry while S1 is unavailable")
        self.assertEqual(self.value(self.s2, self.keys[0]), [-0.2, -0.4])
        # Retry a completed Push, preserving original bytes/ID.
        self.assertTrue(self.client.push(first).accepted)
        self.assertEqual(self.value(self.s2, self.keys[0]), [-0.2, -0.4])
        backup = self.peers[self.s3].get_replica_snapshot(self.s2, "p0")
        self.assertEqual(backup["sequence"], 1)
        self.assertEqual(len(backup["completed_pushes"]), 2)
        self.assertGreaterEqual(recovery["detected_at_s"], t_fault)
        self.assertGreaterEqual(recovery["ready_at_s"], recovery["promoted_at_s"])
        print("F3.3 automatic readiness after injected kill:",
              f"{recovery['ready_at_s'] - t_fault:.3f}s")

    def test_batched_replication_and_nonretryable_error(self):
        self.manager.activate_live_replication(
            self.peers, batch_size=3, batch_timeout_s=0.05,
        )
        first = self.request(self.keys[0], 1)
        self.assertTrue(self.client.push(first).accepted)
        # Invalid vector width: worker must not retry a deterministic error.
        bad = self.request(self.keys[1], 77)
        bad.width = 7
        with self.assertRaises(self.grpc.RpcError) as caught:
            self.client.push(bad)
        self.assertEqual(caught.exception.code(), self.grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(self.retries, [])
        self.start_controller()
        self.processes[self.s1].kill()
        self.processes[self.s1].wait(timeout=3)
        self.assertTrue(self.client.push(self.request(self.keys[0], 2)).accepted)
        self.assertTrue(wait_until(lambda:
            len(self.controller.snapshot()["recoveries"]) == 1))
        self.assertEqual(self.value(self.s2, self.keys[0]), [-0.2, -0.4])
        self.assertTrue(self.client.push(first).accepted)
        self.assertEqual(self.value(self.s2, self.keys[0]), [-0.2, -0.4])
        # Repair retained batch_size=3, so accepted writes still replicate.
        replica = self.peers[self.s3].get_replica_snapshot(self.s2, "p0")
        self.assertEqual(replica["sequence"], 1)


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F3.3a automatic recovery and exact physical retry: PASS")
    else:
        raise SystemExit(1)
