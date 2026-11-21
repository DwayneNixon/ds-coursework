
"""F1.1: manager heartbeats, timeout handling, and gRPC integration."""

import unittest
from concurrent.futures import ThreadPoolExecutor

import grpc

from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_manager(clock, timeout=2.0):
    return ServerManager(
        [Partition("p0", 0, KEY_SPACE_END, "server1")],
        heartbeat_timeout_s=timeout,
        clock=clock,
    )


class HeartbeatManagerTests(unittest.TestCase):

    def setUp(self):
        self.clock = FakeClock()
        self.manager = make_manager(self.clock)

    def test_registration_does_not_imply_liveness(self):
        state = self.manager.heartbeat_snapshot()["server1"]

        self.assertEqual(state["state"], "REGISTERED")
        self.assertIsNone(state["last_seen_s"])
        self.assertEqual(state["registered_at_s"], 100.0)
        self.assertEqual(self.manager.get_map().epoch, 1)

    def test_initially_silent_server_becomes_suspected(self):
        self.clock.advance(2.0)

        self.assertEqual(
            self.manager.check_server_timeouts(),
            ["server1"],
        )

        # No repeated transition for the same suspicion.
        self.assertEqual(self.manager.check_server_timeouts(), [])

        self.assertEqual(
            self.manager.heartbeat_snapshot()["server1"]["state"],
            "SUSPECTED",
        )

    def test_timeout_refresh_and_late_heartbeat(self):
        self.manager.record_heartbeat("server1")

        self.clock.advance(1.99)
        self.assertEqual(self.manager.check_server_timeouts(), [])

        self.clock.advance(0.01)
        self.assertEqual(
            self.manager.check_server_timeouts(),
            ["server1"],
        )

        self.assertEqual(self.manager.check_server_timeouts(), [])

        self.assertEqual(
            self.manager.heartbeat_snapshot()["server1"]["suspected_at_s"],
            102.0,
        )

        # F1.1 permits recovery from suspicion before failover.
        self.clock.advance(0.5)
        self.manager.record_heartbeat("server1")

        state = self.manager.heartbeat_snapshot()["server1"]

        self.assertEqual(state["state"], "ALIVE")
        self.assertEqual(state["last_seen_s"], 102.5)
        self.assertIsNone(state["suspected_at_s"])

        # Heartbeats must not alter ownership.
        self.assertEqual(self.manager.get_map().epoch, 1)

    def test_new_server_registration_and_id_validation(self):
        self.assertTrue(self.manager.register_server("server2"))
        self.assertFalse(self.manager.register_server("server2"))

        self.manager.record_heartbeat("server2")

        self.assertEqual(
            self.manager.heartbeat_snapshot()["server2"]["state"],
            "ALIVE",
        )

        self.assertEqual(
            self.manager.get_map().partitions[0].owner,
            "server1",
        )

        with self.assertRaises(KeyError):
            self.manager.record_heartbeat("not-registered")

        for bad in ("", "   ", None, 7):
            with self.assertRaises(ValueError):
                self.manager.record_heartbeat(bad)

            with self.assertRaises(ValueError):
                self.manager.register_server(bad)

    def test_timeout_configuration_validation(self):
        for bad in (
            0, -1, float("nan"), float("inf"), True, "2"
        ):
            with self.assertRaises(ValueError):
                make_manager(self.clock, timeout=bad)

        self.assertEqual(
            make_manager(self.clock, timeout=0.1).heartbeat_timeout_s,
            0.1,
        )

    def test_snapshots_are_independent(self):
        snapshot = self.manager.heartbeat_snapshot()
        snapshot["server1"]["state"] = "CORRUPTED"

        self.assertEqual(
            self.manager.heartbeat_snapshot()["server1"]["state"],
            "REGISTERED",
        )

    def test_concurrent_heartbeats(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    self.manager.record_heartbeat,
                    ["server1"] * 100,
                )
            )

        self.assertEqual(results, [100.0] * 100)

        self.assertEqual(
            self.manager.heartbeat_snapshot()["server1"]["state"],
            "ALIVE",
        )


class HeartbeatRpcTests(unittest.TestCase):

    def test_heartbeat_rpc_and_existing_getmap(self):
        import manager_pb2 as pb
        import manager_pb2_grpc as pb_grpc
        from manager_rpc import ManagerService

        manager = make_manager(FakeClock())

        server = grpc.server(
            ThreadPoolExecutor(max_workers=4)
        )

        pb_grpc.add_OwnershipManagerServicer_to_server(
            ManagerService(manager),
            server,
        )

        port = server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)

        server.start()

        try:
            with grpc.insecure_channel(
                f"127.0.0.1:{port}"
            ) as channel:
                stub = pb_grpc.OwnershipManagerStub(channel)

                response = stub.Heartbeat(
                    pb.HeartbeatRequest(server_id="server1"),
                    timeout=3.0,
                )

                self.assertTrue(response.accepted)

                self.assertEqual(
                    manager.heartbeat_snapshot()["server1"]["state"],
                    "ALIVE",
                )

                # Existing C2 ownership RPC must remain functional.
                original_map = stub.GetMap(
                    pb.GetMapRequest(),
                    timeout=3.0,
                )

                self.assertEqual(original_map.epoch, 1)
                self.assertEqual(
                    original_map.partitions[0].owner,
                    "server1",
                )

                with self.assertRaises(grpc.RpcError) as missing:
                    stub.Heartbeat(
                        pb.HeartbeatRequest(server_id="unknown"),
                        timeout=3.0,
                    )

                self.assertEqual(
                    missing.exception.code(),
                    grpc.StatusCode.NOT_FOUND,
                )

                with self.assertRaises(grpc.RpcError) as invalid:
                    stub.Heartbeat(
                        pb.HeartbeatRequest(server_id=""),
                        timeout=3.0,
                    )

                self.assertEqual(
                    invalid.exception.code(),
                    grpc.StatusCode.INVALID_ARGUMENT,
                )

        finally:
            server.stop(0).wait()


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result

    if result.wasSuccessful():
        print("F1.1 heartbeat foundations: PASS")
    else:
        raise SystemExit(1)
