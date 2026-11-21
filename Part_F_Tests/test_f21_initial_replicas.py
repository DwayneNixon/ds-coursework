"""F2.1: deterministic backup placement and non-serving initial snapshots.

Run from project root:
    python test_f21_initial_replicas.py

This is the INITIAL-SEED milestone, not live/durable Push replication.
"""

import copy
import unittest

from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


ONE_THIRD = KEY_SPACE_END // 3
PARTITIONS = [
    Partition("p0", 0, ONE_THIRD, "s1"),
    Partition("p1", ONE_THIRD, 2 * ONE_THIRD, "s2"),
    Partition("p2", 2 * ONE_THIRD, KEY_SPACE_END, "s3"),
]


def new_manager(partitions=None):
    return ServerManager(PARTITIONS if partitions is None else partitions)


class ReplicaPlacementTests(unittest.TestCase):
    def setUp(self):
        self.manager = new_manager()

    def test_k1_clockwise_neighbor_placement(self):
        plan = self.manager.configure_replication(["s1", "s2", "s3"], k=1)
        self.assertEqual(plan, {
            "p0": ("s2",), "p1": ("s3",), "p2": ("s1",),
        })
        self.assertEqual(self.manager.get_map().epoch, 1)
        self.assertEqual(self.manager.get_replication_plan(), plan)

    def test_k2_assigns_distinct_neighbors_without_primary(self):
        plan = self.manager.configure_replication(["s1", "s2", "s3"], k=2)
        self.assertEqual(plan, {
            "p0": ("s2", "s3"),
            "p1": ("s3", "s1"),
            "p2": ("s1", "s2"),
        })
        for partition in PARTITIONS:
            backups = plan[partition.partition_id]
            self.assertEqual(len(backups), len(set(backups)))
            self.assertNotIn(partition.owner, backups)

    def test_ring_order_is_explicit_and_repeatable(self):
        # Server order is NOT silently alphabetical.
        plan = self.manager.configure_replication(["s3", "s1", "s2"])
        self.assertEqual(plan["p2"], ("s1",))
        self.assertEqual(plan["p0"], ("s2",))
        self.assertEqual(plan["p1"], ("s3",))

    def test_invalid_configurations_are_rejected(self):
        invalid = [
            ([], 1), (["s1"], 1), (["s1", "s2", "s3"], 0),
            (["s1", "s2", "s3"], 3),
            (["s1", "s2", "s3"], True),
            (["s1", "s2", "s2"], 1),
            (["s1", "s2", ""], 1),
            (["s1", "s2", "unregistered"], 1),
            (["s1", "s2"], 1),  # primary s3 is missing
            ("s1,s2,s3", 1),
        ]
        for server_ids, k in invalid:
            with self.subTest(server_ids=server_ids, k=k):
                with self.assertRaises(ValueError):
                    new_manager().configure_replication(server_ids, k=k)

    def test_backup_only_server_must_be_explicitly_registered(self):
        single = new_manager([Partition("all", 0, KEY_SPACE_END, "s1")])
        single.register_server("s2")
        self.assertEqual(single.configure_replication(["s1", "s2"]), {
            "all": ("s2",),
        })

    def test_plan_is_independent_and_stale_after_repartition(self):
        self.manager.configure_replication(["s1", "s2", "s3"])
        detached = self.manager.get_replication_plan()
        detached["p0"] = ("malicious",)
        self.assertEqual(self.manager.get_replication_plan()["p0"], ("s2",))
        with self.assertRaises(RuntimeError):
            self.manager.configure_replication(["s1", "s2", "s3"])
        self.manager.publish_map(PARTITIONS)
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.manager.get_replication_plan()


class ReplicaSeedTests(unittest.TestCase):
    def setUp(self):
        from server import ParameterServer

        self.manager = new_manager()
        self.table = self.manager.get_map()
        self.servers = {sid: ParameterServer() for sid in ("s1", "s2", "s3")}
        for sid, server in self.servers.items():
            server.set_ownership(sid, self.table)

    def _seed_example(self):
        s1 = self.servers["s1"]
        with s1.lock:
            s1.weights[7] = [1.25, -4.0]
            s1.range_clocks["p0"].record(7, 8, 11, 3)
            task = (11, 123)
            fingerprint = b"A" * 32
            s1.completed_pushes[task] = fingerprint
            s1.completed_push_bounds[task] = (7, 8)
            s1.task_states[task] = "DONE"
        return task, fingerprint

    def test_initial_seed_preserves_weights_clocks_and_dedup(self):
        task, fingerprint = self._seed_example()
        self.manager.configure_replication(["s1", "s2", "s3"], k=1)
        copied = self.manager.seed_replicas(self.servers)
        self.assertEqual(set(copied), {
            ("p0", "s2"), ("p1", "s3"), ("p2", "s1"),
        })
        backup = self.servers["s2"]
        replica = backup.get_replica_snapshot("s1", "p0")
        self.assertEqual(replica["weights"], {7: [1.25, -4.0]})
        self.assertEqual(replica["completed_pushes"][task], (fingerprint, (7, 8)))
        self.assertEqual(replica["clock_segments"],
                         self.servers["s1"].range_clocks["p0"].snapshot())
        self.assertEqual(replica["sequence"], 0)
        self.assertEqual(replica["epoch"], 1)
        self.assertEqual(replica["active_rule"], "sgd")
        # Shadow replica is NOT exposed as primary data to workers.
        self.assertNotIn(7, backup.weights)
        self.assertNotIn("p0", backup.range_clocks)
        self.assertEqual(self.manager.get_map().epoch, 1)

    def test_snapshot_and_replica_are_deeply_isolated(self):
        self._seed_example()
        snapshot = self.servers["s1"].export_replica_snapshot("p0")
        snapshot["weights"][7][0] = 99.0
        self.assertEqual(self.servers["s1"].weights[7][0], 1.25)

        original = self.servers["s1"].export_replica_snapshot("p0")
        self.servers["s2"].install_replica_snapshot(original)
        returned = self.servers["s2"].get_replica_snapshot("s1", "p0")
        returned["weights"][7][0] = 77.0
        returned["completed_pushes"].clear()
        fresh = self.servers["s2"].get_replica_snapshot("s1", "p0")
        self.assertEqual(fresh["weights"][7][0], 1.25)
        self.assertEqual(len(fresh["completed_pushes"]), 1)

    def test_backup_rejects_wrong_primary_epoch_and_values(self):
        self._seed_example()
        original = self.servers["s1"].export_replica_snapshot("p0")
        for change in (
            {"primary_id": "s3"},
            {"epoch": 0},
            {"epoch": 2},
            {"value_width": 3},
            {"begin": 1},
            {"learning_rate": float("nan")},
        ):
            with self.subTest(change=change):
                broken = copy.deepcopy(original)
                broken.update(change)
                with self.assertRaises((ValueError, RuntimeError)):
                    self.servers["s2"].install_replica_snapshot(broken)

    def test_replica_cannot_be_installed_twice(self):
        snapshot = self.servers["s1"].export_replica_snapshot("p0")
        self.servers["s2"].install_replica_snapshot(snapshot)
        with self.assertRaisesRegex(RuntimeError, "already installed"):
            self.servers["s2"].install_replica_snapshot(snapshot)

    def test_initial_seed_does_not_automatically_track_later_writes(self):
        self.manager.configure_replication(["s1", "s2", "s3"], k=1)
        self.manager.seed_replicas(self.servers)
        with self.servers["s1"].lock:
            self.servers["s1"].weights[7] = [5.0, 6.0]
        # This limitation is intentional in F2.1, and will be fixed in F2.2.
        self.assertNotIn(7, self.servers["s2"]
                         .get_replica_snapshot("s1", "p0")["weights"])


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False).result
    if result.wasSuccessful():
        print("F2.1 replica placement and initial state: PASS")
    else:
        raise SystemExit(1)
