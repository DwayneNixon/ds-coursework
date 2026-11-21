
"""Part E3.1 — Dynamic worker membership in ConsistencyGate."""

import unittest

from consistency import ConsistencyGate


class DynamicGateTests(unittest.TestCase):

    def test_original_sequential_behavior(self):
        gate = ConsistencyGate(
            [101, 102],
            mode="sequential",
        )

        self.assertTrue(gate.can_start(1))
        self.assertFalse(gate.can_start(2))

        gate.complete(101, 1)
        self.assertFalse(gate.can_start(2))

        gate.complete(102, 1)
        self.assertTrue(gate.can_start(2))

        self.assertEqual(
            gate.snapshot(),
            {101: 1, 102: 1},
        )

    def test_join_after_training_has_started(self):
        gate = ConsistencyGate(
            [101, 102],
            mode="sequential",
        )

        gate.complete(101, 1)
        gate.complete(102, 1)
        gate.complete(101, 2)

        # Worker 101 is at 2, worker 102 at 1.
        # The new worker starts at min(2, 1) + 1.
        starting_iteration = gate.register_worker(103)

        self.assertEqual(starting_iteration, 2)
        self.assertEqual(gate.membership_epoch, 2)

        self.assertEqual(
            gate.active_snapshot(),
            {101: 2, 102: 1, 103: 1},
        )

        self.assertTrue(
            gate.can_start(2, worker_id=103)
        )

        gate.complete(103, 2)
        gate.complete(102, 2)

        self.assertTrue(gate.can_start(3))

    def test_leave_unblocks_consistency_barrier(self):
        gate = ConsistencyGate(
            [201, 202],
            mode="sequential",
        )

        gate.complete(201, 1)

        # Worker 202 is behind.
        self.assertFalse(
            gate.can_start(2, worker_id=201)
        )

        last_iteration = gate.leave_worker(202)

        self.assertEqual(last_iteration, 0)
        self.assertEqual(gate.membership_epoch, 2)

        # Historical progress remains available.
        self.assertEqual(
            gate.snapshot(),
            {201: 1, 202: 0},
        )

        # Only worker 201 participates in barriers now.
        self.assertEqual(
            gate.active_snapshot(),
            {201: 1},
        )

        self.assertTrue(
            gate.can_start(2, worker_id=201)
        )

        gate.complete(201, 2)

        self.assertEqual(gate.snapshot()[201], 2)

    def test_departed_worker_cannot_complete_or_rejoin(self):
        gate = ConsistencyGate(
            [301, 302],
            mode="sequential",
        )

        gate.complete(302, 1)
        gate.leave_worker(302)

        with self.assertRaises(ValueError):
            gate.complete(302, 2)

        with self.assertRaises(ValueError):
            gate.can_start(2, worker_id=302)

        # Old worker IDs cannot be reused because the
        # parameter servers may retain their vector clocks.
        with self.assertRaises(ValueError):
            gate.register_worker(302)

        self.assertEqual(gate.snapshot()[302], 1)

    def test_cannot_remove_last_worker(self):
        gate = ConsistencyGate([401])

        with self.assertRaises(ValueError):
            gate.leave_worker(401)

        self.assertEqual(
            gate.active_snapshot(),
            {401: 0},
        )

    def test_bounded_join_preserves_delay_bound(self):
        gate = ConsistencyGate(
            [501, 502],
            mode="bounded",
            tau=1,
        )

        gate.complete(501, 1)
        gate.complete(501, 2)

        # Worker 502 is at zero, so worker 501
        # cannot start iteration 3 with tau=1.
        self.assertFalse(
            gate.can_start(3, worker_id=501)
        )

        start = gate.register_worker(503)

        self.assertEqual(start, 1)
        self.assertEqual(gate.snapshot()[503], 0)

        gate.complete(502, 1)
        gate.complete(503, 1)

        self.assertTrue(
            gate.can_start(3, worker_id=501)
        )

    def test_eventual_join(self):
        gate = ConsistencyGate(
            [601],
            mode="eventual",
        )

        for iteration in range(1, 6):
            gate.complete(601, iteration)

        start = gate.register_worker(602)

        self.assertEqual(start, 6)

        self.assertTrue(
            gate.can_start(6, worker_id=602)
        )

        gate.complete(602, 6)

        self.assertEqual(
            gate.active_snapshot(),
            {601: 5, 602: 6},
        )

    def test_invalid_membership_operations(self):
        gate = ConsistencyGate([701, 702])

        for invalid_id in [
            -1,
            1 << 32,
            "worker",
            True,
        ]:
            with self.subTest(worker=invalid_id):
                with self.assertRaises(ValueError):
                    gate.register_worker(invalid_id)

        with self.assertRaises(ValueError):
            gate.register_worker(701)

        with self.assertRaises(ValueError):
            gate.leave_worker(999)

        with self.assertRaises(ValueError):
            gate.can_start(2, worker_id=701)

        with self.assertRaises(ValueError):
            gate.complete(701, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
