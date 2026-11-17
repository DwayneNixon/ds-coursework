
import unittest

from consistency import ConsistencyGate


class TestConsistencyGate(unittest.TestCase):

    def test_sequential_requires_all_workers(self):
        gate = ConsistencyGate([1, 2], mode="sequential")

        self.assertTrue(gate.can_start(1))
        self.assertFalse(gate.can_start(2))

        gate.complete(1, 1)
        self.assertFalse(gate.can_start(2))

        gate.complete(2, 1)
        self.assertTrue(gate.can_start(2))

    def test_bounded_delay_tau_two(self):
        gate = ConsistencyGate(
            [1, 2], mode="bounded", tau=2
        )

        self.assertTrue(gate.can_start(1))
        self.assertTrue(gate.can_start(2))
        self.assertTrue(gate.can_start(3))

        # Iteration 4 requires everyone to finish 1.
        self.assertFalse(gate.can_start(4))

        gate.complete(1, 1)
        self.assertFalse(gate.can_start(4))

        gate.complete(2, 1)
        self.assertTrue(gate.can_start(4))

    def test_eventual_has_no_global_barrier(self):
        gate = ConsistencyGate([1, 2], mode="eventual")

        self.assertTrue(gate.can_start(1))
        self.assertTrue(gate.can_start(10))
        self.assertTrue(gate.can_start(100))

    def test_contiguous_completion(self):
        gate = ConsistencyGate([1, 2])

        with self.assertRaises(ValueError):
            gate.complete(1, 2)

        gate.complete(1, 1)

        with self.assertRaises(ValueError):
            gate.complete(1, 1)

        self.assertEqual(gate.snapshot(), {1: 1, 2: 0})

    def test_wait_timeout(self):
        gate = ConsistencyGate([1, 2])

        with self.assertRaises(TimeoutError):
            gate.wait_until_allowed(2, timeout_ms=10)

    def test_wait_released_by_completion(self):
        import threading

        gate = ConsistencyGate([1, 2])
        finished = threading.Event()
        failures = []

        def wait_for_second_iteration():
            try:
                gate.wait_until_allowed(2, timeout_ms=2000)
                finished.set()
            except Exception as error:
                failures.append(error)

        thread = threading.Thread(
            target=wait_for_second_iteration
        )

        thread.start()

        gate.complete(1, 1)
        gate.complete(2, 1)

        thread.join(timeout=3)

        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertTrue(finished.is_set())

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            ConsistencyGate([1, 2], mode="unknown")

        with self.assertRaises(ValueError):
            ConsistencyGate([1, 2], mode="bounded", tau=-1)

        with self.assertRaises(ValueError):
            ConsistencyGate([1, 1])

    def test_required_iterations(self):
        sequential = ConsistencyGate([1], "sequential")
        bounded = ConsistencyGate([1], "bounded", tau=2)
        eventual = ConsistencyGate([1], "eventual")

        self.assertEqual(
            sequential.required_iteration(4), 3
        )
        self.assertEqual(
            bounded.required_iteration(4), 1
        )
        self.assertEqual(
            eventual.required_iteration(4), 0
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
