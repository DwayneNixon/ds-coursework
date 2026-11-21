
import unittest

from range_clocks import RangeClocks, clock_leq


class TestRangeClocks(unittest.TestCase):

    def test_initial_partition_clock(self):
        clocks = RangeClocks(0, 100)

        self.assertEqual(clocks.count(), 1)
        self.assertEqual(
            clocks.snapshot(),
            [(0, 100, {})],
        )

    def test_coursework_figure_four(self):
        clocks = RangeClocks(
            0, 100, initial={1: 4, 2: 4}
        )

        clocks.record(20, 60, worker_id=1, counter=5)

        self.assertEqual(clocks.snapshot(), [
            (0, 20, {1: 4, 2: 4}),
            (20, 60, {1: 5, 2: 4}),
            (60, 100, {1: 4, 2: 4}),
        ])

        clocks.record(40, 80, worker_id=2, counter=5)

        self.assertEqual(clocks.snapshot(), [
            (0, 20, {1: 4, 2: 4}),
            (20, 40, {1: 5, 2: 4}),
            (40, 60, {1: 5, 2: 5}),
            (60, 80, {1: 4, 2: 5}),
            (80, 100, {1: 4, 2: 4}),
        ])

    def test_adjacent_clocks_merge(self):
        clocks = RangeClocks(0, 10)

        clocks.record(2, 5, 1, 1)
        clocks.record(5, 8, 1, 1)

        self.assertEqual(clocks.snapshot(), [
            (0, 2, {}),
            (2, 8, {1: 1}),
            (8, 10, {}),
        ])

    def test_vector_clock_ordering(self):
        earlier = {1: 2, 2: 3}
        later = {1: 3, 2: 3}

        self.assertTrue(clock_leq(earlier, later))
        self.assertFalse(clock_leq(later, earlier))

        # Neither clock is no later than the other.
        a = {1: 3, 2: 1}
        b = {1: 1, 2: 3}

        self.assertFalse(clock_leq(a, b))
        self.assertFalse(clock_leq(b, a))

    def test_scattered_single_key_updates(self):
        clocks = RangeClocks(0, 100)

        for counter, key in enumerate(
            [10, 20, 30, 40, 50], start=1
        ):
            clocks.record(
                key, key + 1,
                worker_id=1,
                counter=counter,
            )

        # Each isolated key creates a clock region,
        # with unchanged regions in between.
        self.assertEqual(clocks.count(), 11)

    def test_repeated_range_updates(self):
        clocks = RangeClocks(0, 100)

        for counter in range(1, 51):
            clocks.record(20, 80, 1, counter)

        # Repeated updates with the same boundaries
        # should not create additional clock segments.
        self.assertEqual(clocks.count(), 3)
        self.assertEqual(
            clocks.clock_for_key(50),
            {1: 50},
        )

    def test_invalid_update_preserves_state(self):
        clocks = RangeClocks(0, 100)
        before = clocks.snapshot()

        with self.assertRaises(ValueError):
            clocks.record(90, 110, 1, 1)

        self.assertEqual(clocks.snapshot(), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
