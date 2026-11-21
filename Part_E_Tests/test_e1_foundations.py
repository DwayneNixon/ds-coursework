
"""Part E1.1 — Task-ID and range-clock transfer foundations."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from range_clocks import RangeClocks
from routing import KEY_SPACE_END, Partition, RoutingTable
from parameter_client import ParameterClient
from distributed_client import DistributedParameterClient


class RangeClockTransferTests(unittest.TestCase):

    def test_extract_preserves_clocks(self):
        clocks = RangeClocks(0, 100)
        clocks.record(10, 40, 11, 3)
        clocks.record(25, 70, 12, 5)

        moved = clocks.extract(20, 60)

        self.assertEqual((moved.begin, moved.end), (20, 60))
        self.assertEqual(moved.clock_for_key(20), {11: 3})
        self.assertEqual(
            moved.clock_for_key(25),
            {11: 3, 12: 5},
        )
        self.assertEqual(moved.clock_for_key(50), {12: 5})
        self.assertEqual(clocks.clock_for_key(9), {})
        self.assertEqual(
            clocks.clock_for_key(30),
            {11: 3, 12: 5},
        )
        self.assertEqual(clocks.clock_for_key(80), {})

        moved.record(20, 30, 99, 1)
        self.assertNotIn(99, clocks.clock_for_key(25))

    def test_snapshot_restore_is_independent(self):
        source = RangeClocks(0, 32)
        source.record(8, 24, 101, 4)

        snapshot = source.extract(5, 28).snapshot()
        restored = RangeClocks.from_segments(
            5, 28, snapshot
        )

        self.assertEqual(restored.snapshot(), snapshot)

        snapshot[1][2][101] = 99
        self.assertEqual(
            restored.clock_for_key(12),
            {101: 4},
        )

        restored.record(12, 14, 202, 2)
        self.assertEqual(
            source.clock_for_key(12),
            {101: 4},
        )

    def test_keyspace_end_boundary(self):
        source = RangeClocks(
            KEY_SPACE_END - 20, KEY_SPACE_END
        )
        source.record(
            KEY_SPACE_END - 5, KEY_SPACE_END, 7, 8
        )

        moved = source.extract(
            KEY_SPACE_END - 10, KEY_SPACE_END
        )

        restored = RangeClocks.from_segments(
            moved.begin, moved.end, moved.snapshot()
        )

        self.assertEqual(
            restored.clock_for_key(KEY_SPACE_END - 1),
            {7: 8},
        )
        self.assertEqual(
            restored.clock_for_key(KEY_SPACE_END - 10),
            {},
        )

    def test_reject_invalid_ranges_and_snapshots(self):
        source = RangeClocks(0, 20)

        for begin, end in [
            (-1, 5), (5, 21), (10, 10), (18, 4)
        ]:
            with self.subTest(begin=begin, end=end):
                with self.assertRaises(ValueError):
                    source.extract(begin, end)

        malformed = [
            [],
            [(0, 5, {}), (6, 10, {})],
            [(0, 6, {}), (5, 10, {})],
            [(0, 10, {1: 0})],
            [(0, 10, {1 << 32: 1})],
            [(0, 10, {1: 1 << 64})],
            [(0, 10, {}), (10, 11, {})],
        ]

        for segments in malformed:
            with self.subTest(segments=segments):
                with self.assertRaises(ValueError):
                    RangeClocks.from_segments(
                        0, 10, segments
                    )


class DistributedTaskIdTests(unittest.TestCase):

    @staticmethod
    def initial_table():
        return RoutingTable([
            Partition(
                "left", 0, 50, "127.0.0.1:59991"
            ),
            Partition(
                "right", 50, KEY_SPACE_END,
                "127.0.0.1:59992"
            ),
        ], epoch=1)

    def test_standalone_sequence_unchanged(self):
        client = ParameterClient(
            "127.0.0.1:59991", worker_id=77
        )

        try:
            self.assertEqual(
                client._next_task_id(), (77, 1)
            )
            self.assertEqual(
                client._next_task_id(), (77, 2)
            )
            self.assertEqual(
                client._next_pull_task_id(),
                (77, (1 << 63) + 1),
            )
        finally:
            client.close()

    def test_distributed_servers_share_sequences(self):
        client = DistributedParameterClient(
            self.initial_table(),
            worker_id=1001,
            value_width=2,
        )

        try:
            a = client.clients["127.0.0.1:59991"]
            b = client.clients["127.0.0.1:59992"]

            self.assertEqual(
                a._next_task_id(), (1001, 1)
            )
            self.assertEqual(
                b._next_task_id(), (1001, 2)
            )
            self.assertEqual(
                a._next_pull_task_id(),
                (1001, (1 << 63) + 1),
            )
            self.assertEqual(
                b._next_pull_task_id(),
                (1001, (1 << 63) + 2),
            )
            self.assertEqual(
                a._next_task_id(), (1001, 3)
            )
        finally:
            client.close()

    def test_concurrent_task_id_allocation(self):
        client = DistributedParameterClient(
            self.initial_table(),
            worker_id=1002,
            value_width=2,
        )

        try:
            owners = list(client.clients.values())

            with ThreadPoolExecutor(max_workers=12) as pool:
                push_ids = list(pool.map(
                    lambda i: owners[i % 2]._next_task_id(),
                    range(200),
                ))
                pull_ids = list(pool.map(
                    lambda i: owners[i % 2]._next_pull_task_id(),
                    range(200),
                ))

            self.assertEqual(
                len(set(push_ids + pull_ids)),
                400,
            )
            self.assertEqual(
                {seq for _, seq in push_ids},
                set(range(1, 201)),
            )
            self.assertEqual(
                {seq for _, seq in pull_ids},
                {(1 << 63) + i for i in range(1, 201)},
            )
        finally:
            client.close()

    def test_new_server_preserves_allocator_and_compression(self):
        client = DistributedParameterClient(
            self.initial_table(),
            worker_id=1003,
            value_width=2,
            key_caching=True,
            value_compression=True,
        )

        try:
            original = client.clients["127.0.0.1:59991"]

            self.assertEqual(
                original._next_task_id(),
                (1003, 1),
            )

            new_table = RoutingTable([
                Partition(
                    "left", 0, 50, "127.0.0.1:59991"
                ),
                Partition(
                    "right_a", 50, 100,
                    "127.0.0.1:59992"
                ),
                Partition(
                    "right_b", 100, KEY_SPACE_END,
                    "127.0.0.1:59993"
                ),
            ], epoch=2)

            client.manager_address = "mock-manager"

            with patch(
                "distributed_client.fetch_ownership_map",
                return_value=new_table,
            ):
                self.assertTrue(client.refresh_map())

            newcomer = client.clients["127.0.0.1:59993"]

            self.assertTrue(newcomer.value_compression)
            self.assertEqual(
                newcomer._next_task_id(), (1003, 2)
            )
            self.assertEqual(
                original._next_task_id(), (1003, 3)
            )
            self.assertEqual(
                newcomer._next_pull_task_id(),
                (1003, (1 << 63) + 1),
            )
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
