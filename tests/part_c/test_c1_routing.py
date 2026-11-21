
import unittest

from routing import (
    Partition,
    RoutingTable,
    KEY_SPACE_END,
    split_push,
    merge_pull,
)

class TestC1Routing(unittest.TestCase):

    def setUp(self):
        self.table = RoutingTable([
            Partition("p1", 0, 40, "127.0.0.1:50051"),
            Partition("p2", 40, 80, "127.0.0.1:50052"),
            Partition("p3", 80, KEY_SPACE_END, "127.0.0.1:50053"),
        ])

    def test_single_partition(self):
        result = self.table.split_range(10, 20)
        self.assertEqual(len(result), 1)
        self.assertEqual(
            (result[0].partition_id, result[0].begin, result[0].end),
            ("p1", 10, 20),
        )

    def test_multiple_partitions(self):
        result = self.table.split_range(30, 90)

        actual = [
            (r.partition_id, r.begin, r.end)
            for r in result
        ]

        self.assertEqual(actual, [
            ("p1", 30, 40),
            ("p2", 40, 80),
            ("p3", 80, 90),
        ])

    def test_exact_boundary(self):
        result = self.table.split_range(40, 80)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].partition_id, "p2")

    def test_empty_range(self):
        self.assertEqual(self.table.split_range(40, 40), [])

    def test_invalid_range(self):
        with self.assertRaises(ValueError):
            self.table.split_range(90, 30)

    def test_gap_rejected(self):
        with self.assertRaises(ValueError):
            RoutingTable([
                Partition("p1", 0, 40, "server1"),
                Partition("p2", 41, KEY_SPACE_END, "server2"),
            ])

    def test_high_uint64_key(self):
        result = self.table.split_range(
            KEY_SPACE_END - 1, KEY_SPACE_END
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].partition_id, "p3")


    def test_sparse_scattered_keys(self):
        result = self.table.split_keys([90, 5, 45, 10, 85])

        actual = [
            (r.partition_id, r.keys, r.positions)
            for r in result
        ]

        self.assertEqual(actual, [
            ("p1", (5, 10), (1, 3)),
            ("p2", (45,), (2,)),
            ("p3", (90, 85), (0, 4)),
        ])

    def test_sparse_exact_boundaries(self):
        result = self.table.split_keys([0, 40, 80])

        self.assertEqual(
            [r.partition_id for r in result],
            ["p1", "p2", "p3"],
        )

    def test_sparse_empty(self):
        self.assertEqual(self.table.split_keys([]), [])

    def test_sparse_duplicates_rejected(self):
        with self.assertRaises(ValueError):
            self.table.split_keys([5, 45, 5])

    def test_sparse_invalid_keys(self):
        invalid_keys = [
            -1,
            KEY_SPACE_END,
            3.5,
            True,
        ]

        for key in invalid_keys:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    self.table.split_keys([key])

    def test_sparse_preserves_local_order(self):
        result = self.table.split_keys([30, 5, 20])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].keys, (30, 5, 20))
        self.assertEqual(result[0].positions, (0, 1, 2))


    def test_dense_push_splitting(self):
        target = range(38, 42)
        routes = self.table.split_range(38, 42)

        gradients = [1, 10, 2, 20, 3, 30, 4, 40]
        chunks = split_push(target, gradients, routes, width=2)

        self.assertEqual(
            [chunks[r] for r in routes],
            [
                [1, 10, 2, 20],
                [3, 30, 4, 40],
            ],
        )

    def test_sparse_push_splitting(self):
        target = [90, 5, 45, 10, 85]
        routes = self.table.split_keys(target)

        gradients = [9, 90, 5, 50, 4, 40, 1, 10, 8, 80]
        chunks = split_push(target, gradients, routes, width=2)

        self.assertEqual(
            [chunks[r] for r in routes],
            [
                [5, 50, 1, 10],
                [4, 40],
                [9, 90, 8, 80],
            ],
        )

    def test_dense_pull_merging(self):
        target = range(38, 42)
        routes = self.table.split_range(38, 42)

        # Responses arrive in reverse server order.
        replies = {
            routes[1]: [3, 30, 4, 40],
            routes[0]: [1, 10, 2, 20],
        }

        result = merge_pull(target, routes, replies, width=2)

        self.assertEqual(
            result,
            [1, 10, 2, 20, 3, 30, 4, 40],
        )

    def test_sparse_pull_merging(self):
        target = [90, 5, 45, 10, 85]
        routes = self.table.split_keys(target)

        replies = {
            routes[2]: [9, 90, 8, 80],
            routes[0]: [5, 50, 1, 10],
            routes[1]: [4, 40],
        }

        result = merge_pull(target, routes, replies, width=2)

        self.assertEqual(
            result,
            [9, 90, 5, 50, 4, 40, 1, 10, 8, 80],
        )

    def test_push_gradient_length_mismatch(self):
        target = range(38, 42)
        routes = self.table.split_range(38, 42)

        with self.assertRaises(ValueError):
            split_push(
                target,
                [1, 2, 3],
                routes,
                width=2,
            )

    def test_pull_missing_server_response(self):
        target = range(38, 42)
        routes = self.table.split_range(38, 42)

        replies = {
            routes[0]: [1, 10, 2, 20],
        }

        with self.assertRaises(ValueError):
            merge_pull(target, routes, replies, width=2)

if __name__ == "__main__":
    unittest.main(verbosity=2)
