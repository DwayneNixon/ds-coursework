
import unittest

from routing import Partition, RoutingTable, KEY_SPACE_END


class TestOwnership(unittest.TestCase):

    def setUp(self):
        self.table = RoutingTable([
            Partition("p1", 0, 40, "server1"),
            Partition("p2", 40, KEY_SPACE_END, "server2"),
        ])

    def test_dense_range_correct_owner(self):
        self.assertTrue(
            self.table.owns("server1", range(10, 20))
        )

        self.assertTrue(
            self.table.owns("server2", range(40, 50))
        )

    def test_wrong_owner_rejected(self):
        self.assertFalse(
            self.table.owns("server1", range(40, 50))
        )

        self.assertFalse(
            self.table.owns("server2", [10, 20])
        )

    def test_cross_partition_request_rejected(self):
        self.assertFalse(
            self.table.owns("server1", range(38, 42))
        )

        self.assertFalse(
            self.table.owns("server2", range(38, 42))
        )

    def test_sparse_key_ownership(self):
        self.assertTrue(
            self.table.owns("server1", [5, 10, 39])
        )

        self.assertTrue(
            self.table.owns("server2", [40, 50, 90])
        )

        self.assertFalse(
            self.table.owns("server1", [10, 50])
        )

    def test_ownership_changes_with_map(self):
        new_table = RoutingTable([
            Partition("p1", 0, 40, "server1"),
            Partition("p2", 40, KEY_SPACE_END, "server3"),
        ], epoch=2)

        self.assertTrue(
            self.table.owns("server2", [50])
        )

        self.assertFalse(
            new_table.owns("server2", [50])
        )

        self.assertTrue(
            new_table.owns("server3", [50])
        )

    def test_invalid_range_step(self):
        with self.assertRaises(ValueError):
            self.table.owns(
                "server1", range(0, 20, 2)
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
