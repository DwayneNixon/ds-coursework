
import unittest

from routing import Partition, KEY_SPACE_END
from server_manager import ServerManager


class TestServerManager(unittest.TestCase):

    def setUp(self):
        self.manager = ServerManager([
            Partition("p1", 0, 40, "server1"),
            Partition("p2", 40, KEY_SPACE_END, "server2"),
        ])

    def test_initial_ownership(self):
        table = self.manager.get_map()

        self.assertEqual(table.epoch, 1)
        self.assertEqual(
            table.split_keys([10, 50])[0].owner,
            "server1",
        )
        self.assertEqual(
            table.split_keys([10, 50])[1].owner,
            "server2",
        )

    def test_new_map_increments_epoch(self):
        old_table = self.manager.get_map()

        epoch = self.manager.publish_map([
            Partition("p1", 0, 40, "server1"),
            Partition("p2", 40, KEY_SPACE_END, "server3"),
        ])

        new_table = self.manager.get_map()

        self.assertEqual(epoch, 2)
        self.assertEqual(new_table.epoch, 2)

        # Previously cached snapshots remain unchanged.
        self.assertEqual(
            old_table.split_keys([50])[0].owner,
            "server2",
        )
        self.assertEqual(
            new_table.split_keys([50])[0].owner,
            "server3",
        )

    def test_invalid_map_not_published(self):
        with self.assertRaises(ValueError):
            self.manager.publish_map([
                Partition("p1", 0, 40, "server1"),
                Partition("p2", 41, KEY_SPACE_END, "server2"),
            ])

        # Invalid publication must not modify the map.
        self.assertEqual(self.manager.get_map().epoch, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
