
import unittest
from types import SimpleNamespace

import parameter_server_pb2 as pb

from parameter_client import Task
from coordinated_client import (
    CoordinatedClient,
    clock_covers_push,
)
from routing import KEY_SPACE_END


def segment(begin, end, worker_id=7, counter=3):
    result = pb.ClockSegment(
        partition_id="test",
        begin=begin,
        end=end,
    )
    result.entries.add(
        worker_id=worker_id,
        counter=counter,
    )
    return result


class TestClockEvidence(unittest.TestCase):

    def setUp(self):
        self.clocks = [
            segment(0, 40),
            segment(40, 100),
        ]

    def test_sparse_keys_across_partitions(self):
        self.assertTrue(
            clock_covers_push(
                [10, 50], self.clocks, 7, 3
            )
        )

    def test_dense_range_across_partitions(self):
        self.assertTrue(
            clock_covers_push(
                range(38, 42), self.clocks, 7, 3
            )
        )

    def test_missing_partition_fails(self):
        self.assertFalse(
            clock_covers_push(
                [10, 50], self.clocks[:1], 7, 3
            )
        )

    def test_older_clock_fails(self):
        self.assertFalse(
            clock_covers_push(
                [10, 50], self.clocks, 7, 4
            )
        )

    def test_wrong_worker_fails(self):
        self.assertFalse(
            clock_covers_push(
                [10, 50], self.clocks, 8, 3
            )
        )

    def test_maximum_key_boundary(self):
        last = pb.ClockSegment(
            partition_id="last",
            begin=KEY_SPACE_END - 2,
            end=0,
            end_is_keyspace_end=True,
        )
        last.entries.add(worker_id=7, counter=3)

        self.assertTrue(
            clock_covers_push(
                [KEY_SPACE_END - 1], [last], 7, 3
            )
        )

    def test_invalid_clock_does_not_report_completion(self):
        class ManagerThatMustNotBeCalled:
            def CompleteIteration(self, *args, **kwargs):
                raise AssertionError(
                    "Manager must not receive an invalid completion"
                )

        task = Task()
        task._future.set_result(
            SimpleNamespace(accepted=True, clocks=[])
        )

        # Construct a minimal client to test the callback
        # without making any network requests.
        client = CoordinatedClient.__new__(
            CoordinatedClient
        )
        client.worker_id = 7
        client.iteration = 1
        client._pending_push = task
        client._pending_target = [10]
        client.manager = ManagerThatMustNotBeCalled()

        with self.assertRaisesRegex(
            RuntimeError, "CLOCK_NOT_SATISFIED"
        ):
            client.iteration_finished(1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
