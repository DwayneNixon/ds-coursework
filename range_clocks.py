
def clock_leq(a, b):
    """True if vector clock a is no later than b."""
    return all(
        counter <= b.get(worker_id, 0)
        for worker_id, counter in a.items()
    )


class RangeClocks:
    """Vector clocks for one server-owned partition."""

    def __init__(self, begin, end, initial=None):
        if not (0 <= begin < end <= (1 << 64)):
            raise ValueError("Invalid clock range")

        self.begin = begin
        self.end = end

        # Start with exactly one clock for the partition.
        self._segments = [
            (begin, end, dict(initial or {}))
        ]

    def snapshot(self):
        """Return independent copies of all clock segments."""
        return [
            (begin, end, clock.copy())
            for begin, end, clock in self._segments
        ]

    def count(self):
        return len(self._segments)

    def clock_for_key(self, key):
        """Find the vector clock associated with one key."""
        if not self.begin <= key < self.end:
            raise ValueError("Key outside clock partition")

        for begin, end, clock in self._segments:
            if begin <= key < end:
                return clock.copy()

        raise RuntimeError("Clock coverage is incomplete")

    def record(self, begin, end, worker_id, counter):
        """Record a worker counter over [begin, end)."""

        if not self.begin <= begin < end <= self.end:
            raise ValueError("Update outside clock partition")

        if (
            type(worker_id) is not int
            or not 0 <= worker_id < (1 << 32)
            or type(counter) is not int
            or counter < 1
        ):
            raise ValueError("Invalid worker ID or counter")

        updated = []

        for left, right, clock in self._segments:
            # This segment is outside the updated range.
            if right <= begin or left >= end:
                updated.append((left, right, clock))
                continue

            # Preserve the unaffected left portion.
            if left < begin:
                updated.append((left, begin, clock.copy()))

            # Record the newer worker counter.
            middle = clock.copy()
            middle[worker_id] = max(
                middle.get(worker_id, 0),
                counter,
            )

            updated.append((
                max(left, begin),
                min(right, end),
                middle,
            ))

            # Preserve the unaffected right portion.
            if right > end:
                updated.append((end, right, clock.copy()))

        self._segments = self._merge_adjacent(updated)

    def _merge_adjacent(self, segments):
        """Combine neighbouring intervals with identical clocks."""

        merged = []

        for begin, end, clock in segments:
            if (
                merged
                and merged[-1][1] == begin
                and merged[-1][2] == clock
            ):
                previous_begin, _, previous_clock = merged[-1]
                merged[-1] = (
                    previous_begin, end, previous_clock
                )
            else:
                merged.append((begin, end, clock))

        return merged
    
    def clone(self):
        """Create an independent copy of these clock segments."""
        copied = RangeClocks(self.begin, self.end)
        copied._segments = self.snapshot()
        return copied

    def extract(self, begin, end):
        """Return an independent clock state for an owned subrange."""

        if (
            type(begin) is not int
            or type(end) is not int
            or not self.begin <= begin < end <= self.end
        ):
            raise ValueError("Invalid clock extraction range")

        clipped = []

        for left, right, clock in self._segments:
            overlap_begin = max(left, begin)
            overlap_end = min(right, end)

            if overlap_begin < overlap_end:
                clipped.append((
                    overlap_begin,
                    overlap_end,
                    clock.copy(),
                ))

        return RangeClocks.from_segments(begin, end, clipped)

    @classmethod
    def from_segments(cls, begin, end, segments):
        """Restore clocks from validated, complete interval segments."""

        if (
            type(begin) is not int
            or type(end) is not int
            or not 0 <= begin < end <= (1 << 64)
        ):
            raise ValueError("Invalid restored clock range")

        copied = []
        expected_begin = begin

        for segment in segments:
            if (
                not isinstance(segment, (tuple, list))
                or len(segment) != 3
            ):
                raise ValueError("Invalid clock segment")

            left, right, clock = segment

            if (
                type(left) is not int
                or type(right) is not int
                or left != expected_begin
                or not left < right <= end
                or not isinstance(clock, dict)
            ):
                raise ValueError(
                    "Clock segments must cover the range exactly"
                )

            for worker_id, counter in clock.items():
                if (
                    type(worker_id) is not int
                    or not 0 <= worker_id < (1 << 32)
                    or type(counter) is not int
                    or not 1 <= counter < (1 << 64)
                ):
                    raise ValueError("Invalid worker clock entry")

            copied.append((left, right, clock.copy()))
            expected_begin = right

        if expected_begin != end:
            raise ValueError(
                "Clock snapshot does not cover the range"
            )

        restored = cls(begin, end)
        restored._segments = restored._merge_adjacent(copied)

        return restored
