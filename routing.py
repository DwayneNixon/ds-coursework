
from dataclasses import dataclass
from bisect import bisect_right

KEY_SPACE_END = 1 << 64


@dataclass(frozen=True)
class Partition:
    partition_id: str
    begin: int
    end: int
    owner: str


@dataclass(frozen=True)
class RoutedRange:
    partition_id: str
    owner: str
    begin: int
    end: int

@dataclass(frozen=True)
class RoutedKeys:
    partition_id: str
    owner: str
    keys: tuple
    positions: tuple

class RoutingTable:
    def __init__(self, partitions, epoch=1):
        if not isinstance(epoch, int) or epoch < 0:
            raise ValueError("Invalid ownership epoch")

        if not partitions:
            raise ValueError("At least one partition is required")

        self.epoch = epoch
        self.partitions = sorted(
            partitions, key=lambda p: p.begin
        )
        self._starts = [
            partition.begin
            for partition in self.partitions
        ]

        self._validate()

    def _validate(self):
        expected_begin = 0
        seen_ids = set()

        for partition in self.partitions:
            if not partition.partition_id or not partition.owner:
                raise ValueError("Missing partition ID or owner")

            if partition.partition_id in seen_ids:
                raise ValueError("Duplicate partition ID")

            if (
                not isinstance(partition.begin, int)
                or not isinstance(partition.end, int)
                or partition.begin < 0
                or partition.end > KEY_SPACE_END
                or partition.begin >= partition.end
            ):
                raise ValueError("Invalid partition boundaries")

            if partition.begin != expected_begin:
                raise ValueError(
                    "Ownership map has a gap or overlap"
                )

            seen_ids.add(partition.partition_id)
            expected_begin = partition.end

        if expected_begin != KEY_SPACE_END:
            raise ValueError(
                "Ownership map does not cover the key space"
            )

    def split_range(self, begin, end):
        if (
            not isinstance(begin, int)
            or not isinstance(end, int)
            or begin < 0
            or end > KEY_SPACE_END
            or begin > end
        ):
            raise ValueError("Invalid requested range")

        if begin == end:
            return []

        result = []

        for partition in self.partitions:
            sub_begin = max(begin, partition.begin)
            sub_end = min(end, partition.end)

            if sub_begin < sub_end:
                result.append(
                    RoutedRange(
                        partition_id=partition.partition_id,
                        owner=partition.owner,
                        begin=sub_begin,
                        end=sub_end,
                    )
                )

        return result

    def split_keys(self, keys):
        if not isinstance(keys, (list, tuple)):
            raise ValueError("Keys must be a list or tuple")

        key_groups = [[] for _ in self.partitions]
        position_groups = [[] for _ in self.partitions]

        seen = set()

        for position, key in enumerate(keys):
            if type(key) is not int or not (0 <= key < KEY_SPACE_END):
                raise ValueError("Invalid uint64 key")

            if key in seen:
                raise ValueError("Duplicate key")
            seen.add(key)

            index = bisect_right(self._starts, key) - 1

            key_groups[index].append(key)
            position_groups[index].append(position)

        result = []

        for index, partition in enumerate(self.partitions):
            if key_groups[index]:
                result.append(
                    RoutedKeys(
                        partition_id=partition.partition_id,
                        owner=partition.owner,
                        keys=tuple(key_groups[index]),
                        positions=tuple(position_groups[index]),
                    )
                )

        return result

    def owns(self, owner, target):
        """Check whether one server owns the entire target."""

        if isinstance(target, range):
            if target.step != 1:
                raise ValueError("Range step must be 1")

            routes = self.split_range(
                target.start, target.stop
            )
        else:
            routes = self.split_keys(target)

        return bool(routes) and all(
            route.owner == owner
            for route in routes
        )
    
def split_push(target, gradients, routes, width):
    """
    Divide flat gradients according to a routing plan.

    Returns a dictionary:
        route -> gradient values for that route
    """
    if type(width) is not int or width < 1:
        raise ValueError("Invalid vector width")

    if len(gradients) != len(target) * width:
        raise ValueError("Gradient length mismatch")

    chunks = {}

    for route in routes:
        if isinstance(route, RoutedRange):
            # Dense range: extract a continuous slice.
            start = (route.begin - target.start) * width
            end = (route.end - target.start) * width

            chunks[route] = list(gradients[start:end])

        elif isinstance(route, RoutedKeys):
            # Sparse keys: use the original key positions.
            values = []

            for position in route.positions:
                start = position * width
                end = start + width
                values.extend(gradients[start:end])

            chunks[route] = values

        else:
            raise ValueError("Unknown routing entry")

    return chunks

def merge_pull(target, routes, replies, width):
    """
    Merge flat values returned by several servers.

    replies is a dictionary:
        route -> returned values
    """
    if type(width) is not int or width < 1:
        raise ValueError("Invalid vector width")

    if set(replies) != set(routes):
        raise ValueError("Missing or unexpected server response")

    merged = [None] * (len(target) * width)

    for route in routes:
        values = replies[route]

        if isinstance(route, RoutedRange):
            count = route.end - route.begin

            if len(values) != count * width:
                raise ValueError("Incorrect response length")

            start = (route.begin - target.start) * width
            end = start + len(values)

            merged[start:end] = values

        elif isinstance(route, RoutedKeys):
            if len(values) != len(route.keys) * width:
                raise ValueError("Incorrect response length")

            for local_index, original_position in enumerate(route.positions):
                source = local_index * width
                destination = original_position * width

                merged[destination:destination + width] = (
                    values[source:source + width]
                )

        else:
            raise ValueError("Unknown routing entry")

    return merged




