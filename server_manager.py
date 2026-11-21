import threading

from routing import Partition, RoutingTable
from consistency import ConsistencyGate

class ServerManager:
    """Stores the authoritative partition ownership map."""

    def __init__(self, partitions):
        self._lock = threading.Lock()
        self._table = RoutingTable(partitions, epoch=1)
        self.consistency_gate = None
        self._migration_lock = threading.Lock()

    def get_map(self):
        """Return a snapshot of the current ownership map."""
        with self._lock:
            return RoutingTable(
                list(self._table.partitions),
                epoch=self._table.epoch,
            )

    def publish_map(self, partitions):
        """Validate and publish a newer ownership map."""
        with self._lock:
            new_table = RoutingTable(
                partitions,
                epoch=self._table.epoch + 1,
            )

            self._table = new_table
            return new_table.epoch
        
    def configure_consistency(self, workers, mode="sequential", tau=0):
        """Configure worker coordination before training starts."""

        gate = ConsistencyGate(
            workers,
            mode=mode,
            tau=tau,
        )

        with self._lock:
            if self.consistency_gate is not None:
                raise RuntimeError(
                    "Consistency has already been configured"
                )

            self.consistency_gate = gate

    def move_range(
        self,
        source,
        destination,
        destination_address,
        begin,
        end,
    ):
        """Move one interval between live server objects.

        Intended for the in-process coursework test harness.
        Clients continue communicating with servers over gRPC.
        """

        with self._migration_lock:
            current = self.get_map()

            # Servers not involved in an earlier migration
            # may still hold an older ownership epoch.
            # Synchronize the participating servers before
            # starting another transfer.
            source.set_ownership(source.owner, current)
            destination.set_ownership(
                destination_address, current
            )

            if (
                type(begin) is not int
                or type(end) is not int
                or not 0 <= begin < end <= (1 << 64)
            ):
                raise ValueError("Invalid migration interval")

            # The interval must be inside one existing partition.
            matches = [
                part for part in current.partitions
                if part.begin <= begin < end <= part.end
            ]

            if len(matches) != 1:
                raise ValueError(
                    "Migration must stay inside one partition"
                )

            original = matches[0]

            if (
                source is destination
                or source.owner != original.owner
                or destination.owner != destination_address
                or original.owner == destination_address
            ):
                raise ValueError("Invalid migration source or destination")

            next_epoch = current.epoch + 1

            # Split the original partition into up to three ranges:
            # retained-left, transferred, retained-right.
            new_partitions = []

            for part in current.partitions:
                if part != original:
                    new_partitions.append(part)
                    continue

                if part.begin < begin:
                    new_partitions.append(
                        Partition(
                            f"{part.partition_id}_left_e{next_epoch}",
                            part.begin,
                            begin,
                            part.owner,
                        )
                    )

                moved_id = (
                    f"{part.partition_id}_moved_e{next_epoch}"
                )

                new_partitions.append(
                    Partition(
                        moved_id,
                        begin,
                        end,
                        destination_address,
                    )
                )

                if end < part.end:
                    new_partitions.append(
                        Partition(
                            f"{part.partition_id}_right_e{next_epoch}",
                            end,
                            part.end,
                            part.owner,
                        )
                    )

            # Validate complete, non-overlapping coverage before
            # touching any server.
            new_table = RoutingTable(
                new_partitions,
                epoch=next_epoch,
            )

            published = False

            # Phase 1: source remains live while state is copied.
            snapshot = source.begin_range_transfer(begin, end)

            try:
                destination.stage_incoming_transfer(
                    moved_id, snapshot
                )

                # Phase 2: briefly fence the moving range.
                delta = source.freeze_range_transfer()

                destination.apply_incoming_delta(delta)
                destination.install_incoming_transfer()

                # Prepare the destination before advertising it.
                destination.set_ownership(
                    destination_address,
                    new_table,
                )

                # Clients may now discover the new owner.
                published_epoch = self.publish_map(
                    new_partitions
                )
                published = True

                if published_epoch != next_epoch:
                    raise RuntimeError(
                        "Unexpected published ownership epoch"
                    )

                # Change the source map and release waiting requests.
                source.finish_outgoing_transfer(new_table)

                return new_table

            except Exception:
                # Safe rollback for failures before publication
                # while only staged state exists.
                if not published:
                    if source._transfer is not None:
                        source.cancel_range_transfer()

                    if destination._incoming_transfer is not None:
                        destination.cancel_incoming_transfer()

                # Recovery after installed state or publication
                # is deliberately outside this initial coursework
                # implementation.
                raise

    def remove_server(
        self,
        source,
        destination,
        destination_address,
    ):
        """Gracefully transfer all source partitions to a destination.

        The caller may stop the source's gRPC server after this
        method returns successfully.
        """

        source_address = source.owner

        if (
            source is destination
            or source_address is None
            or destination.owner != destination_address
            or source_address == destination_address
        ):
            raise ValueError(
                "Invalid source or destination for server removal"
            )

        # Snapshot the list of partitions to migrate.
        current = self.get_map()

        owned_partitions = [
            partition
            for partition in current.partitions
            if partition.owner == source_address
        ]

        if not owned_partitions:
            raise ValueError(
                "Source server does not own any partitions"
            )

        # Reuse E1's tested two-phase migration protocol.
        # Each call updates the ownership epoch and safely
        # transfers one partition.
        for partition in owned_partitions:
            self.move_range(
                source=source,
                destination=destination,
                destination_address=destination_address,
                begin=partition.begin,
                end=partition.end,
            )

        # Ensure no ownership remains with the departing server.
        final_table = self.get_map()

        if any(
            partition.owner == source_address
            for partition in final_table.partitions
        ):
            raise RuntimeError(
                "Server removal left some partitions behind"
            )

        return final_table
