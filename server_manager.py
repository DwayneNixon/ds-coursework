
import threading

from routing import RoutingTable
from consistency import ConsistencyGate

class ServerManager:
    """Stores the authoritative partition ownership map."""

    def __init__(self, partitions):
        self._lock = threading.Lock()
        self._table = RoutingTable(partitions, epoch=1)
        self.consistency_gate = None

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