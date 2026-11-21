
import threading
import copy
from concurrent import futures
import math
import grpc
import hashlib
import time
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc
from range_clocks import RangeClocks
from routing import KEY_SPACE_END, RoutingTable
from collections import OrderedDict
from value_compression import compress_doubles, decompress_doubles

def sgd_update(weights, gradients, learning_rate):
    """Apply SGD to one parameter vector."""

    return [
        weight - learning_rate * gradient
        for weight, gradient in zip(weights, gradients)
    ]

class ParameterServer(pb_grpc.ParameterServerServicer):

    def __init__(self, value_width=2, max_values_per_request=200_000):
        if value_width < 1:
            raise ValueError("Vector width must be at least 1")
        if (
            type(max_values_per_request) is not int
            or max_values_per_request < value_width
        ):
            raise ValueError(
                "Request value limit must be an integer >= vector width"
            )

        # Prevent a single dense range from exhausting memory while keeping
        # a million+ key *model* possible via multiple bounded requests.
        self.max_values_per_request = max_values_per_request
        self.max_keys_per_request = max_values_per_request // value_width
        self.value_width = value_width
        self.weights = {}
        self.learning_rate = 0.1
        self.lock = threading.Lock()
        # Maps completed task IDs to request fingerprints.
        self.completed_pushes = {}

        # E1.2: Keep the affected interval for migration-safe deduplication.
        self.completed_push_bounds = {}

        # E1.2: At most one outgoing range transfer at a time.
        self._transfer = None

        # State received from another server, not yet activated.
        self._incoming_transfer = None

        # Track successful and failed tasks for dependencies.
        self.task_states = {}

        # Partition ID -> RangeClocks instance
        self.range_clocks = {}

        # Share the existing lock with dependency waiters.
        self.task_condition = threading.Condition(self.lock)

        # Prevent dependency waits from lasting indefinitely.
        self.dependency_timeout = 5.0

        # Available server-side update functions.
        self.update_rules = {
            "sgd": sgd_update
        }

        # SGD remains the default.
        self.active_update_rule = "sgd"

        # None means ownership enforcement is disabled.
        # This preserves the original Part A server behaviour.
        self.owner = None
        self.ownership_table = None

        # D1: Sparse key-list cache.
        self.key_cache = OrderedDict()
        self.max_cached_key_lists = 128

        # F2.1: Shadow copies of other servers' ranges. These MUST NOT be
        # inserted into self.weights or self.range_clocks until F3 promotion.
        # Keyed by (primary_address, partition_id).
        self._replica_ranges = {}

        # F2.2: Ordered, fail-closed replication is opt-in. The separate
        # serial lock prevents concurrent Pushes overtaking replication.
        # Do not hold self.lock while applying a record to another server:
        # mutual primary/backup pairs would otherwise deadlock.
        self._push_serial_lock = threading.Lock()
        self._live_replication_targets = {}
        self._replication_sequences = {}
        self._replication_faulted = set()
        self._replication_inflight = False

        # F2.3: A batch never becomes worker-visible before backup delivery.
        # A dedicated flusher gathers concurrent Pushes and commits the
        # entire successfully replicated group in one local critical section.
        self._batch_size = 1                  # 1 preserves F2.2 behaviour.
        self._batch_timeout_s = 0.05
        self._batch_condition = threading.Condition()
        self._batch_pending = []
        self._batch_stopping = False
        self._batch_thread = None
        self._batch_flushes = 0
        self._batch_committed_pushes = 0

        # F3.1: manager-coordinated, single-range promotion. A promoted
        # range remains write-fenced until redundancy is rebuilt in F3.2.
        self._pending_promotion = None
        self._completed_promotions = set()
        self._promoted_write_fenced = set()

        # F3.2: repair is a separately fenced, epoch-scoped reconfiguration.
        # A new Push cannot run while backup state is being re-seeded.
        self._repair_active_epoch = None
        self._repair_write_fenced = False
        self._repair_ready = False
        self._repair_target_ids = None
        self._repair_released_epochs = set()

        # F3.4: temporary write fence during replicated E1 cutover.
        self._migration_fenced = False
        self._unpublished_migration_install = None


    def export_replica_snapshot(self, partition_id):
        """Snapshot a primary range under the same lock used by Push.

        Initial seeding must occur before live writes until F2.2 adds
        ordered incremental replication/catch-up. No transfer is started.
        """
        if not isinstance(partition_id, str) or not partition_id:
            raise ValueError("Partition ID must be nonempty")

        with self.lock:
            if self.ownership_table is None or self.owner is None:
                raise RuntimeError("Primary ownership must be configured")

            partition = next(
                (p for p in self.ownership_table.partitions
                 if p.partition_id == partition_id), None,
            )
            if partition is None or partition.owner != self.owner:
                raise ValueError("Cannot snapshot a range not owned by primary")
            if self._transfer is not None:
                begin, end = self._transfer["begin"], self._transfer["end"]
                if partition.begin < end and begin < partition.end:
                    raise RuntimeError("Cannot seed while range is migrating")

            begin, end = partition.begin, partition.end
            completed = {
                task_id: (fingerprint, self.completed_push_bounds[task_id])
                for task_id, fingerprint in self.completed_pushes.items()
                if task_id in self.completed_push_bounds
                and self._task_within_transfer(task_id, begin, end)
            }
            return {
                "primary_id": self.owner,
                "partition_id": partition_id,
                "epoch": self.ownership_table.epoch,
                "begin": begin,
                "end": end,
                "value_width": self.value_width,
                "learning_rate": self.learning_rate,
                "active_rule": self.active_update_rule,
                "weights": {
                    key: list(vector)
                    for key, vector in self.weights.items()
                    if begin <= key < end
                },
                "clock_segments": self.range_clocks[partition_id].snapshot(),
                "completed_pushes": completed,
            }

    def install_replica_snapshot(self, snapshot, *, allow_replace=False):
        """Install a shadow replica; replacement is fenced F3.2 repair only."""
        if not isinstance(snapshot, dict):
            raise ValueError("Replica snapshot must be a dictionary")
        primary_id = snapshot.get("primary_id")
        partition_id = snapshot.get("partition_id")
        epoch = snapshot.get("epoch")
        begin = snapshot.get("begin")
        end = snapshot.get("end")
        if (not isinstance(primary_id, str) or not primary_id
                or not isinstance(partition_id, str) or not partition_id
                or type(epoch) is not int or epoch < 1
                or type(begin) is not int or type(end) is not int
                or not 0 <= begin < end <= KEY_SPACE_END):
            raise ValueError("Invalid replica identity or boundaries")
        if type(snapshot.get("value_width")) is not int or (
            snapshot["value_width"] != self.value_width
        ):
            raise ValueError("Replica value width mismatch")
        rate = snapshot.get("learning_rate")
        if (isinstance(rate, bool) or not isinstance(rate, (int, float))
                or not math.isfinite(rate)):
            raise ValueError("Invalid replica learning rate")
        active_rule = snapshot.get("active_rule")
        if not isinstance(active_rule, str) or not active_rule:
            raise ValueError("Invalid replica update rule")

        # Validates sparse keys, values, clocks, and dedup records using
        # the same parser already tested for E1 destination transfers.
        weights, clocks, completed = self._copy_transfer_data(
            snapshot, begin, end
        )

        with self.lock:
            if self.ownership_table is None or self.owner is None:
                raise RuntimeError("Backup ownership must be configured")
            if epoch != self.ownership_table.epoch:
                raise ValueError("Replica snapshot has a stale map epoch")
            partition = next(
                (p for p in self.ownership_table.partitions
                 if p.partition_id == partition_id), None,
            )
            if (partition is None or partition.owner != primary_id
                    or (partition.begin, partition.end) != (begin, end)
                    or primary_id == self.owner):
                raise ValueError("Replica does not match a remote primary range")
            if active_rule not in self.update_rules:
                raise ValueError("Backup is missing the primary's update rule")

            replica_key = (primary_id, partition_id)
            if allow_replace:
                if (self._repair_active_epoch != epoch
                        or not self._repair_write_fenced):
                    raise RuntimeError("Replica replacement requires active repair")
            elif replica_key in self._replica_ranges:
                raise RuntimeError("Replica seed already installed")

            prepared = {
                "primary_id": primary_id,
                "partition_id": partition_id,
                "epoch": epoch,
                "begin": begin,
                "end": end,
                "value_width": self.value_width,
                "learning_rate": float(rate),
                "active_rule": active_rule,
                "weights": weights,
                "clock_segments": clocks.snapshot(),
                "completed_pushes": completed,
                "sequence": 0,
            }

            if allow_replace and replica_key in self._replica_ranges:
                existing = self._replica_ranges[replica_key]
                if existing["epoch"] == epoch:
                    # A lost repair-seed RPC reply may be retried. Once
                    # the repair is ready, only an identical seed is legal.
                    if existing == prepared:
                        return False
                    raise RuntimeError("Conflicting replica seed in repair epoch")
                if existing["epoch"] > epoch:
                    raise RuntimeError("Cannot replace a newer replica")
            self._replica_ranges[replica_key] = prepared
            return True

    def install_repair_replica_snapshot(self, snapshot):
        """F3.2 manager-compatible entrypoint for direct or RPC backups."""
        return self.install_replica_snapshot(snapshot, allow_replace=True)

    def get_replica_snapshot(self, primary_id, partition_id):
        """Return a detached shadow-state snapshot for tests and F3."""
        with self.lock:
            return copy.deepcopy(
                self._replica_ranges[(primary_id, partition_id)]
            )

    def prepare_replica_promotion(self, failed_primary, partition_id, new_table):
        """Stage one surviving shadow replica, without exposing it to clients.

        The manager MUST confirm a crash (rather than simply a heartbeat
        timeout) before beginning this F3.1 crash-stop promotion. Old and new
        maps must differ only in the owner of one unchanged partition.
        """
        if not isinstance(new_table, RoutingTable):
            raise TypeError("Promotion requires a RoutingTable")
        if (not isinstance(failed_primary, str) or not failed_primary
                or not isinstance(partition_id, str) or not partition_id):
            raise ValueError("Invalid promotion identifiers")

        with self._push_serial_lock:
            with self.lock:
                if self.owner is None or self.ownership_table is None:
                    raise RuntimeError("Backup ownership is not configured")
                old_table = self.ownership_table
                token = (failed_primary, partition_id, new_table.epoch)
                if self._pending_promotion is not None:
                    pending = self._pending_promotion
                    if pending["token"] == token and pending["table"].partitions == new_table.partitions:
                        return {"prepared": True, "sequence": pending["replica"]["sequence"]}
                    raise RuntimeError("Another promotion is pending")
                if token in self._completed_promotions:
                    return {"prepared": True, "sequence": self._replication_sequences[partition_id]}
                if new_table.epoch != old_table.epoch + 1:
                    raise ValueError("Promotion must advance ownership epoch once")
                old_parts = {part.partition_id: part for part in old_table.partitions}
                new_parts = {part.partition_id: part for part in new_table.partitions}
                if old_parts.keys() != new_parts.keys():
                    raise ValueError("Promotion may not split or merge partitions")
                for pid, old in old_parts.items():
                    proposed = new_parts[pid]
                    if (proposed.begin, proposed.end) != (old.begin, old.end):
                        raise ValueError("Promotion cannot change range boundaries")
                    owner = self.owner if pid == partition_id else old.owner
                    if proposed.owner != owner:
                        raise ValueError("Promotion changed unrelated ownership")
                old = old_parts.get(partition_id)
                if old is None or old.owner != failed_primary or failed_primary == self.owner:
                    raise ValueError("Range must currently belong to failed primary")
                replica = self._replica_ranges.get((failed_primary, partition_id))
                if replica is None or replica["epoch"] != old_table.epoch:
                    raise RuntimeError("No current shadow replica is available")
                if (replica["begin"], replica["end"]) != (old.begin, old.end):
                    raise ValueError("Replica boundaries do not match ownership")
                if replica["active_rule"] != self.active_update_rule or replica["learning_rate"] != self.learning_rate:
                    raise RuntimeError("Replica update rule or learning rate differs")
                if (self._transfer is not None or self._incoming_transfer is not None
                        or self._replication_inflight):
                    raise RuntimeError("Cannot promote during transfer or replication")
                if (partition_id in self.range_clocks
                        or any(old.begin <= key < old.end for key in self.weights)):
                    raise RuntimeError("Promoted state conflicts with local primary state")
                if any(task in self.task_states or task in self.completed_pushes
                       for task in replica["completed_pushes"]):
                    raise RuntimeError("Promoted Task ID conflicts with local history")

                # Validate clocks again, and detach all staged data before
                # returning. Prepare leaves the worker-visible state unchanged.
                clock = RangeClocks.from_segments(
                    old.begin, old.end, replica["clock_segments"]
                )
                self._pending_promotion = {
                    "token": token,
                    "table": new_table,
                    "replica": copy.deepcopy(replica),
                    "clock": clock,
                }
                return {"prepared": True, "sequence": replica["sequence"]}

    def cancel_replica_promotion(self, failed_primary, partition_id, epoch):
        """Cancel a prepared promotion only BEFORE manager publication."""
        with self._push_serial_lock:
            with self.lock:
                token = (failed_primary, partition_id, epoch)
                if self._pending_promotion is None:
                    return False
                if self._pending_promotion["token"] != token:
                    raise ValueError("Promotion token mismatch")
                self._pending_promotion = None
                return True

    def commit_replica_promotion(self, failed_primary, partition_id, epoch):
        """Install a prepared replica after manager publishes its new map.

        Idempotent for a lost RPC reply. Newly promoted keys can be Pulled,
        and exact completed Push retries are recognized, but NEW Pushes are
        fenced until F3.2 restores acknowledged replication.
        """
        token = (failed_primary, partition_id, epoch)
        with self._push_serial_lock:
            with self.task_condition:
                if token in self._completed_promotions:
                    return True
                pending = self._pending_promotion
                if pending is None or pending["token"] != token:
                    raise RuntimeError("No matching prepared promotion")
                replica = pending["replica"]
                table = pending["table"]
                self.weights.update(copy.deepcopy(replica["weights"]))
                self.range_clocks[partition_id] = pending["clock"]
                for task_id, (fingerprint, bounds) in replica["completed_pushes"].items():
                    self.completed_pushes[task_id] = fingerprint
                    self.completed_push_bounds[task_id] = bounds
                    self.task_states[task_id] = "DONE"
                self._replication_sequences[partition_id] = replica["sequence"]

                # The previous replication plan is stale. Do NOT allow an
                # unreplicated new write on either the promoted range or
                # previously owned ranges until backups are reconfigured.
                self._replication_faulted.update(self._live_replication_targets)
                self._replication_faulted.add(partition_id)
                self._promoted_write_fenced.add(partition_id)
                self.ownership_table = table
                self.key_cache.clear()
                self._replica_ranges.pop((failed_primary, partition_id))
                self._pending_promotion = None
                self._completed_promotions.add(token)
                self.task_condition.notify_all()
                return True

    def begin_replication_repair(self, new_table):
        """Fence *all* local writes before new-epoch replica seeding.

        Called on every surviving process. A promoted server already has
        this epoch; unaffected owners may still have the preceding epoch.
        No new primary data is created by this method.
        """
        if not isinstance(new_table, RoutingTable):
            raise TypeError("Repair requires a RoutingTable")
        with self._push_serial_lock:
            with self.task_condition:
                if self.ownership_table is None or self.owner is None:
                    raise RuntimeError("Server ownership must be configured")
                if self._repair_active_epoch is not None:
                    if (self._repair_active_epoch == new_table.epoch
                            and self.ownership_table.partitions == new_table.partitions):
                        return False
                    raise RuntimeError("Different replication repair already active")
                if new_table.epoch in self._repair_released_epochs:
                    return False
                old = self.ownership_table
                if old.epoch not in (new_table.epoch - 1, new_table.epoch):
                    raise ValueError("Repair must use the current or next map epoch")
                original = {p.partition_id: p for p in old.partitions}
                replacement = {p.partition_id: p for p in new_table.partitions}
                if original.keys() != replacement.keys():
                    raise ValueError("Repair cannot change partition IDs")
                for part_id, previous in original.items():
                    proposed = replacement[part_id]
                    if (previous.begin, previous.end) != (proposed.begin, proposed.end):
                        raise ValueError("Repair cannot change range boundaries")
                    if previous.owner == self.owner and proposed.owner != self.owner:
                        raise ValueError("Live primary cannot lose a range in repair")
                    if proposed.owner == self.owner and part_id not in self.range_clocks:
                        raise RuntimeError("Repair cannot invent missing primary state")
                if old.epoch == new_table.epoch and old.partitions != new_table.partitions:
                    raise ValueError("Ownership changed without an epoch advance")
                if self._pending_promotion is not None or self._transfer is not None or self._incoming_transfer is not None:
                    raise RuntimeError("Cannot repair during promotion or migration")
                if self._replication_inflight:
                    raise RuntimeError("Cannot repair during replication")

                self._repair_active_epoch = new_table.epoch
                self._repair_write_fenced = True
                self._repair_ready = False
                self._repair_target_ids = None
                self._replication_faulted.update(
                    part.partition_id for part in new_table.partitions
                    if part.owner == self.owner
                )
                if old.epoch != new_table.epoch:
                    self.ownership_table = new_table
                    self.key_cache.clear()
                self.task_condition.notify_all()
                return True

    def prepare_repaired_replication(
        self, targets, epoch, batch_size=1, batch_timeout_s=0.05,
    ):
        """Verify the new shadows, but KEEP local Pushes write-fenced.

        Manager must first fence all survivors, then seed the new epoch.
        Repeated calls are safe if the plan and settings are identical.
        """
        if not isinstance(targets, dict):
            raise TypeError("Repair targets must be a dictionary")
        if type(epoch) is not int or epoch < 1:
            raise ValueError("Invalid repair epoch")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if (type(batch_timeout_s) not in (int, float)
                or not math.isfinite(batch_timeout_s) or batch_timeout_s <= 0):
            raise ValueError("batch_timeout_s must be positive and finite")

        with self._push_serial_lock:
            with self.lock:
                if self._repair_active_epoch != epoch or not self._repair_write_fenced:
                    raise RuntimeError("No active fenced repair for this epoch")
                owned = {p.partition_id: p for p in self.ownership_table.partitions
                         if p.owner == self.owner}
                if set(targets) != set(owned):
                    raise ValueError("Repair targets must cover all owned partitions")
                if (self._batch_thread is not None and self._batch_thread.is_alive()
                        and batch_size == 1):
                    raise ValueError("Cannot switch off an active batcher during repair")

                identities = {}
                verified = {}
                for part_id, partition in owned.items():
                    backups = targets[part_id]
                    if not isinstance(backups, (tuple, list)) or not backups:
                        raise ValueError("Every repaired range requires a backup")
                    ids = tuple(getattr(b, "owner", None) for b in backups)
                    if (any(not isinstance(x, str) or not x or x == self.owner for x in ids)
                            or len(set(ids)) != len(ids)):
                        raise ValueError("Invalid or duplicate repair backup")
                    current = self.export_replica_snapshot_unlocked(part_id)
                    for backup in backups:
                        snapshot = backup.get_replica_snapshot(self.owner, part_id)
                        if (snapshot["epoch"] != epoch
                                or snapshot["sequence"] != 0
                                or snapshot["primary_id"] != self.owner
                                or snapshot["partition_id"] != part_id
                                or (snapshot["begin"], snapshot["end"]) != (partition.begin, partition.end)
                                or snapshot["value_width"] != self.value_width
                                or snapshot["learning_rate"] != self.learning_rate
                                or snapshot["active_rule"] != self.active_update_rule
                                or snapshot["weights"] != current["weights"]
                                or snapshot["clock_segments"] != current["clock_segments"]
                                or snapshot["completed_pushes"] != current["completed_pushes"]):
                            raise RuntimeError("Backup seed does not match primary")
                    identities[part_id] = ids
                    verified[part_id] = tuple(backups)

                signature = (identities, batch_size, float(batch_timeout_s))
                if self._repair_ready:
                    if signature != self._repair_target_ids:
                        raise RuntimeError("Conflicting prepared repair configuration")
                    return False
                self._repair_target_ids = signature
                self._live_replication_targets = verified
                # Old sequences are represented in the seeded snapshots;
                # the new epoch starts a fresh, unambiguous sequence space.
                self._replication_sequences = {pid: 0 for pid in owned}
                self._batch_size = batch_size
                self._batch_timeout_s = float(batch_timeout_s)
                self._repair_ready = True
                return True

    def release_repaired_replication(self, epoch):
        """Release the write fence only after ALL survivors were prepared."""
        with self._push_serial_lock:
            with self.task_condition:
                if epoch in self._repair_released_epochs:
                    return False
                if (self._repair_active_epoch != epoch or not self._repair_ready
                        or not self._repair_write_fenced):
                    raise RuntimeError("Cannot release an unprepared repair")
                owned = {p.partition_id for p in self.ownership_table.partitions
                         if p.owner == self.owner}
                self._replication_faulted.difference_update(owned)
                self._promoted_write_fenced.difference_update(owned)
                self._repair_write_fenced = False
                self._migration_fenced = False
                self._repair_active_epoch = None
                self._repair_ready = False
                self._repair_target_ids = None
                self._repair_released_epochs.add(epoch)
                if owned and self._batch_size > 1 and (
                    self._batch_thread is None or not self._batch_thread.is_alive()
                ):
                    self._batch_stopping = False
                    self._batch_thread = threading.Thread(
                        target=self._batch_loop,
                        name=f"replica-batcher-{self.owner}",
                        daemon=True,
                    )
                    self._batch_thread.start()
                self.task_condition.notify_all()
                return True

    def configure_live_replication(self, targets, batch_size=1, batch_timeout_s=0.05):
        """Enable ordered replication after F2.1 seeding, before training.

        targets maps *every* locally owned partition ID to a nonempty
        tuple/list of distinct live backup ParameterServer objects.
        Coordination remains in-process until a later transport milestone.
        """
        if not isinstance(targets, dict):
            raise TypeError("Replica targets must be a dictionary")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if (isinstance(batch_timeout_s, bool)
                or not isinstance(batch_timeout_s, (int, float))
                or not math.isfinite(batch_timeout_s)
                or batch_timeout_s <= 0):
            raise ValueError("batch_timeout_s must be positive and finite")

        with self._push_serial_lock:
            with self.lock:
                if self._live_replication_targets:
                    raise RuntimeError("Live replication already enabled")
                if self.ownership_table is None or self.owner is None:
                    raise RuntimeError("Primary ownership must be configured")
                if self._transfer is not None or self._incoming_transfer is not None:
                    raise RuntimeError("Cannot activate replication during migration")

                owned = {
                    p.partition_id: p for p in self.ownership_table.partitions
                    if p.owner == self.owner
                }
                if not owned or set(targets) != set(owned):
                    raise ValueError("Configure a backup for every owned partition")

                verified = {}
                for part_id, partition in owned.items():
                    backups = targets[part_id]
                    if not isinstance(backups, (list, tuple)) or not backups:
                        raise ValueError("Each range requires at least one backup")
                    if any(backup is self for backup in backups):
                        raise ValueError("Primary cannot back up itself")
                    if len({id(backup) for backup in backups}) != len(backups):
                        raise ValueError("Duplicate backup object")

                    # Seeding is a quiescent setup operation. Any later
                    # concurrent Push runs under _push_serial_lock.
                    primary_snapshot = self.export_replica_snapshot_unlocked(part_id)
                    for backup in backups:
                        snapshot = backup.get_replica_snapshot(self.owner, part_id)
                        if (snapshot["sequence"] != 0
                                or snapshot["epoch"] != self.ownership_table.epoch
                                or snapshot["primary_id"] != self.owner
                                or snapshot["partition_id"] != part_id
                                or snapshot["value_width"] != self.value_width
                                or snapshot["learning_rate"] != self.learning_rate
                                or snapshot["active_rule"] != self.active_update_rule
                                or (snapshot["begin"], snapshot["end"])
                                != (partition.begin, partition.end)
                                or snapshot["weights"] != primary_snapshot["weights"]
                                or snapshot["clock_segments"] != primary_snapshot["clock_segments"]
                                or snapshot["completed_pushes"] != primary_snapshot["completed_pushes"]):
                            raise ValueError("Replica seed does not match primary")
                    verified[part_id] = tuple(backups)

                self._replication_sequences = {part_id: 0 for part_id in verified}
                self._live_replication_targets = verified
                self._batch_size = batch_size
                self._batch_timeout_s = float(batch_timeout_s)

            # Start outside self.lock. The flusher takes self.lock while
            # preparing and committing a batch, but never during RPC delivery.
            if batch_size > 1:
                self._batch_thread = threading.Thread(
                    target=self._batch_loop,
                    name=f"replica-batcher-{self.owner}",
                    daemon=True,
                )
                self._batch_thread.start()

    def export_replica_snapshot_unlocked(self, partition_id):
        """Used only during initial configuration with the primary lock held."""
        # Mirrors export_replica_snapshot, without recursively taking lock.
        partition = next(
            p for p in self.ownership_table.partitions
            if p.partition_id == partition_id and p.owner == self.owner
        )
        begin, end = partition.begin, partition.end
        return {
            "weights": {key: list(vector) for key, vector in self.weights.items()
                        if begin <= key < end},
            "clock_segments": self.range_clocks[partition_id].snapshot(),
            "completed_pushes": {
                task_id: (fingerprint, self.completed_push_bounds[task_id])
                for task_id, fingerprint in self.completed_pushes.items()
                if task_id in self.completed_push_bounds
                and self._task_within_transfer(task_id, begin, end)
            },
        }

    def _apply_replica_update_locked(self, record, replica_ranges):
        """Validate and apply one record to a caller-owned replica map.

        Caller holds self.lock. The map can hold copies for atomic batches.
        """
        if not isinstance(record, dict):
            raise ValueError("Replica update must be a dictionary")

        primary_id = record.get("primary_id")
        partition_id = record.get("partition_id")
        epoch = record.get("epoch")
        sequence = record.get("sequence")
        task_id = record.get("task_id")
        fingerprint = record.get("fingerprint")
        bounds = record.get("bounds")
        weights = record.get("weights")
        clock_segments = record.get("clock_segments")

        if (not isinstance(primary_id, str) or not primary_id
                or not isinstance(partition_id, str) or not partition_id
                or type(epoch) is not int or epoch < 1
                or type(sequence) is not int or sequence < 1
                or not isinstance(task_id, tuple) or len(task_id) != 2
                or type(task_id[0]) is not int or not 0 <= task_id[0] < (1 << 32)
                or type(task_id[1]) is not int or not 0 < task_id[1] < (1 << 64)
                or not isinstance(fingerprint, bytes) or len(fingerprint) != 32
                or not isinstance(bounds, tuple) or len(bounds) != 2
                or type(bounds[0]) is not int or type(bounds[1]) is not int
                or not isinstance(weights, dict) or not weights
                or not isinstance(clock_segments, (list, tuple))):
            raise ValueError("Invalid replica update metadata")

        pending = self._pending_promotion
        if pending is not None and pending["token"][:2] == (primary_id, partition_id):
            raise RuntimeError("Cannot update replica during prepared promotion")
        replica = replica_ranges.get((primary_id, partition_id))
        if replica is None:
            raise ValueError("Replica was not seeded")
        if (epoch != replica["epoch"] or self.ownership_table is None
                or epoch != self.ownership_table.epoch):
            raise ValueError("Stale replica epoch")
        begin, end = replica["begin"], replica["end"]
        if not begin <= bounds[0] < bounds[1] <= end:
            raise ValueError("Replica update bounds outside partition")

        # Replays are checked before advancing the sequence.
        if sequence == replica["sequence"]:
            if record == replica.get("last_record"):
                return False
            raise ValueError("Conflicting replica record replay")
        if sequence != replica["sequence"] + 1:
            raise ValueError("Out-of-order replica sequence")
        if task_id in replica["completed_pushes"]:
            raise ValueError("Conflicting replica Task ID")

        copied = {}
        for key, values in weights.items():
            if type(key) is not int or not begin <= key < end:
                raise ValueError("Replica key outside partition")
            if (not isinstance(values, (list, tuple))
                    or len(values) != self.value_width
                    or any(not isinstance(v, (int, float))
                           or isinstance(v, bool) or not math.isfinite(v)
                           for v in values)):
                raise ValueError("Invalid replicated parameter values")
            copied[key] = list(values)
        if min(copied) < bounds[0] or max(copied) >= bounds[1]:
            raise ValueError("Replica keys inconsistent with Push bounds")

        # Validate the full clock snapshot without touching live clocks.
        new_clocks = RangeClocks.from_segments(begin, end, clock_segments)

        replica["weights"].update(copied)
        replica["clock_segments"] = new_clocks.snapshot()
        replica["completed_pushes"][task_id] = (fingerprint, bounds)
        replica["sequence"] = sequence
        replica["last_record"] = copy.deepcopy(record)
        return True

    def apply_replica_update(self, record):
        """F2.2-compatible single-record replication, with replay checking."""
        with self.lock:
            return self._apply_replica_update_locked(record, self._replica_ranges)

    def apply_replica_batch(self, records):
        """Atomically store an ordered set of records on one backup.

        No partial batch is visible if any record is invalid. Distinct primary
        ranges may share this backup. The primary fences the affected ranges
        if a batch's acknowledgement is uncertain.
        """
        if not isinstance(records, (list, tuple)) or not records:
            raise ValueError("Replica batch must contain at least one record")

        with self.lock:
            staging = {}
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError("Invalid replica batch record")
                key = (record.get("primary_id"), record.get("partition_id"))
                if key not in self._replica_ranges:
                    raise ValueError("Replica was not seeded")
                if key not in staging:
                    staging[key] = copy.deepcopy(self._replica_ranges[key])

            for record in records:
                self._apply_replica_update_locked(record, staging)

            # Install every updated replica together under one backup lock.
            self._replica_ranges.update(staging)
        return True

    def live_replication_snapshot(self):
        """Inspect committed primary sequences and faulted ranges."""
        with self.lock:
            return {
                "enabled": bool(self._live_replication_targets),
                "sequences": dict(self._replication_sequences),
                "faulted_partitions": tuple(sorted(self._replication_faulted)),
                "batch_size": self._batch_size,
                "batch_timeout_s": self._batch_timeout_s,
                "batch_flushes": self._batch_flushes,
                "batch_committed_pushes": self._batch_committed_pushes,
                "repair_epoch": self._repair_active_epoch,
                "repair_write_fenced": self._repair_write_fenced,
                "repair_ready": self._repair_ready,
                "migration_fenced": self._migration_fenced,
            }

    def _replicate_prepared_push(self, targets, record, context):
        """Ship record while temporarily releasing primary's state lock.

        Caller holds both self.lock and _push_serial_lock. Any uncertain
        delivery FENCES this partition; a failed Push is never acknowledged.
        """
        self._replication_inflight = True
        self.lock.release()
        failure = None
        try:
            for backup in targets:
                backup.apply_replica_update(copy.deepcopy(record))
        except Exception as error:
            failure = error
        finally:
            self.lock.acquire()
            self._replication_inflight = False

        if failure is not None:
            self._replication_faulted.add(record["partition_id"])
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "REPLICATION_UNAVAILABLE: backup update not confirmed; "
                "range fenced pending repair",
            )

    def set_ownership(self, owner, table):
        """Install an ownership map for this server."""

        with self.lock:
            if self._replication_inflight:
                raise RuntimeError("Cannot change ownership during replication")
            if (self._live_replication_targets
                    and self.ownership_table is not None
                    and table.epoch != self.ownership_table.epoch):
                raise RuntimeError("Live replicas require reconfiguration before repartitioning")
            if self.ownership_table is not None:
                current = self.ownership_table

                if table.epoch < current.epoch:
                    raise ValueError("Cannot install an older map")

                if (
                    table.epoch == current.epoch
                    and table.partitions != current.partitions
                ):
                    raise ValueError(
                        "Ownership changed without a new epoch"
                    )
            
            # Prepare clocks for newly owned partitions.
            new_clocks = {}

            for partition in table.partitions:
                if partition.owner != owner:
                    continue

                existing = self.range_clocks.get(
                    partition.partition_id
                )

                if existing is None:
                    new_clocks[partition.partition_id] = RangeClocks(
                        partition.begin,
                        partition.end,
                    )

                elif (
                    existing.begin != partition.begin
                    or existing.end != partition.end
                ):
                    raise ValueError(
                        "Changing clock boundaries requires "
                        "a range transfer"
                    )

            self.range_clocks.update(new_clocks)

            if (
                self.ownership_table is None
                or table.epoch != self.ownership_table.epoch
            ):
                self.key_cache.clear()

            self.owner = owner
            self.ownership_table = table
            self.task_condition.notify_all()

    def begin_migration_fence(self, epoch):
        """F3.4: quiesce local Pushes at a known ownership epoch.

        The serial lock drains any ongoing acknowledged replication before
        the fence becomes visible. This is deliberately a short-pause
        coursework migration, not a nonblocking online write-transfer.
        """
        with self._push_serial_lock:
            with self.task_condition:
                if self.ownership_table is None or self.ownership_table.epoch != epoch:
                    raise ValueError("Cannot fence migration with stale ownership")
                if self._repair_write_fenced or self._pending_promotion is not None:
                    raise RuntimeError("Cannot migrate while failover is active")
                if self._replication_inflight or self._transfer is not None:
                    raise RuntimeError("Migration or replication already active")
                if self._migration_fenced:
                    return False
                self._migration_fenced = True
                self.task_condition.notify_all()
                return True

    def end_migration_fence(self, epoch):
        """Release a cancelled pre-publication migration, not a published one."""
        with self._push_serial_lock:
            with self.task_condition:
                if self.ownership_table is None or self.ownership_table.epoch != epoch:
                    raise RuntimeError("Cannot release after ownership publication")
                if self._transfer is not None or self._incoming_transfer is not None:
                    raise RuntimeError("Cannot release an unfinished transfer")
                if self._unpublished_migration_install is not None:
                    raise RuntimeError("Unpublished destination still installed")
                if self._repair_write_fenced or self._pending_promotion is not None:
                    raise RuntimeError("Recovery must finish before releasing")
                was_fenced = self._migration_fenced
                self._migration_fenced = False
                self.task_condition.notify_all()
                return was_fenced

    def begin_migration_epoch(self, new_table):
        """Install the published migration map while writes remain fenced.

        E1 has already installed destination weights and cleaned source state.
        Unaffected servers simply advance their map. Every server then joins
        the existing F3.2 snapshot/verification/release protocol.
        """
        if not isinstance(new_table, RoutingTable):
            raise TypeError("Migration epoch requires a RoutingTable")
        with self._push_serial_lock:
            with self.task_condition:
                if not self._migration_fenced:
                    raise RuntimeError("Migration epoch requires a write fence")
                if self._transfer is not None or self._incoming_transfer is not None:
                    raise RuntimeError("Cannot enter new epoch during transfer")
                if self._repair_active_epoch is not None:
                    if (self._repair_active_epoch == new_table.epoch
                            and self.ownership_table.partitions == new_table.partitions):
                        return False
                    raise RuntimeError("Conflicting migration repair")
                if (new_table.epoch not in (
                    self.ownership_table.epoch, self.ownership_table.epoch + 1
                )):
                    raise ValueError("Migration must advance by one epoch")
                if new_table.epoch == self.ownership_table.epoch and (
                    new_table.partitions != self.ownership_table.partitions
                ):
                    raise ValueError("Conflicting map in same epoch")
                for part in new_table.partitions:
                    if part.owner == self.owner:
                        clock = self.range_clocks.get(part.partition_id)
                        if (clock is None or clock.begin != part.begin
                                or clock.end != part.end):
                            raise RuntimeError("Missing transferred range clocks")
                if self._unpublished_migration_install is not None:
                    installed = self._unpublished_migration_install
                    if not any(p.partition_id == installed["partition_id"]
                               and p.owner == self.owner for p in new_table.partitions):
                        raise RuntimeError("Unpublished install not assigned here")
                    self._unpublished_migration_install = None
                self.ownership_table = new_table
                self.key_cache.clear()
                self._repair_active_epoch = new_table.epoch
                self._repair_write_fenced = True
                self._repair_ready = False
                self._repair_target_ids = None
                self._replication_faulted.update(
                    p.partition_id for p in new_table.partitions if p.owner == self.owner
                )
                self.task_condition.notify_all()
                return True

    def abort_unpublished_migration(self):
        """Discard only staged/unpublished E1 destination data.

        Never call this after the manager publishes the ownership epoch.
        Other keys and existing primary data are not changed.
        """
        with self._push_serial_lock:
            with self.task_condition:
                if self._incoming_transfer is not None:
                    self._incoming_transfer = None
                installed = self._unpublished_migration_install
                if installed is None:
                    return False
                if not self._migration_fenced:
                    raise RuntimeError("Unpublished rollback requires the write fence")
                if self.ownership_table.epoch != installed["epoch"]:
                    raise RuntimeError("Cannot roll back a published migration")
                for key in installed["weights"]:
                    self.weights.pop(key, None)
                self.range_clocks.pop(installed["partition_id"], None)
                for task_id in installed["completed"]:
                    self.completed_pushes.pop(task_id, None)
                    self.completed_push_bounds.pop(task_id, None)
                    self.task_states.pop(task_id, None)
                self._unpublished_migration_install = None
                self.task_condition.notify_all()
                return True

    def begin_range_transfer(self, begin, end):
        """Snapshot an owned interval while normal requests continue."""

        with self.lock:
            if self._live_replication_targets and not self._migration_fenced:
                raise RuntimeError("Live replication migration requires F3 coordination")
            if self._transfer is not None:
                raise RuntimeError("A range transfer is already active")

            if (
                type(begin) is not int
                or type(end) is not int
                or not 0 <= begin < end <= KEY_SPACE_END
            ):
                raise ValueError("Invalid transfer range")

            if self.ownership_table is None:
                raise RuntimeError("Ownership must be configured")

            # E1.2 supports an interval within one owned partition.
            partition = next(
                (
                    part for part in self.ownership_table.partitions
                    if part.owner == self.owner
                    and part.begin <= begin < end <= part.end
                ),
                None,
            )

            if partition is None:
                raise ValueError(
                    "Transfer must lie within one owned partition"
                )

            # Everything below is protected by the same lock as Push.
            self._transfer = {
                "begin": begin,
                "end": end,
                "partition_id": partition.partition_id,
                "dirty_keys": set(),
                "new_pushes": set(),
                "frozen": False,
            }

            weights = {
                key: vector.copy()
                for key, vector in self.weights.items()
                if begin <= key < end
            }

            clocks = self.range_clocks[
                partition.partition_id
            ].extract(begin, end).snapshot()

            # Only copy completed requests entirely inside the interval.
            completed = {
                task_id: (
                    fingerprint,
                    self.completed_push_bounds[task_id],
                )
                for task_id, fingerprint in self.completed_pushes.items()
                if task_id in self.completed_push_bounds
                and self._task_within_transfer(task_id, begin, end)
            }

            return {
                "begin": begin,
                "end": end,
                "weights": weights,
                "clock_segments": clocks,
                "completed_pushes": completed,
            }

    def _task_within_transfer(self, task_id, begin, end):
        """Whether a completed Push is wholly within an interval.

        Called with self.lock already held.
        """
        left, right = self.completed_push_bounds[task_id]
        return left < right and begin <= left and right <= end

    def freeze_range_transfer(self):
        """Fence the moving interval and capture changes since snapshot."""

        with self.task_condition:
            transfer = self._transfer

            if transfer is None or transfer["frozen"]:
                raise RuntimeError("No active copying transfer")

            transfer["frozen"] = True
            begin = transfer["begin"]
            end = transfer["end"]
            partition_id = transfer["partition_id"]

            completed = {
                task_id: (
                    self.completed_pushes[task_id],
                    self.completed_push_bounds[task_id],
                )
                for task_id in transfer["new_pushes"]
                if self._task_within_transfer(task_id, begin, end)
            }

            # Transfer final parameter values (after-images), not gradients.
            return {
                "begin": begin,
                "end": end,
                "weights": {
                    key: self.weights[key].copy()
                    for key in sorted(transfer["dirty_keys"])
                },
                "clock_segments": self.range_clocks[
                    partition_id
                ].extract(begin, end).snapshot(),
                "completed_pushes": completed,
            }

    def cancel_range_transfer_if_active(self):
        """Idempotent prepublication cancellation for gRPC retries."""
        with self.task_condition:
            if self._transfer is None:
                return False
            self._transfer = None
            self.task_condition.notify_all()
            return True

    def cancel_range_transfer(self):
        """Cancel an outgoing transfer and resume source requests."""

        with self.task_condition:
            if self._transfer is None:
                raise RuntimeError("No active transfer")

            self._transfer = None
            self.task_condition.notify_all()

    def finish_outgoing_transfer(self, new_table):
        """Switch source ownership and preserve retained clock history."""

        with self.task_condition:
            transfer = self._transfer

            if transfer is None or not transfer["frozen"]:
                raise RuntimeError(
                    "Source must be frozen before cutover"
                )

            if new_table.epoch != self.ownership_table.epoch + 1:
                raise ValueError("Incorrect ownership epoch")

            begin = transfer["begin"]
            end = transfer["end"]
            old_id = transfer["partition_id"]

            original = next(
                part for part in self.ownership_table.partitions
                if part.partition_id == old_id
            )

            # The moving range must belong to another server.
            moved = [
                part for part in new_table.partitions
                if part.begin == begin
                and part.end == end
                and part.owner != self.owner
            ]

            if len(moved) != 1:
                raise ValueError(
                    "New map must assign the moving range "
                    "to another server"
                )

            # Identify the portions that remain on this server.
            retained = [
                part for part in new_table.partitions
                if part.owner == self.owner
                and original.begin <= part.begin
                and part.end <= original.end
            ]

            retained.sort(key=lambda part: part.begin)

            expected = []

            if original.begin < begin:
                expected.append((original.begin, begin))

            if end < original.end:
                expected.append((end, original.end))

            actual = [
                (part.begin, part.end)
                for part in retained
            ]

            if actual != expected:
                raise ValueError(
                    "Retained source ranges do not match "
                    "the original partition"
                )

            # Other previously owned partitions must be unchanged.
            for part in self.ownership_table.partitions:
                if (
                    part.owner == self.owner
                    and part.partition_id != old_id
                    and part not in new_table.partitions
                ):
                    raise ValueError(
                        "Unrelated source partition was modified"
                    )

            # Crucial: preserve clocks for BOTH retained sides.
            original_clocks = self.range_clocks[old_id]

            retained_clocks = {
                part.partition_id: original_clocks.extract(
                    part.begin, part.end
                )
                for part in retained
            }

            self.range_clocks.update(retained_clocks)

            owned_ids = {
                part.partition_id
                for part in new_table.partitions
                if part.owner == self.owner
            }

            if old_id not in owned_ids:
                self.range_clocks.pop(old_id, None)

            # The destination now holds the transferred values.
            # The source must no longer store these parameters.
            for key in list(self.weights):
                if begin <= key < end:
                    del self.weights[key]

            # Existing deduplication records are retained for
            # exact historical retries.
            self.ownership_table = new_table
            self.key_cache.clear()

            # Release frozen requests. They will now check the
            # new ownership map and receive NOT_OWNER if moved.
            self._transfer = None
            self.task_condition.notify_all()

    def finish_outgoing_transfer_if_active(self, new_table):
        """Complete E1 cutover, safe to repeat after a lost RPC response."""
        with self.lock:
            if self._transfer is None:
                if (self._migration_fenced and self.ownership_table is not None
                        and self.ownership_table.epoch == new_table.epoch
                        and self.ownership_table.partitions == new_table.partitions):
                    return False
                raise RuntimeError("No matching migration source transfer")
        self.finish_outgoing_transfer(new_table)
        return True

    def _wait_for_transfer(self, keys):
        """Wait if the requested keys intersect a frozen transfer.

        The caller must hold self.lock (shared with task_condition).
        """
        while self._transfer is not None and self._transfer["frozen"]:
            begin = self._transfer["begin"]
            end = self._transfer["end"]

            if isinstance(keys, range):
                affected = keys.start < end and begin < keys.stop
            else:
                affected = any(begin <= key < end for key in keys)

            if not affected:
                return

            # Condition.wait releases self.lock while the request waits.
            self.task_condition.wait(timeout=0.1)

    def _copy_transfer_data(self, payload, begin, end):
        """Validate and copy incoming transfer data."""

        if not isinstance(payload, dict) or (
            payload.get("begin"), payload.get("end")
        ) != (begin, end):
            raise ValueError("Transfer boundaries do not match")

        clocks = RangeClocks.from_segments(
            begin, end, payload["clock_segments"]
        )

        weights = {}

        for key, vector in payload["weights"].items():
            if type(key) is not int or not begin <= key < end:
                raise ValueError("Transferred key outside interval")

            if len(vector) != self.value_width or any(
                not isinstance(v, (int, float))
                or isinstance(v, bool)
                or not math.isfinite(v)
                for v in vector
            ):
                raise ValueError("Invalid transferred parameter vector")

            weights[key] = list(vector)

        completed = {}

        for task_id, (fingerprint, bounds) in (
            payload["completed_pushes"].items()
        ):
            if (
                not isinstance(task_id, tuple)
                or len(task_id) != 2
                or type(task_id[0]) is not int
                or not 0 <= task_id[0] < (1 << 32)
                or type(task_id[1]) is not int
                or not 0 < task_id[1] < (1 << 64)
                or not isinstance(fingerprint, bytes)
                or len(fingerprint) != 32
                or not isinstance(bounds, tuple)
                or len(bounds) != 2
                or not begin <= bounds[0] < bounds[1] <= end
            ):
                raise ValueError("Invalid transferred Push record")

            completed[task_id] = (fingerprint, bounds)

        return weights, clocks, completed

    def stage_incoming_transfer(self, partition_id, snapshot):
        """Stage a snapshot without changing live server state."""

        if not isinstance(partition_id, str) or not partition_id:
            raise ValueError("Invalid destination partition ID")

        begin = snapshot["begin"]
        end = snapshot["end"]

        if (
            type(begin) is not int
            or type(end) is not int
            or not 0 <= begin < end <= KEY_SPACE_END
        ):
            raise ValueError("Invalid transfer interval")

        weights, clocks, completed = self._copy_transfer_data(
            snapshot, begin, end
        )

        with self.lock:
            if self._incoming_transfer is not None:
                raise RuntimeError(
                    "An incoming transfer is already staged"
                )

            if partition_id in self.range_clocks:
                raise ValueError("Partition ID already exists")

            if any(begin <= key < end for key in self.weights):
                raise ValueError(
                    "Destination already holds these keys"
                )

            if any(
                task_id in self.task_states
                or task_id in self.completed_pushes
                for task_id in completed
            ):
                raise ValueError("Destination Task ID collision")

            self._incoming_transfer = {
                "partition_id": partition_id,
                "begin": begin,
                "end": end,
                "weights": weights,
                "clocks": clocks,
                "completed": completed,
                "caught_up": False,
            }

    def apply_incoming_delta(self, delta):
        """Apply final changes after the source has been frozen."""

        with self.lock:
            stage = self._incoming_transfer

            if stage is None:
                raise RuntimeError("No staged incoming transfer")

            weights, clocks, completed = self._copy_transfer_data(
                delta,
                stage["begin"],
                stage["end"],
            )

            for task_id in completed:
                if (
                    task_id in self.task_states
                    or task_id in self.completed_pushes
                ):
                    raise ValueError("Destination Task ID collision")

                if task_id in stage["completed"]:
                    raise ValueError("Duplicate transferred Task ID")

            # Changed parameter values replace their snapshot values.
            stage["weights"].update(weights)

            # The delta contains the latest complete clock snapshot.
            stage["clocks"] = clocks

            # Preserve successful Push history for deduplication.
            stage["completed"].update(completed)

            stage["caught_up"] = True

    def install_incoming_transfer(self):
        """Install fully caught-up data before changing ownership."""

        with self.task_condition:
            stage = self._incoming_transfer

            if stage is None:
                raise RuntimeError("No staged incoming transfer")

            if not stage["caught_up"]:
                raise RuntimeError(
                    "Final transfer changes not applied"
                )

            begin = stage["begin"]
            end = stage["end"]

            if stage["partition_id"] in self.range_clocks:
                raise ValueError("Partition already installed")

            if any(begin <= key < end for key in self.weights):
                raise ValueError("Destination has conflicting keys")

            if any(
                task_id in self.task_states
                or task_id in self.completed_pushes
                for task_id in stage["completed"]
            ):
                raise ValueError(
                    "Destination has conflicting Task IDs"
                )

            # F3.4: only the data installed while fenced can be rolled back
            # if the source crashes before manager publication.
            if self._migration_fenced:
                self._unpublished_migration_install = {
                    "epoch": self.ownership_table.epoch,
                    "partition_id": stage["partition_id"],
                    "weights": tuple(stage["weights"]),
                    "completed": tuple(stage["completed"]),
                }

            # Install everything under the existing server lock.
            self.weights.update(stage["weights"])

            self.range_clocks[
                stage["partition_id"]
            ] = stage["clocks"]

            for task_id, (fingerprint, bounds) in (
                stage["completed"].items()
            ):
                self.completed_pushes[task_id] = fingerprint
                self.completed_push_bounds[task_id] = bounds
                self.task_states[task_id] = "DONE"

            self._incoming_transfer = None
            self.task_condition.notify_all()

    def cancel_incoming_transfer(self):
        """Discard staging without modifying live server state."""

        with self.lock:
            if self._incoming_transfer is None:
                raise RuntimeError("No staged incoming transfer")

            self._incoming_transfer = None

    def _check_ownership(self, keys, context):
        """Reject requests containing keys owned elsewhere."""

        if self.ownership_table is None:
            return

        if not self.ownership_table.owns(self.owner, keys):
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"NOT_OWNER: server={self.owner}, "
                f"epoch={self.ownership_table.epoch}",
            )

    
    def _resolve_keys(self, request, context):
        """Resolve dense, sparse, or cached sparse targets."""

        cache_id = bytes(request.key_cache_id)

        # A request that uses a previously registered key list.
        if request.use_cached_keys:
            if (
                request.HasField("key_range")
                or request.keys
                or len(cache_id) != 16
            ):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Cached requests require a 16-byte cache ID "
                    "and no explicit keys or range",
                )

            with self.lock:
                keys = self.key_cache.get(cache_id)

                if keys is None:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "KEY_CACHE_MISS: key list not available",
                    )

                self.key_cache.move_to_end(cache_id)
                return list(keys)

        # A complete sparse request can register a cache ID.
        if cache_id:
            if (
                len(cache_id) != 16
                or request.HasField("key_range")
                or not request.keys
            ):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Cache registration requires a 16-byte ID "
                    "and a nonempty sparse key list",
                )

        # Existing dense-range behaviour.
        if request.HasField("key_range"):
            if request.keys:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Specify either keys or key_range, not both",
                )

            begin = request.key_range.begin
            end = request.key_range.end

            if begin > end:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Range begin must not exceed end",
                )
            # Compare endpoints directly: len(range(...)) can overflow for
            # uint64 intervals larger than Py_ssize_t.
            if end - begin > self.max_keys_per_request:
                context.abort(
                    grpc.StatusCode.RESOURCE_EXHAUSTED,
                    "Request exceeds configured dense-range key limit",
                )

            return range(begin, end)

        # Existing sparse-key behaviour.
        keys = list(request.keys)

        if len(keys) > self.max_keys_per_request:
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "Request exceeds configured sparse-key limit",
            )

        if len(keys) != len(set(keys)):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Duplicate keys are not supported",
            )

        return keys

    def _register_key_cache(self, request, keys, context):
        """Register a sparse key list while holding self.lock."""

        if request.use_cached_keys or not request.key_cache_id:
            return

        cache_id = bytes(request.key_cache_id)
        key_list = tuple(keys)

        existing = self.key_cache.get(cache_id)

        if existing is not None and existing != key_list:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "KEY_CACHE_CONFLICT: ID already registered "
                "for different keys",
            )

        self.key_cache[cache_id] = key_list
        self.key_cache.move_to_end(cache_id)

        if len(self.key_cache) > self.max_cached_key_lists:
            self.key_cache.popitem(last=False)


    def _clock_requirements_satisfied(self, keys, required):
        """Check required worker counters on every requested key.

        Called while holding self.lock.
        """

        if isinstance(keys, range):
            routes = self.ownership_table.split_range(
                keys.start, keys.stop
            )
        else:
            routes = self.ownership_table.split_keys(keys)

        for route in routes:
            state = self.range_clocks.get(route.partition_id)

            if state is None:
                return False

            if isinstance(keys, range):
                # Check intervals without enumerating dense keys.
                position = route.begin

                for begin, end, clock in state.snapshot():
                    if end <= position:
                        continue

                    if begin >= route.end:
                        break

                    if begin > position:
                        return False

                    if any(
                        clock.get(worker, 0) < counter
                        for worker, counter in required.items()
                    ):
                        return False

                    position = min(end, route.end)

                    if position >= route.end:
                        break

                if position < route.end:
                    return False

            else:
                # Sparse requests check their individual keys.
                for key in route.keys:
                    clock = state.clock_for_key(key)

                    if any(
                        clock.get(worker, 0) < counter
                        for worker, counter in required.items()
                    ):
                        return False

        return True

    def _wait_for_clock_dependencies(self, request, keys, context):
        """Wait until all requested keys satisfy the clock."""

        entries = list(request.required_clock)

        if not entries:
            return

        required = {
            entry.worker_id: entry.counter
            for entry in entries
        }

        if (
            len(required) != len(entries)
            or any(entry.counter == 0 for entry in entries)
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Invalid clock dependency",
            )

        deadline = time.monotonic() + self.dependency_timeout

        # task_condition uses the same lock as the weights
        # and per-range clocks.
        with self.task_condition:
            if self.ownership_table is None:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "CLOCKS_DISABLED: ownership not configured",
                )

            while True:
                # Ownership might change while waiting.
                self._check_ownership(keys, context)

                if self._clock_requirements_satisfied(
                    keys, required
                ):
                    return

                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    context.abort(
                        grpc.StatusCode.DEADLINE_EXCEEDED,
                        "CLOCK_DEPENDENCY_TIMEOUT: "
                        "required updates not yet received",
                    )

                # Releases the shared lock while waiting.
                self.task_condition.wait(
                    timeout=min(remaining, 0.1)
                )


    def _wait_for_dependencies(self, request, context):
        """Wait for all prerequisite tasks to succeed."""

        dependencies = [
            (dep.worker_id, dep.sequence)
            for dep in request.dependencies
        ]

        if not dependencies:
            return

        if any(sequence == 0 for _, sequence in dependencies):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Dependency sequence must be positive"
            )

        if len(set(dependencies)) != len(dependencies):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Duplicate dependencies"
            )

        if request.HasField("task_id"):
            own_id = (
                request.task_id.worker_id,
                request.task_id.sequence
            )

            if own_id in dependencies:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "A task cannot depend on itself"
                )

        deadline = time.monotonic() + self.dependency_timeout

        with self.task_condition:
            while True:
                states = [
                    self.task_states.get(dep)
                    for dep in dependencies
                ]

                if "FAILED" in states:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "DEPENDENCY_FAILED: prerequisite failed"
                    )

                if all(state == "DONE" for state in states):
                    return

                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    context.abort(
                        grpc.StatusCode.DEADLINE_EXCEEDED,
                        "TIMEOUT: prerequisites did not complete"
                    )

                self.task_condition.wait(
                    timeout=min(remaining, 0.1)
                )


    def _finish_task(self, request, succeeded):
        """Record a task outcome and notify its dependents."""

        if not request.HasField("task_id"):
            return

        task_id = (
            request.task_id.worker_id,
            request.task_id.sequence
        )

        with self.task_condition:
            if succeeded:
                self.task_states[task_id] = "DONE"

            elif self.task_states.get(task_id) != "DONE":
                self.task_states[task_id] = "FAILED"

            self.task_condition.notify_all()


    def _prepare_push_clocks(self, request, task_key, keys):
        """Prepare clock changes without modifying stored clocks."""
        
        # Preserve compatibility with legacy clients and
        # standalone Part A servers.
        if task_key is None or self.ownership_table is None:
            return {}

        worker_id, sequence = task_key

        # Explicit training iteration takes precedence.
        # Older clients retain Task-sequence behaviour.
        clock_counter = (
            request.iteration
            if request.HasField("iteration")
            else sequence
        )

        # Use the resolved keys, including cached sparse keys.
        if isinstance(keys, range):
            routes = self.ownership_table.split_range(
                keys.start,
                keys.stop,
            )
        else:
            routes = self.ownership_table.split_keys(keys)

        prepared = {}

        for route in routes:
            partition_id = route.partition_id

            if partition_id not in prepared:
                original = self.range_clocks[partition_id]
                prepared[partition_id] = original.clone()

            clock = prepared[partition_id]

            if hasattr(route, "keys"):
                # Sparse keys may affect separate intervals.
                for key in route.keys:
                    clock.record(
                        key, key + 1, worker_id, clock_counter,
                    )
            else:
                # A dense interval can be recorded at once.
                clock.record(
                    route.begin,
                    route.end,
                    worker_id,
                    clock_counter,
                )

        return prepared


    def _attach_clock_segments(self, response, keys):
        """Include clock snapshots for partitions touched by a request.

        Called while holding the existing server lock.
        """

        if self.ownership_table is None:
            return

        if isinstance(keys, range):
            routes = self.ownership_table.split_range(
                keys.start, keys.stop
            )
        else:
            routes = self.ownership_table.split_keys(keys)

        for route in routes:
            state = self.range_clocks.get(route.partition_id)

            if state is None:
                continue

            # Begin with all segments of each touched partition.
            for begin, end, clock in state.snapshot():
                at_end = end == KEY_SPACE_END

                segment = response.clocks.add(
                    partition_id=route.partition_id,
                    begin=begin,
                    end=0 if at_end else end,
                    end_is_keyspace_end=at_end,
                )

                # Stable ordering makes results easier to test.
                for worker_id, counter in sorted(clock.items()):
                    segment.entries.add(
                        worker_id=worker_id,
                        counter=counter,
                    )

    def _execute_push(self, request, context):
        keys = self._resolve_keys(request, context)
        gradients = list(request.gradients)
        self._wait_for_dependencies(request, context)

        # Validate the request before changing any parameters.
        if request.width != self.value_width:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Incorrect vector width"
            )

        expected_length = len(keys) * self.value_width

        # D2: Decode compressed gradients when supplied.
        if request.gradients_compressed:
            if request.gradients or not request.compressed_gradients:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Specify compressed gradients without raw gradients",
                )

            try:
                gradients = decompress_doubles(
                    request.compressed_gradients,
                    expected_length,
                )
            except ValueError as error:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    str(error),
                )

        elif request.compressed_gradients:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Compressed payload requires gradients_compressed",
            )

        if len(gradients) != expected_length:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Incorrect number of gradient values"
            )
            
        # Reject gradients that could corrupt parameter values.
        if any(
            not math.isfinite(gradient)
            for gradient in gradients
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Gradients must contain only finite numbers"
            )

        if request.HasField("iteration"):
            if request.iteration == 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Iteration must be positive",
                )

            if not request.HasField("task_id"):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Explicit iteration requires a Task ID",
                )

        # Identify this logical operation, if an ID was supplied.
        task_key = None
        fingerprint = None

        if request.HasField("task_id"):
            if request.task_id.sequence == 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Task sequence must be greater than zero"
                )

            task_key = (
                request.task_id.worker_id,
                request.task_id.sequence
            )

            # Create a compact fingerprint of the request.
            request_bytes = request.SerializeToString(
                deterministic=True
            )

            fingerprint = hashlib.sha256(
                request_bytes
            ).digest()

        self._wait_for_clock_dependencies(
            request, keys, context
        )

        # In batch mode, concurrent requests must be allowed to enqueue.
        # Taking _push_serial_lock here would prevent the batch from filling.
        if self._batch_size > 1:
            return self._enqueue_batched_push(
                request, context, keys, gradients, task_key, fingerprint,
            )

        # Replicated Pushes require a global send order so backups receive
        # exactly the same sequence. Unreplicated Pushes already commit
        # atomically under self.lock and MUST NOT hold the global serial lock
        # while a frozen E1 range is waiting: that would also block unrelated
        # nonmigrating ranges (E1.2's selective-fence guarantee).
        if self._live_replication_targets:
            with self._push_serial_lock:
                return self._execute_push_commit(
                    request, context, keys, gradients, task_key, fingerprint,
                )
        return self._execute_push_commit(
            request, context, keys, gradients, task_key, fingerprint,
        )

    def _enqueue_batched_push(
        self, request, context, keys, gradients, task_key, fingerprint,
    ):
        """Wait until this Push's entire batch has been replicated/committed."""
        job = {
            "request": request,
            "keys": keys,
            "gradients": gradients,
            "task_key": task_key,
            "fingerprint": fingerprint,
            "arrived": time.monotonic(),
            "event": threading.Event(),
            "error": None,
            "response": None,
        }
        with self._batch_condition:
            if self._batch_stopping:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Batcher is stopped")
            self._batch_pending.append(job)
            self._batch_condition.notify_all()

        # We intentionally do not acknowledge during accumulation.
        # At most batch_timeout_s elapses before an underfilled batch starts.
        job["event"].wait()
        if job["error"] is not None:
            code, detail = job["error"]
            context.abort(code, detail)
        return job["response"]

    def _batch_loop(self):
        """Flush on size or on age of the oldest queued request."""
        while True:
            with self._batch_condition:
                while not self._batch_pending and not self._batch_stopping:
                    self._batch_condition.wait()
                if not self._batch_pending and self._batch_stopping:
                    return

                while (len(self._batch_pending) < self._batch_size
                       and not self._batch_stopping):
                    deadline = (
                        self._batch_pending[0]["arrived"] + self._batch_timeout_s
                    )
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._batch_condition.wait(timeout=remaining)

                jobs = self._batch_pending[:self._batch_size]
                del self._batch_pending[:len(jobs)]

            try:
                self._flush_push_batch(jobs)
            except Exception:
                # Unexpected coordinator failure must never acknowledge work.
                # Without repair, no new replicated Push is safe to accept.
                with self.lock:
                    self._replication_faulted.update(self._live_replication_targets)
                for job in jobs:
                    job["error"] = (
                        grpc.StatusCode.UNAVAILABLE,
                        "REPLICATION_UNAVAILABLE: batch coordinator failed; "
                        "all primary ranges fenced",
                    )
            finally:
                for job in jobs:
                    job["event"].set()

    def stop_replication_batcher(self):
        """Flush a partially filled batch and stop the background worker.

        For use during an orderly server shutdown. Can be called repeatedly.
        """
        with self._batch_condition:
            thread = self._batch_thread
            if thread is None:
                return
            self._batch_stopping = True
            self._batch_condition.notify_all()
        if thread is threading.current_thread():
            raise RuntimeError("Cannot join the batcher from itself")
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError("Replication batcher did not stop")
        with self._batch_condition:
            if self._batch_thread is thread:
                self._batch_thread = None

    def _flush_push_batch(self, jobs):
        """Speculatively prepare, replicate, then atomically commit a batch.

        Prepared state stays private until every required backup has applied
        its records. A failed batch never leaks unreplicated weights to Pull.
        Different primary partitions can be combined in the same flush.
        """
        with self._push_serial_lock:
            with self.lock:
                working_weights = {}
                working_clocks = {}
                sequences = dict(self._replication_sequences)
                seen_tasks = {}
                records = []
                record_jobs = []
                touched = set()
                outgoing = {}  # backup instance -> ordered records

                for job in jobs:
                    keys = job["keys"]
                    task_key = job["task_key"]
                    fingerprint = job["fingerprint"]
                    request = job["request"]
                    gradients = job["gradients"]

                    previous = self.completed_pushes.get(task_key)
                    if previous is None and task_key in seen_tasks:
                        previous = seen_tasks[task_key]
                    if previous is not None:
                        if previous != fingerprint:
                            job["error"] = (
                                grpc.StatusCode.INVALID_ARGUMENT,
                                "Task ID reused with different request",
                            )
                        # Same Task ID / same request: resolve after flush.
                        continue

                    if task_key is None:
                        job["error"] = (
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Replicated Push requires a Task ID",
                        )
                        continue

                    if not keys:
                        job["error"] = (
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Replicated Push cannot have an empty key set",
                        )
                        continue

                    try:
                        if self.ownership_table is None or not self.ownership_table.owns(
                            self.owner, keys,
                        ):
                            job["error"] = (
                                grpc.StatusCode.FAILED_PRECONDITION,
                                "NOT_OWNER: range is not owned by this server",
                            )
                            continue
                        routes = (
                            self.ownership_table.split_range(keys.start, keys.stop)
                            if isinstance(keys, range)
                            else self.ownership_table.split_keys(keys)
                        )
                        if len(routes) != 1:
                            job["error"] = (
                                grpc.StatusCode.FAILED_PRECONDITION,
                                "Replicated Push must address one primary partition",
                            )
                            continue
                        partition_id = routes[0].partition_id
                        if self._migration_fenced:
                            job["error"] = (
                                grpc.StatusCode.UNAVAILABLE,
                                "MIGRATION_FENCED: migration or recovery in progress",
                            )
                            continue
                        if self._repair_write_fenced:
                            job["error"] = (
                                grpc.StatusCode.UNAVAILABLE,
                                "REPLICATION_REPAIR: writes temporarily fenced",
                            )
                            continue
                        if partition_id in self._promoted_write_fenced:
                            job["error"] = (
                                grpc.StatusCode.UNAVAILABLE,
                                "PROMOTED_READ_ONLY: replication repair required",
                            )
                            continue
                        if partition_id in self._replication_faulted:
                            job["error"] = (
                                grpc.StatusCode.UNAVAILABLE,
                                "REPLICATION_UNAVAILABLE: partition fenced after error",
                            )
                            continue

                        # Calculate complete after-images from private tentative
                        # state; subsequent Pushes in this batch see predecessors.
                        rule = self.update_rules[self.active_update_rule]
                        updates = {}
                        for index, key in enumerate(keys):
                            grad = gradients[
                                index * request.width:(index + 1) * request.width
                            ]
                            base = working_weights.get(
                                key, self.weights.get(key, [0.0] * request.width)
                            )
                            values = list(rule(
                                list(base), list(grad), self.learning_rate,
                            ))
                            if (len(values) != request.width
                                    or any(not isinstance(v, (int, float))
                                           or isinstance(v, bool) or not math.isfinite(v)
                                           for v in values)):
                                raise ValueError("Update rule returned invalid values")
                            updates[key] = values

                        clock = working_clocks.get(partition_id)
                        if clock is None:
                            clock = self.range_clocks[partition_id].clone()
                        else:
                            clock = clock.clone()
                        counter = (
                            request.iteration if request.HasField("iteration")
                            else task_key[1]
                        )
                        if isinstance(keys, range):
                            clock.record(keys.start, keys.stop, task_key[0], counter)
                            bounds = (keys.start, keys.stop)
                        else:
                            for key in keys:
                                clock.record(key, key + 1, task_key[0], counter)
                            bounds = (min(keys), max(keys) + 1)

                    except Exception as error:
                        job["error"] = (
                            grpc.StatusCode.INTERNAL,
                            f"Batch Push preparation failed: {type(error).__name__}",
                        )
                        continue

                    # Finalize tentative state only for a validated Push.
                    working_weights.update(updates)
                    working_clocks[partition_id] = clock
                    seen_tasks[task_key] = fingerprint
                    sequences[partition_id] += 1
                    record = {
                        "primary_id": self.owner,
                        "partition_id": partition_id,
                        "epoch": self.ownership_table.epoch,
                        "sequence": sequences[partition_id],
                        "task_id": task_key,
                        "fingerprint": fingerprint,
                        "bounds": bounds,
                        "weights": copy.deepcopy(updates),
                        "clock_segments": clock.snapshot(),
                    }
                    records.append(record)
                    record_jobs.append(job)
                    touched.add(partition_id)
                    for backup in self._live_replication_targets[partition_id]:
                        outgoing.setdefault(backup, []).append(record)

                if records:
                    # Cross-primary backup calls must not hold the local lock.
                    self._replication_inflight = True
                    self.lock.release()
                    failure = None
                    try:
                        for backup, batch in outgoing.items():
                            backup.apply_replica_batch(copy.deepcopy(batch))
                    except Exception as error:
                        failure = error
                    finally:
                        self.lock.acquire()
                        self._replication_inflight = False

                    if failure is not None:
                        self._replication_faulted.update(touched)
                        for job in jobs:
                            if job["error"] is None:
                                job["error"] = (
                                    grpc.StatusCode.UNAVAILABLE,
                                    "REPLICATION_UNAVAILABLE: batch not confirmed; "
                                    "affected ranges fenced pending repair",
                                )
                        return

                    # This is the only worker-visible commit point.
                    self.weights.update(working_weights)
                    self.range_clocks.update(working_clocks)
                    self._replication_sequences.update(sequences)
                    for record in records:
                        task_key = record["task_id"]
                        self.completed_pushes[task_key] = record["fingerprint"]
                        self.completed_push_bounds[task_key] = record["bounds"]
                        self.task_states[task_key] = "DONE"
                    self._batch_flushes += 1
                    self._batch_committed_pushes += len(records)
                    self.task_condition.notify_all()

                for job in jobs:
                    if job["error"] is None:
                        response = pb.PushResponse(accepted=True)
                        self._attach_clock_segments(response, job["keys"])
                        job["response"] = response

    def _execute_push_commit(
        self, request, context, keys, gradients, task_key, fingerprint,
    ):
        with self.lock:
            self._wait_for_transfer(keys)

            # Check whether this task has already completed.
            if task_key is not None:
                previous = self.completed_pushes.get(task_key)

                if previous is not None:
                    if previous != fingerprint:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Task ID reused with different request"
                        )

                    # Already applied: do not update weights again.
                    response = pb.PushResponse(accepted=True)
                    self._attach_clock_segments(response, keys)
                    return response

            # Prepare every update before modifying stored parameters.
            # Keep this entire section inside the existing server lock.

            # Only new updates need current ownership.
            # Already-completed requests were handled above.
            self._check_ownership(keys, context)

            # F3.4: reject new Pushes while a manager-controlled E1
            # transfer is using the current committed replica state.
            if self._migration_fenced:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "MIGRATION_FENCED: migration or recovery in progress",
                )

            # F3.2: no new local commit may race with reseeding shadows.
            if self._repair_write_fenced:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "REPLICATION_REPAIR: writes temporarily fenced",
                )

            # F3.1: an installed replica is readable, but may not accept
            # a new unreplicated Push until F3.2 reestablishes backups.
            if self._promoted_write_fenced:
                routes = (
                    self.ownership_table.split_range(keys.start, keys.stop)
                    if isinstance(keys, range)
                    else self.ownership_table.split_keys(keys)
                )
                if any(route.partition_id in self._promoted_write_fenced for route in routes):
                    context.abort(
                        grpc.StatusCode.UNAVAILABLE,
                        "PROMOTED_READ_ONLY: replication repair required",
                    )

            replicate_partition = None
            replicate_targets = None
            if self._live_replication_targets:
                if task_key is None:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Replicated Push requires a Task ID",
                    )
                routes = (
                    self.ownership_table.split_range(keys.start, keys.stop)
                    if isinstance(keys, range)
                    else self.ownership_table.split_keys(keys)
                )
                if len(routes) != 1:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "Replicated Push must address one primary partition",
                    )
                replicate_partition = routes[0].partition_id
                if replicate_partition in self._replication_faulted:
                    context.abort(
                        grpc.StatusCode.UNAVAILABLE,
                        "REPLICATION_UNAVAILABLE: partition fenced after error",
                    )
                replicate_targets = self._live_replication_targets[replicate_partition]

            try:
                prepared_clocks = self._prepare_push_clocks(
                    request, task_key, keys
                )
            except (ValueError, KeyError) as error:
                context.abort(
                    grpc.StatusCode.INTERNAL,
                    f"Clock preparation failed: {error}"
                )

            prepared_updates = {}

            rule = self.update_rules[self.active_update_rule]

            try:
                for index, key in enumerate(keys):
                    start = index * request.width
                    end = start + request.width

                    gradient_vector = list(
                        gradients[start:end]
                    )

                    # Copy the stored vector so a custom rule cannot
                    # mutate the original parameter through its input.
                    current_vector = list(
                        self.weights.get(
                            key, [0.0] * request.width
                        )
                    )

                    # Give the custom rule independent input lists.
                    updated_vector = rule(
                        current_vector.copy(),
                        gradient_vector.copy(),
                        self.learning_rate
                    )

                    # Materialize the result before committing anything.
                    updated_vector = list(updated_vector)

                    # Every parameter vector must retain its width.
                    if len(updated_vector) != request.width:
                        raise ValueError(
                            f"Update rule returned width "
                            f"{len(updated_vector)}, expected {request.width}"
                        )

                    # A custom rule must not introduce invalid values.
                    for value in updated_vector:
                        if (
                            not isinstance(value, (int, float))
                            or isinstance(value, bool)
                            or not math.isfinite(value)
                        ):
                            raise ValueError(
                                "Update rule returned a non-finite "
                                "or non-numeric value"
                            )

                    prepared_updates[key] = updated_vector

            except Exception as error:
                context.abort(
                    grpc.StatusCode.INTERNAL,
                    f"Update rule failed: {type(error).__name__}"
                )

            # F2.2: Acknowledged Pushes must already be on every required
            # backup before the local atomic commit occurs. Only one
            # physical partition is handled in this milestone.
            if replicate_partition is not None:
                if not prepared_updates:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Replicated Push cannot have an empty key set",
                    )
                if isinstance(keys, range):
                    bounds = (keys.start, keys.stop)
                else:
                    bounds = (min(keys), max(keys) + 1)
                next_sequence = self._replication_sequences[replicate_partition] + 1
                replica_record = {
                    "primary_id": self.owner,
                    "partition_id": replicate_partition,
                    "epoch": self.ownership_table.epoch,
                    "sequence": next_sequence,
                    "task_id": task_key,
                    "fingerprint": fingerprint,
                    "bounds": bounds,
                    "weights": copy.deepcopy(prepared_updates),
                    "clock_segments": prepared_clocks[replicate_partition].snapshot(),
                }
                self._replicate_prepared_push(
                    replicate_targets, replica_record, context,
                )
                # The backing replicas have accepted this ordered record.
                self._replication_sequences[replicate_partition] = next_sequence

            # COMMIT PHASE: atomic weights, clocks and migration metadata.
            self.weights.update(prepared_updates)
            self.range_clocks.update(prepared_clocks)

            if task_key is not None:
                self.completed_pushes[task_key] = fingerprint

                if isinstance(keys, range):
                    bounds = (keys.start, keys.stop)
                elif keys:
                    bounds = (min(keys), max(keys) + 1)
                else:
                    bounds = (0, 0)

                self.completed_push_bounds[task_key] = bounds

                # The RPC handler will also mark DONE; recording it here
                # makes transfer snapshots consistent with the Push commit.
                self.task_states[task_key] = "DONE"

            transfer = self._transfer
            if transfer is not None:
                begin = transfer["begin"]
                end = transfer["end"]

                dirty = {
                    key for key in prepared_updates
                    if begin <= key < end
                }

                if dirty:
                    transfer["dirty_keys"].update(dirty)
                    if task_key is not None:
                        transfer["new_pushes"].add(task_key)

            self.task_condition.notify_all()

            response = pb.PushResponse(accepted=True)
            self._attach_clock_segments(response, keys)

        return response


    def _execute_pull(self, request, context):
        self._wait_for_dependencies(request, context)
        keys = self._resolve_keys(request, context)

        values = []

        self._wait_for_clock_dependencies(
            request, keys, context
        )

        with self.lock:
            self._wait_for_transfer(keys)
            self._check_ownership(keys, context)
            self._register_key_cache(request, keys, context)

            for key in keys:
                vector = self.weights.get(
                    key, [0.0] * self.value_width
                )
                values.extend(vector)

            response = pb.PullResponse(
                keys=keys,
                values=values,
                width=self.value_width,
            )

            self._attach_clock_segments(response, keys)

        # D2: Optionally compress the response values.
        if request.accept_compressed_values:
            compressed = pb.PullResponse()
            compressed.CopyFrom(response)

            compressed.ClearField("values")
            compressed.compressed_values = compress_doubles(values)
            compressed.values_compressed = True

            # Send compressed values only when this actually
            # reduces the serialized Protobuf response size.
            if compressed.ByteSize() < response.ByteSize():
                return compressed

        return response

    def _is_key_cache_miss(self, context):
        return (
            context.code() == grpc.StatusCode.FAILED_PRECONDITION
            and (context.details() or "").startswith("KEY_CACHE_MISS:")
        )


    def Push(self, request, context):
        try:
            response = self._execute_push(request, context)

        except Exception:
            if not self._is_key_cache_miss(context):
                self._finish_task(request, succeeded=False)
            raise

        self._finish_task(request, succeeded=True)
        return response


    def Pull(self, request, context):
        try:
            response = self._execute_pull(request, context)

        except Exception:
            if not self._is_key_cache_miss(context):
                self._finish_task(request, succeeded=False)
            raise

        self._finish_task(request, succeeded=True)
        return response


    def register_update_rule(self, name, function):
        """Register a named update function."""

        if not isinstance(name, str) or not name.strip():
            raise ValueError("Rule name must be a nonempty string")

        if not callable(function):
            raise TypeError("Update rule must be callable")

        with self.lock:
            if name in self.update_rules:
                raise ValueError("Update rule already registered")

            self.update_rules[name] = function


    def set_update_rule(self, name):
        """Select an existing update rule."""

        with self.lock:
            if self._live_replication_targets and name != self.active_update_rule:
                raise ValueError("Cannot change update rule during live replication")
            if name not in self.update_rules:
                raise ValueError("Unknown update rule")

            self.active_update_rule = name

    
    def SetUpdateRule(self, request, context):
        """Select a registered update rule through gRPC."""

        try:
            self.set_update_rule(request.name)

        except ValueError as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(error)
            )

        return pb.SetUpdateRuleResponse(
            active_rule=request.name
        )

    def RegisterUpdateRule(self, request, context):
        """Register a client-configured linear update rule."""

        scale = request.gradient_scale

        # Reject invalid numerical coefficients.
        if not math.isfinite(scale):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Gradient scale must be finite"
            )

        # Create the actual update function on the server.
        def scaled_update(weights, gradients, learning_rate):
            return [
                weight + scale * gradient
                for weight, gradient in zip(weights, gradients)
            ]

        # Reuse our existing thread-safe registration method.
        try:
            self.register_update_rule(
                request.name,
                scaled_update
            )

        except (ValueError, TypeError) as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(error)
            )

        return pb.RegisterUpdateRuleResponse(
            registered=True
        )


def serve(
    listen_address="127.0.0.1:50051",
    manager_address=None,
    heartbeat_interval_s=0.1,
    heartbeat_rpc_timeout_s=0.4,
    bootstrap_ownership=False,
):
    """Start the existing parameter server, optionally with heartbeats.

    A bare serve() preserves Part A's original default endpoint.
    The manager must have registered listen_address first.
    """
    if bootstrap_ownership and manager_address is None:
        raise ValueError("--bootstrap-ownership requires --manager")

    instance = ParameterServer()
    if bootstrap_ownership:
        from manager_rpc import fetch_ownership_map
        table = fetch_ownership_map(manager_address)
        instance.set_ownership(listen_address, table)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), options=[
        ("grpc.max_send_message_length", 32 * 1024 * 1024 + 1024),
        ("grpc.max_receive_message_length", 32 * 1024 * 1024 + 1024),
    ])
    pb_grpc.add_ParameterServerServicer_to_server(instance, server)

    # F2.4: expose backup-only control operations separately from worker RPCs.
    from replica_transport import add_replica_transport_to_server
    add_replica_transport_to_server(instance, server)

    bound_port = server.add_insecure_port(listen_address)
    if not bound_port:
        raise RuntimeError(f"Cannot bind parameter server: {listen_address}")

    sender = None
    try:
        server.start()
        print(f"Parameter server listening at {listen_address}", flush=True)

        if manager_address is not None:
            # Import only when needed: default Part A entrypoint has
            # no additional dependency on the heartbeat module.
            from server_heartbeat import ServerHeartbeatSender
            sender = ServerHeartbeatSender(
                manager_address=manager_address,
                server_id=listen_address,
                interval_s=heartbeat_interval_s,
                rpc_timeout_s=heartbeat_rpc_timeout_s,
            )
            sender.start()

        server.wait_for_termination()

    except KeyboardInterrupt:
        pass

    finally:
        if sender is not None:
            sender.stop()
        instance.stop_replication_batcher()
        server.stop(grace=0).wait()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parameter-server process")
    parser.add_argument("--listen", default="127.0.0.1:50051")
    parser.add_argument("--manager", default=None)
    parser.add_argument("--bootstrap-ownership", action="store_true")
    parser.add_argument("--heartbeat-interval", type=float, default=0.1)
    parser.add_argument("--heartbeat-rpc-timeout", type=float, default=0.4)
    args = parser.parse_args()

    serve(
        listen_address=args.listen,
        manager_address=args.manager,
        heartbeat_interval_s=args.heartbeat_interval,
        heartbeat_rpc_timeout_s=args.heartbeat_rpc_timeout,
        bootstrap_ownership=args.bootstrap_ownership,
    )
