import threading
import math
import time

from routing import Partition, RoutingTable
from consistency import ConsistencyGate

class ServerManager:
    """Stores the authoritative partition ownership map."""

    def __init__(self, partitions, heartbeat_timeout_s=2.0, clock=None):
        if (
            isinstance(heartbeat_timeout_s, bool)
            or not isinstance(heartbeat_timeout_s, (int, float))
            or not math.isfinite(heartbeat_timeout_s)
            or heartbeat_timeout_s <= 0
        ):
            raise ValueError("Heartbeat timeout must be a positive finite number")
        if clock is not None and not callable(clock):
            raise TypeError("Clock must be callable")

        self.heartbeat_timeout_s = float(heartbeat_timeout_s)
        self._heartbeat_clock = time.monotonic if clock is None else clock
        self._lock = threading.Lock()
        self._table = RoutingTable(partitions, epoch=1)
        self.consistency_gate = None
        # F4: Disabled unless configured, preserving the original C/E paths.
        self._worker_timeout_s = None
        self._worker_health = {}
        self._migration_lock = threading.Lock()

        # F2.1: Replica placement is separate from the primary routing map.
        # The manager owns this plan; backups never become clients' owners
        # until an explicit F3 promotion changes the routing epoch.
        self._replication_plan = None
        self._replication_plan_epoch = None
        self._replication_k = 0
        self._replication_servers = ()

        # F3.1: a published promotion can be retried if its final RPC reply
        # is lost. It must never publish a second epoch for the same crash.
        self._pending_failover = None
        self._completed_failovers = set()

        # F3.2: retain a repair phase for safe retries after uncertain RPCs.
        self._pending_replication_repair = None
        self._repaired_epochs = set()
        self._active_replication_batch_size = None
        self._active_replication_batch_timeout_s = None

        # F3.4: a published migration can resume its repair without
        # publishing a second epoch or overwriting post-release writes.
        self._pending_migration_repair = None

        # F1.2: Periodic detector lifecycle is independent of the
        # ownership lock and does not run until explicitly started.
        self._monitor_control_lock = threading.Lock()
        self._monitor_stop = threading.Event()
        self._monitor_thread = None
        self._monitor_interval_s = None

        # F1.1: Register current primary server addresses, without
        # assuming that a registered server has started heartbeating.
        registered_at = self._heartbeat_clock()
        self._server_health = {
            owner: {
                "state": "REGISTERED",
                "registered_at_s": registered_at,
                "last_seen_s": None,
                "suspected_at_s": None,
            }
            for owner in sorted({p.owner for p in self._table.partitions})
        }

    @staticmethod
    def _validate_server_id(server_id):
        if not isinstance(server_id, str) or not server_id.strip():
            raise ValueError("Server ID must be a nonempty string")

    def register_server(self, server_id):
        """Register an upcoming server, without marking it healthy.

        Returns True for a new registration, False if already registered.
        A live newcomer should be registered before sending heartbeats.
        """
        self._validate_server_id(server_id)
        with self._lock:
            if server_id in self._server_health:
                return False
            self._server_health[server_id] = {
                "state": "REGISTERED",
                "registered_at_s": self._heartbeat_clock(),
                "last_seen_s": None,
                "suspected_at_s": None,
            }
            return True

    def record_heartbeat(self, server_id):
        """Record receipt at manager monotonic time; return reception time.

        A heartbeat from a suspected server clears suspicion. Failover
        and fencing are deliberately not part of F1.1.
        """
        self._validate_server_id(server_id)
        with self._lock:
            if server_id not in self._server_health:
                raise KeyError(f"Unregistered server: {server_id}")
            now = self._heartbeat_clock()
            entry = self._server_health[server_id]
            if entry["state"] == "FAILED":
                raise RuntimeError("Failed server cannot regain authority by heartbeat")
            entry["state"] = "ALIVE"
            entry["last_seen_s"] = now
            entry["suspected_at_s"] = None
            return now

    def check_server_timeouts(self):
        """Mark newly timed-out servers SUSPECTED; return their IDs.

        Called manually in F1.1. A periodic monitor is added in F1.2.
        Each timeout creates at most one transition until a new heartbeat.
        """
        with self._lock:
            now = self._heartbeat_clock()
            newly_suspected = []
            for server_id, entry in self._server_health.items():
                if entry["state"] in ("SUSPECTED", "FAILED"):
                    continue
                reference = (
                    entry["last_seen_s"]
                    if entry["last_seen_s"] is not None
                    else entry["registered_at_s"]
                )
                if now - reference >= self.heartbeat_timeout_s:
                    entry["state"] = "SUSPECTED"
                    entry["suspected_at_s"] = now
                    newly_suspected.append(server_id)
            return newly_suspected

    def heartbeat_snapshot(self):
        """Return an independent diagnostic snapshot of server health."""
        with self._lock:
            return {
                server_id: dict(entry)
                for server_id, entry in self._server_health.items()
            }

    def _heartbeat_monitor_loop(self):
        """F1.2: Poll server timeouts until asked to stop."""
        while not self._monitor_stop.wait(self._monitor_interval_s):
            self.check_server_timeouts()
            self.check_worker_timeouts()

    def start_heartbeat_monitor(self, interval_s=0.05):
        """Start a single periodic timeout detector (opt-in lifecycle).

        A manager hosting process should call this after configuring
        the cluster and stop_heartbeat_monitor() during shutdown.
        """
        if (
            isinstance(interval_s, bool)
            or not isinstance(interval_s, (int, float))
            or not math.isfinite(interval_s)
            or interval_s <= 0
        ):
            raise ValueError("Monitor interval must be positive and finite")

        with self._monitor_control_lock:
            if self._monitor_thread is not None:
                raise RuntimeError("Heartbeat monitor has already started")

            self._monitor_interval_s = float(interval_s)
            self._monitor_stop.clear()
            thread = threading.Thread(
                target=self._heartbeat_monitor_loop,
                name="server-heartbeat-monitor",
                daemon=True,
            )
            self._monitor_thread = thread
            thread.start()

    def stop_heartbeat_monitor(self):
        """Stop the background detector and wait until it exits."""
        with self._monitor_control_lock:
            thread = self._monitor_thread
            if thread is None:
                return
            self._monitor_stop.set()

        # Never join while holding the lifecycle lock.
        if thread is threading.current_thread():
            raise RuntimeError("Cannot stop heartbeat monitor from itself")
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError("Heartbeat monitor did not stop")

        with self._monitor_control_lock:
            if self._monitor_thread is thread:
                self._monitor_thread = None
                self._monitor_interval_s = None

    def configure_replication(self, server_ids, k=1):
        """Place each primary range on k distinct clockwise ring neighbours.

        Server IDs are existing heartbeat identities (currently gRPC addresses).
        The order of server_ids is the deterministic ring order.
        Only the placement plan is created here: no live Push replication yet.
        """
        if isinstance(server_ids, (str, bytes)):
            raise ValueError("Server IDs must be a sequence of server IDs")
        try:
            server_ids = tuple(server_ids)
        except TypeError as error:
            raise ValueError("Server IDs must be iterable") from error
        if not server_ids:
            raise ValueError("Server IDs must be unique and nonempty")
        for server_id in server_ids:
            self._validate_server_id(server_id)
        if len(server_ids) != len(set(server_ids)):
            raise ValueError("Server IDs must be unique and nonempty")
        if type(k) is not int or not 1 <= k < len(server_ids):
            raise ValueError("k must be between 1 and server_count - 1")

        with self._lock:
            if self._replication_plan is not None:
                raise RuntimeError("Replication is already configured")
            unregistered = set(server_ids) - self._server_health.keys()
            if unregistered:
                raise ValueError("Replica servers must be registered first")

            positions = {server_id: i for i, server_id in enumerate(server_ids)}
            owners = {p.owner for p in self._table.partitions}
            if not owners <= positions.keys():
                raise ValueError("Every primary owner must be in the server ring")

            count = len(server_ids)
            self._replication_plan = {
                p.partition_id: tuple(
                    server_ids[(positions[p.owner] + offset) % count]
                    for offset in range(1, k + 1)
                )
                for p in self._table.partitions
            }
            self._replication_plan_epoch = self._table.epoch
            self._replication_k = k
            self._replication_servers = server_ids
            return dict(self._replication_plan)

    def get_replication_plan(self):
        """Get a detached plan, refusing silently stale partition metadata."""
        with self._lock:
            if self._replication_plan is None:
                raise RuntimeError("Replication has not been configured")
            if self._replication_plan_epoch != self._table.epoch:
                raise RuntimeError(
                    "Replication plan is stale after ownership change"
                )
            return dict(self._replication_plan)

    def seed_replicas(self, servers):
        """Copy an initial primary snapshot to each designated backup.

        For F2.1 this must be called before concurrent training starts.
        A partial failure can leave some installed seeds; do not treat the
        operation as transactionally atomic. Retry/rebuild is a later step.
        """
        if not isinstance(servers, dict):
            raise TypeError("Servers must map server IDs to server objects")
        with self._migration_lock:
            with self._lock:
                if self._replication_plan is None:
                    raise RuntimeError("Replication has not been configured")
                if self._replication_plan_epoch != self._table.epoch:
                    raise RuntimeError("Replication plan is stale")
                partitions = tuple(self._table.partitions)
                plan = dict(self._replication_plan)

            required = (
                {p.owner for p in partitions}
                | {backup for copies in plan.values() for backup in copies}
            )
            missing = required - servers.keys()
            if missing:
                raise ValueError("Missing server object(s) for replica seeding")

            copied = []
            for partition in partitions:
                snapshot = servers[partition.owner].export_replica_snapshot(
                    partition.partition_id
                )
                for backup_id in plan[partition.partition_id]:
                    servers[backup_id].install_replica_snapshot(snapshot)
                    copied.append((partition.partition_id, backup_id))
            return copied

    def activate_live_replication(
        self, servers, batch_size=1, batch_timeout_s=0.05,
    ):
        """Enable acknowledged per-Push or timed batched replication.

        Setup must be called while no workers are issuing Pushes. For F2.2,
        manager and backups are in one Python process, as in Part E.
        """
        if not isinstance(servers, dict):
            raise TypeError("Servers must map IDs to server objects")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if (isinstance(batch_timeout_s, bool)
                or not isinstance(batch_timeout_s, (int, float))
                or not math.isfinite(batch_timeout_s)
                or batch_timeout_s <= 0):
            raise ValueError("batch_timeout_s must be positive and finite")
        with self._migration_lock:
            with self._lock:
                if self._replication_plan is None:
                    raise RuntimeError("Configure replication and seed first")
                if self._replication_plan_epoch != self._table.epoch:
                    raise RuntimeError("Replication plan is stale")
                partitions = tuple(self._table.partitions)
                plan = dict(self._replication_plan)

            needed = {p.owner for p in partitions}
            needed.update(b for backs in plan.values() for b in backs)
            missing = needed - servers.keys()
            if missing:
                raise ValueError("Missing server object for live replication")

            by_owner = {}
            for partition in partitions:
                by_owner.setdefault(partition.owner, {})[
                    partition.partition_id
                ] = tuple(servers[backup] for backup in plan[partition.partition_id])

            # Fail early if a seed is missing before configuring primaries.
            for primary_id, targets in by_owner.items():
                for partition_id, backups in targets.items():
                    for backup in backups:
                        backup.get_replica_snapshot(primary_id, partition_id)

            for primary_id, targets in by_owner.items():
                servers[primary_id].configure_live_replication(
                    targets, batch_size=batch_size,
                    batch_timeout_s=batch_timeout_s,
                )
            with self._lock:
                self._active_replication_batch_size = batch_size
                self._active_replication_batch_timeout_s = float(batch_timeout_s)
            return {primary_id: tuple(targets) for primary_id, targets in by_owner.items()}

    def promote_confirmed_crash(
        self, failed_server_id, servers, replacement_id=None,
        *, confirmed_dead=False,
    ):
        """F3.1: publish one failed range to a surviving, seeded replica.

        This deliberately requires externally CONFIRMED crash-stop failure;
        a heartbeat suspicion alone is not proof that the old primary is
        fenced. No split-brain safety is claimed for network partitions.
        F3.1 only handles a failed owner with one partition. After
        promotion, new writes stay fenced pending F3.2 replica repair.
        """
        if confirmed_dead is not True:
            raise RuntimeError("Promotion requires confirmed crash-stop failure")
        if not isinstance(servers, dict):
            raise TypeError("Servers must map identities to server objects")
        self._validate_server_id(failed_server_id)

        with self._migration_lock:
            with self._lock:
                if failed_server_id in self._completed_failovers:
                    return RoutingTable(list(self._table.partitions), epoch=self._table.epoch)
                pending = self._pending_failover

            # If an RPC acknowledgement was lost after the map changed,
            # finalize the SAME epoch on retry rather than publishing again.
            if pending is not None:
                if (pending["failed_server_id"] != failed_server_id
                        or (replacement_id is not None
                            and pending["replacement_id"] != replacement_id)):
                    raise RuntimeError("Another promotion must be finished first")
                target = servers[pending["replacement_id"]]
                target.commit_replica_promotion(
                    failed_server_id, pending["partition_id"], pending["epoch"]
                )
                with self._lock:
                    self._pending_failover = None
                    self._completed_failovers.add(failed_server_id)
                return self.get_map()

            with self._lock:
                health = self._server_health.get(failed_server_id)
                if health is None or health["state"] != "SUSPECTED":
                    raise RuntimeError("Server must be SUSPECTED before confirmed promotion")
                if (self._replication_plan is None
                        or self._replication_plan_epoch != self._table.epoch):
                    raise RuntimeError("Current replication plan required")
                current = RoutingTable(list(self._table.partitions), epoch=self._table.epoch)
                affected = [part for part in current.partitions
                            if part.owner == failed_server_id]
                if len(affected) != 1:
                    raise RuntimeError("F3.1 supports exactly one failed primary range")
                partition = affected[0]
                candidates = self._replication_plan[partition.partition_id]
                if replacement_id is not None and replacement_id not in candidates:
                    raise ValueError("Replacement is not a configured backup")
                selected = (replacement_id,) if replacement_id is not None else candidates
                replacement = next((sid for sid in selected
                                    if self._server_health.get(sid, {}).get("state") == "ALIVE"
                                    and sid in servers), None)
                if replacement is None:
                    raise RuntimeError("No healthy configured replica is available")
                new_table = RoutingTable([
                    Partition(p.partition_id, p.begin, p.end,
                              replacement if p.partition_id == partition.partition_id else p.owner)
                    for p in current.partitions
                ], epoch=current.epoch + 1)

            # Prepare on the backup WITHOUT changing worker-visible state.
            target = servers[replacement]
            target.prepare_replica_promotion(
                failed_server_id, partition.partition_id, new_table
            )
            try:
                with self._lock:
                    if self._table.epoch != current.epoch:
                        raise RuntimeError("Map changed during failover preparation")
                    self._table = new_table
                    self._server_health[failed_server_id]["state"] = "FAILED"
                    self._pending_failover = {
                        "failed_server_id": failed_server_id,
                        "replacement_id": replacement,
                        "partition_id": partition.partition_id,
                        "epoch": new_table.epoch,
                    }
            except Exception:
                # Safe cancellation only BEFORE the new map is published.
                target.cancel_replica_promotion(
                    failed_server_id, partition.partition_id, new_table.epoch
                )
                raise

            # Published map may briefly precede promotion activation.
            # If RPC outcome is uncertain, _pending_failover enables retry.
            target.commit_replica_promotion(
                failed_server_id, partition.partition_id, new_table.epoch
            )
            with self._lock:
                self._pending_failover = None
                self._completed_failovers.add(failed_server_id)
            return self.get_map()

    def restore_replication_after_failover(
        self, servers, *, batch_size=None, batch_timeout_s=None,
    ):
        """F3.2: re-seed the surviving ring and resume acknowledged Pushes.

        The manager first fences EVERY survivor (including backup-only
        processes), then re-seeds all partitions in the promoted epoch.
        Preparing all primaries precedes ANY write-fence release. An
        interrupted operation is retried in the same phase and epoch.
        """
        if not isinstance(servers, dict):
            raise TypeError("Servers must map IDs to server objects")
        with self._lock:
            previously_active = self._active_replication_batch_size
            previous_timeout = self._active_replication_batch_timeout_s
        if previously_active is None:
            raise RuntimeError("Live replication must be activated before repair")
        if batch_size is None:
            batch_size = previously_active
        if batch_timeout_s is None:
            batch_timeout_s = previous_timeout
        # Keep the running batcher configuration unchanged during recovery.
        # A separate quiescent reconfiguration can change it later.
        if (batch_size != previously_active or
                batch_timeout_s != previous_timeout):
            raise ValueError("Recovery must retain the active batch settings")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if (type(batch_timeout_s) not in (int, float)
                or not math.isfinite(batch_timeout_s) or batch_timeout_s <= 0):
            raise ValueError("batch_timeout_s must be positive and finite")

        with self._migration_lock:
            with self._lock:
                if self._pending_failover is not None:
                    raise RuntimeError("Finish promotion before replica repair")
                epoch = self._table.epoch
                if epoch in self._repaired_epochs:
                    return dict(self._replication_plan)
                pending = self._pending_replication_repair
                if pending is None:
                    if not self._completed_failovers:
                        raise RuntimeError("No completed failover to repair")
                    if self._replication_plan is None:
                        raise RuntimeError("No preceding replication plan")
                    ring = tuple(sid for sid in self._replication_servers
                                 if self._server_health.get(sid, {}).get("state") == "ALIVE")
                    if len(ring) <= self._replication_k:
                        raise RuntimeError("Insufficient surviving servers for k replicas")
                    if set(ring) - servers.keys():
                        raise ValueError("Missing surviving server proxy")
                    partitions = tuple(self._table.partitions)
                    if any(p.owner not in ring for p in partitions):
                        raise RuntimeError("An owned partition lacks an alive primary")
                    indexes = {sid: n for n, sid in enumerate(ring)}
                    plan = {
                        p.partition_id: tuple(ring[(indexes[p.owner] + offset) % len(ring)]
                                              for offset in range(1, self._replication_k + 1))
                        for p in partitions
                    }
                    pending = {
                        "epoch": epoch, "ring": ring, "partitions": partitions,
                        "plan": plan, "phase": "prepare",
                        "batch_size": batch_size,
                        "batch_timeout_s": float(batch_timeout_s),
                    }
                    self._pending_replication_repair = pending
                elif (pending["epoch"] != epoch
                      or pending["batch_size"] != batch_size
                      or pending["batch_timeout_s"] != float(batch_timeout_s)):
                    raise RuntimeError("Retry must use the same repair epoch and settings")

                ring = pending["ring"]
                partitions = pending["partitions"]
                plan = pending["plan"]
                phase = pending["phase"]
                new_table = RoutingTable(list(partitions), epoch=epoch)
                if set(ring) - servers.keys():
                    raise ValueError("Missing server proxy for pending repair")

            if phase == "prepare":
                # No snapshots until every surviving owner is write-fenced.
                for sid in ring:
                    servers[sid].begin_replication_repair(new_table)

                # Full after-image snapshots replace uncertain/stale shadows.
                # All primaries are quiescent before this copying begins.
                for partition in partitions:
                    snapshot = servers[partition.owner].export_replica_snapshot(
                        partition.partition_id
                    )
                    for backup_id in plan[partition.partition_id]:
                        servers[backup_id].install_repair_replica_snapshot(snapshot)

                targets_by_owner = {}
                for partition in partitions:
                    targets_by_owner.setdefault(partition.owner, {})[
                        partition.partition_id
                    ] = tuple(servers[bid] for bid in plan[partition.partition_id])

                # Prepare (but do NOT release) every owning process.
                for sid in ring:
                    servers[sid].prepare_repaired_replication(
                        targets_by_owner.get(sid, {}), epoch,
                        batch_size=batch_size,
                        batch_timeout_s=batch_timeout_s,
                    )

                with self._lock:
                    self._replication_plan = dict(plan)
                    self._replication_plan_epoch = epoch
                    self._replication_servers = ring
                    pending["phase"] = "release"

            # Once all backups are installed and all owners verified,
            # partial release is safe: each released primary is protected.
            # Never re-seed when retrying this phase; writes may have begun.
            for sid in ring:
                servers[sid].release_repaired_replication(epoch)

            with self._lock:
                self._pending_replication_repair = None
                self._repaired_epochs.add(epoch)
                return dict(plan)

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

    @staticmethod
    def _validate_worker_id(worker_id):
        if type(worker_id) is not int or not 0 <= worker_id < (1 << 32):
            raise ValueError("Worker ID must be a uint32 integer")

    def configure_worker_failure_detection(self, timeout_s=2.0):
        """F4: enable explicit worker heartbeats and crash exclusion.

        Call after configure_consistency and just before starting workers.
        Workers must send their first heartbeat promptly; registrations alone
        do not prove that a worker process is running.
        """
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError("Worker timeout must be positive and finite")
        with self._lock:
            if self.consistency_gate is None:
                raise RuntimeError("Configure consistency before worker detection")
            if self._worker_timeout_s is not None:
                raise RuntimeError("Worker detection is already configured")
            now = self._heartbeat_clock()
            self._worker_timeout_s = float(timeout_s)
            self._worker_health = {
                wid: {
                    "state": "REGISTERED",
                    "registered_at_s": now,
                    "last_seen_s": None,
                    "excluded_at_s": None,
                    "reason": None,
                }
                for wid in self.consistency_gate.active_snapshot()
            }

    def record_worker_heartbeat(self, worker_id):
        """Record worker liveness. Excluded identities cannot rejoin."""
        self._validate_worker_id(worker_id)
        with self._lock:
            if self._worker_timeout_s is None:
                raise RuntimeError("Worker failure detection is disabled")
            entry = self._worker_health.get(worker_id)
            if entry is None:
                raise KeyError(f"Unregistered worker: {worker_id}")
            if entry["state"] == "EXCLUDED":
                raise RuntimeError("Excluded worker cannot resume its session")
            if worker_id not in self.consistency_gate.active_snapshot():
                entry["state"] = "EXCLUDED"
                entry["excluded_at_s"] = self._heartbeat_clock()
                entry["reason"] = "MEMBERSHIP_CHANGED"
                raise RuntimeError("Worker is no longer an active participant")
            now = self._heartbeat_clock()
            entry["state"] = "ALIVE"
            entry["last_seen_s"] = now
            return now

    def check_worker_timeouts(self):
        """Exclude silent workers, preserving historical completed counters.

        Manager lock serializes heartbeat reception with exclusion. Gate
        condition wakes any peers blocked by a removed worker.
        """
        with self._lock:
            if self._worker_timeout_s is None:
                return []
            now = self._heartbeat_clock()
            excluded = []
            active = self.consistency_gate.active_snapshot()
            for worker_id, entry in self._worker_health.items():
                if entry["state"] == "EXCLUDED":
                    continue
                if worker_id not in active:
                    entry["state"] = "EXCLUDED"
                    entry["excluded_at_s"] = now
                    entry["reason"] = "MEMBERSHIP_CHANGED"
                    continue
                previous = (
                    entry["last_seen_s"]
                    if entry["last_seen_s"] is not None
                    else entry["registered_at_s"]
                )
                if now - previous < self._worker_timeout_s:
                    continue
                self.consistency_gate.exclude_failed_worker(worker_id)
                entry["state"] = "EXCLUDED"
                entry["excluded_at_s"] = now
                entry["reason"] = "HEARTBEAT_TIMEOUT"
                excluded.append(worker_id)
            return excluded

    def worker_health_snapshot(self):
        """Return independent diagnostic copies of worker liveness."""
        with self._lock:
            return {
                wid: dict(entry)
                for wid, entry in self._worker_health.items()
            }

    def join_training_worker(self, worker_id):
        """Join a training worker and register its liveness atomically."""
        self._validate_worker_id(worker_id)
        with self._lock:
            if self.consistency_gate is None:
                raise RuntimeError("Consistency has not been configured")
            starting = self.consistency_gate.register_worker(worker_id)
            if self._worker_timeout_s is not None:
                self._worker_health[worker_id] = {
                    "state": "REGISTERED",
                    "registered_at_s": self._heartbeat_clock(),
                    "last_seen_s": None,
                    "excluded_at_s": None,
                    "reason": None,
                }
            return starting, self.consistency_gate.membership_epoch

    def leave_training_worker(self, worker_id):
        """Gracefully leave and retire the heartbeat session."""
        self._validate_worker_id(worker_id)
        with self._lock:
            if self.consistency_gate is None:
                raise RuntimeError("Consistency has not been configured")
            completed = self.consistency_gate.leave_worker(worker_id)
            if worker_id in self._worker_health:
                entry = self._worker_health[worker_id]
                entry["state"] = "EXCLUDED"
                entry["excluded_at_s"] = self._heartbeat_clock()
                entry["reason"] = "GRACEFUL_LEAVE"
            return completed, self.consistency_gate.membership_epoch

    def _migration_repair_locked(self, servers):
        """Complete a PUBLISHED migration while _migration_lock is held.

        Before release, retries may repeat fencing/seeding. Once release
        starts, a retry must NEVER overwrite a backup that may have seen
        subsequent acknowledged Pushes.
        """
        pending = self._pending_migration_repair
        if pending is None:
            raise RuntimeError("No published migration repair is pending")
        table, epoch = pending["table"], pending["table"].epoch
        if pending["phase"] == "source":
            # Publication is irreversible. The source must relinquish the
            # transferred interval before we seed new-epoch backups.
            sid = pending["source_id"]
            death_checker = pending["is_confirmed_dead"]
            dead = death_checker is not None and death_checker(sid)
            if dead:
                if any(p.owner == sid for p in table.partitions):
                    raise RuntimeError(
                        "Source failed after publication with retained ranges; "
                        "writes remain fenced"
                    )
                with self._lock:
                    self._server_health[sid]["state"] = "FAILED"
                ring = tuple(x for x in pending["ring"] if x != sid)
            else:
                servers[sid].finish_outgoing_transfer(table)
                ring = pending["ring"]
            if len(ring) <= self._replication_k:
                raise RuntimeError("Insufficient survivors after cutover")
            indices = {sid: i for i, sid in enumerate(ring)}
            plan = {
                p.partition_id: tuple(
                    ring[(indices[p.owner] + j) % len(ring)]
                    for j in range(1, self._replication_k + 1)
                )
                for p in table.partitions
            }
            pending["ring"] = ring
            pending["plan"] = plan
            pending["phase"] = "prepare"

        ring, plan = pending["ring"], pending["plan"]
        if set(ring) - servers.keys():
            raise ValueError("Missing server proxy for migration repair")
        if pending["phase"] == "prepare":
            # Every process remains write-fenced until the entire plan is
            # verified. The E1 destination is already installed here.
            for sid in ring:
                servers[sid].begin_migration_epoch(table)
            for partition in table.partitions:
                snapshot = servers[partition.owner].export_replica_snapshot(
                    partition.partition_id
                )
                for backup_id in plan[partition.partition_id]:
                    servers[backup_id].install_repair_replica_snapshot(snapshot)
            by_owner = {}
            for partition in table.partitions:
                by_owner.setdefault(partition.owner, {})[
                    partition.partition_id
                ] = tuple(servers[sid] for sid in plan[partition.partition_id])
            for sid in ring:
                servers[sid].prepare_repaired_replication(
                    by_owner.get(sid, {}), epoch,
                    batch_size=pending["batch_size"],
                    batch_timeout_s=pending["batch_timeout_s"],
                )
            # Once this phase advances, backup reseeding is forbidden.
            with self._lock:
                self._replication_plan = dict(plan)
                self._replication_plan_epoch = epoch
                self._replication_servers = tuple(ring)
                pending["phase"] = "release"

        # A release RPC reply can be lost AFTER writes have resumed. Do not
        # resend snapshots or invoke PrepareRepair again at this point.
        for sid in ring:
            servers[sid].release_repaired_replication(epoch)
        with self._lock:
            self._pending_migration_repair = None
            self._repaired_epochs.add(epoch)
        return table

    def resume_published_migration(self, servers):
        """Retry incomplete post-publication repair in the same epoch."""
        if not isinstance(servers, dict):
            raise TypeError("Servers must map identities to objects")
        with self._migration_lock:
            return self._migration_repair_locked(servers)

    def move_range_replicated(
        self, source_id, destination_id, begin, end, servers,
        *, checkpoint=None, is_confirmed_dead=None, crash_timeout_s=4.0,
    ):
        """F3.4: fenced E1 transfer with real-process crash recovery.

        All live primaries pause new writes while E1 copies the range. The
        original replicas remain a crash-safe source of the last acknowledged
        updates. A crash BEFORE publication aborts the unpublished destination
        and uses F3.1+F3.2 to promote/repair. After publication, destination
        ownership is authoritative and new backups must be verified before
        writes resume.

        This crash-stop mode requires a process supervisor callback that can
        confirm termination; heartbeat suspicion alone cannot trigger
        promotion. Manager and other replicas are assumed available.
        """
        if not isinstance(servers, dict):
            raise TypeError("Servers must be a dictionary")
        if source_id == destination_id or not isinstance(source_id, str) or (
            not isinstance(destination_id, str)
        ):
            raise ValueError("Source and destination must be distinct identities")
        if type(begin) is not int or type(end) is not int or not 0 <= begin < end <= (1 << 64):
            raise ValueError("Invalid migration interval")
        if checkpoint is not None and not callable(checkpoint):
            raise TypeError("Checkpoint callback must be callable")
        if is_confirmed_dead is not None and not callable(is_confirmed_dead):
            raise TypeError("Process death checker must be callable")
        if (type(crash_timeout_s) not in (float, int)
                or not math.isfinite(crash_timeout_s) or crash_timeout_s <= 0):
            raise ValueError("Invalid crash recovery timeout")

        failed_id = None
        with self._migration_lock:
            with self._lock:
                if self._pending_migration_repair is not None:
                    raise RuntimeError("Finish published migration repair first")
                if (self._pending_failover is not None
                        or self._pending_replication_repair is not None):
                    raise RuntimeError("Failover repair in progress")
                current = RoutingTable(list(self._table.partitions), epoch=self._table.epoch)
                if (self._replication_plan is None
                        or self._replication_plan_epoch != current.epoch
                        or self._active_replication_batch_size is None):
                    raise RuntimeError("Active acknowledged replication required")
                ring = self._replication_servers
                if any(self._server_health.get(sid, {}).get("state") != "ALIVE"
                       for sid in ring):
                    raise RuntimeError("All ring servers must be ALIVE at migration start")
                if set(ring) - servers.keys():
                    raise ValueError("Missing active server proxy")
                matched = [p for p in current.partitions
                           if p.owner == source_id and p.begin <= begin < end <= p.end]
                if len(matched) != 1 or destination_id not in ring:
                    raise ValueError("Range must be inside one source-owned partition")
                old_part = matched[0]
                if not any(p.owner == destination_id for p in current.partitions):
                    raise ValueError("Destination must be an active cluster member")

                next_epoch = current.epoch + 1
                new_parts = []
                moved_id = f"{old_part.partition_id}_moved_e{next_epoch}"
                for part in current.partitions:
                    if part != old_part:
                        new_parts.append(part)
                        continue
                    if part.begin < begin:
                        new_parts.append(Partition(
                            f"{part.partition_id}_left_e{next_epoch}",
                            part.begin, begin, source_id,
                        ))
                    new_parts.append(Partition(moved_id, begin, end, destination_id))
                    if end < part.end:
                        new_parts.append(Partition(
                            f"{part.partition_id}_right_e{next_epoch}",
                            end, part.end, source_id,
                        ))
                new_table = RoutingTable(new_parts, epoch=next_epoch)

            paused = []
            try:
                # Serialize with in-flight Pushes and make their last
                # acknowledged state stable on all replicas.
                for sid in ring:
                    servers[sid].begin_migration_fence(current.epoch)
                    paused.append(sid)

                snapshot = servers[source_id].begin_range_transfer(begin, end)
                servers[destination_id].stage_incoming_transfer(moved_id, snapshot)
                if checkpoint is not None:
                    checkpoint("staged")

                delta = servers[source_id].freeze_range_transfer()
                servers[destination_id].apply_incoming_delta(delta)
                servers[destination_id].install_incoming_transfer()
                if checkpoint is not None:
                    checkpoint("installed")

                # Nothing on the destination is authoritative before HERE.
                # Check that the source has not died at this cutover point.
                if (is_confirmed_dead is not None
                        and is_confirmed_dead(source_id)):
                    raise RuntimeError("Source died before migration publication")
                published_epoch = self.publish_map(new_parts)
                if published_epoch != next_epoch:
                    raise RuntimeError("Migration published unexpected epoch")
                with self._lock:
                    # Store BEFORE the next RPC/callback; publication cannot
                    # be undone, and completion must be retryable.
                    self._pending_migration_repair = {
                        "table": new_table, "ring": ring, "plan": None,
                        "phase": "source", "source_id": source_id,
                        "is_confirmed_dead": is_confirmed_dead,
                        "batch_size": self._active_replication_batch_size,
                        "batch_timeout_s": self._active_replication_batch_timeout_s,
                    }
                if checkpoint is not None:
                    checkpoint("published")

            except Exception:
                if self.get_map().epoch != current.epoch:
                    # Publication has occurred: NEVER revert the manager's
                    # routing table or delete destination state.
                    raise
                # Pre-publication: delete speculative destination data,
                # including data already installed but not yet advertised.
                # If the destination is dead, its RAM state is gone.
                dead = []
                if is_confirmed_dead is not None:
                    dead = [sid for sid in ring if is_confirmed_dead(sid)]
                if len(dead) > 1:
                    raise RuntimeError("Multiple simultaneous failures unsupported")
                cleanup_errors = []
                try:
                    if destination_id not in dead:
                        servers[destination_id].abort_unpublished_migration()
                except Exception as error:
                    cleanup_errors.append(error)
                try:
                    if source_id not in dead:
                        servers[source_id].cancel_range_transfer_if_active()
                except Exception as error:
                    cleanup_errors.append(error)
                if cleanup_errors:
                    raise RuntimeError(
                        "Unpublished rollback is uncertain; surviving servers remain fenced"
                    ) from cleanup_errors[0]
                if dead:
                    failed_id = dead[0]
                else:
                    for sid in paused:
                        servers[sid].end_migration_fence(current.epoch)
                    raise
            else:
                # Never repeat publication; the persisted pending phase
                # determines whether source finalization or backup repair
                # remains. Retried release cannot overwrite newer writes.
                return self._migration_repair_locked(servers)

        # A prepublication crash falls through AFTER _migration_lock is
        # released, so F3.1/F3.2 can acquire the same lock normally.
        deadline = time.monotonic() + crash_timeout_s
        while time.monotonic() < deadline:
            self.check_server_timeouts()
            if self.heartbeat_snapshot()[failed_id]["state"] == "SUSPECTED":
                break
            time.sleep(0.02)
        else:
            raise RuntimeError("Dead process not yet SUSPECTED; survivors remain fenced")
        # Promotion will restore exactly the previously acknowledged state.
        self.promote_confirmed_crash(
            failed_id, servers, confirmed_dead=True,
        )
        self.restore_replication_after_failover(servers)
        return self.get_map()

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
