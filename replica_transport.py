"""F2.4: loopback-oriented inter-process control/replication over gRPC.

The RPC envelopes are standard protobuf BytesValue messages. Typed JSON inside the
bytes preserves Python integer keys, tuple Task IDs, bytes fingerprints, clocks,
and full uint64 range boundaries without using pickle or JS numbers.

This is *not* an authenticated external API; bind server endpoints to trusted
localhost interfaces for the coursework crash-stop experiments.
"""

import base64
import json
import math

import grpc
from google.protobuf.wrappers_pb2 import BytesValue

from routing import Partition, RoutingTable

_SERVICE = "ds.ReplicaTransport"
_MAX_BYTES = 32 * 1024 * 1024


def _encode(value):
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Non-finite float in replica payload")
        return value
    if isinstance(value, bytes):
        return {"tag": "bytes", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return {"tag": "tuple", "items": [_encode(x) for x in value]}
    if isinstance(value, list):
        return {"tag": "list", "items": [_encode(x) for x in value]}
    if isinstance(value, dict):
        return {"tag": "dict", "items": [[_encode(k), _encode(v)] for k, v in value.items()]}
    raise TypeError(f"Unsupported replica payload type: {type(value).__name__}")


def _decode(value):
    if value is None or type(value) in (str, bool, int, float):
        if type(value) is float and not math.isfinite(value):
            raise ValueError("Non-finite float in replica payload")
        return value
    if not isinstance(value, dict):
        raise ValueError("Malformed replica payload")
    tag = value.get("tag")
    if tag == "bytes" and set(value) == {"tag", "data"}:
        return base64.b64decode(value["data"], validate=True)
    if tag == "tuple" and set(value) == {"tag", "items"}:
        return tuple(_decode(x) for x in value["items"])
    if tag == "list" and set(value) == {"tag", "items"}:
        return [_decode(x) for x in value["items"]]
    if tag == "dict" and set(value) == {"tag", "items"}:
        result = {}
        for pair in value["items"]:
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError("Malformed replica mapping")
            key, data = (_decode(pair[0]), _decode(pair[1]))
            if key in result:
                raise ValueError("Duplicate replica mapping key")
            result[key] = data
        return result
    raise ValueError("Unknown replica payload tag")


def pack(value):
    payload = json.dumps(_encode(value), allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > _MAX_BYTES:
        raise ValueError("Replica payload too large")
    return BytesValue(value=payload)


def unpack(envelope):
    if len(envelope.value) > _MAX_BYTES:
        raise ValueError("Replica payload too large")
    return _decode(json.loads(envelope.value.decode("utf-8"), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x))))


def _pack_table(table):
    return {
        "epoch": table.epoch,
        "partitions": [
            (part.partition_id, part.begin, part.end, part.owner)
            for part in table.partitions
        ],
    }


def _unpack_table(data):
    if (not isinstance(data, dict) or type(data.get("epoch")) is not int
            or not isinstance(data.get("partitions"), list)):
        raise ValueError("Invalid promotion routing table")
    try:
        parts = [Partition(*values) for values in data["partitions"]]
        return RoutingTable(parts, epoch=data["epoch"])
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid promotion routing table") from error


def add_replica_transport_to_server(parameter_server, grpc_server):
    """Register generic unary Protobuf RPCs alongside existing worker RPCs."""
    def handler(method):
        def dispatch(request, context):
            try:
                arg = unpack(request)
                if method == "Export":
                    reply = parameter_server.export_replica_snapshot(arg)
                elif method == "Seed":
                    reply = parameter_server.install_replica_snapshot(arg)
                elif method == "GetReplica":
                    reply = parameter_server.get_replica_snapshot(*arg)
                elif method == "Update":
                    reply = parameter_server.apply_replica_update(arg)
                elif method == "Batch":
                    reply = parameter_server.apply_replica_batch(arg)
                elif method == "Configure":
                    targets = {
                        part_id: tuple(ReplicaPeer(address) for address in addresses)
                        for part_id, addresses in arg["targets"].items()
                    }
                    reply = parameter_server.configure_live_replication(
                        targets,
                        batch_size=arg["batch_size"],
                        batch_timeout_s=arg["batch_timeout_s"],
                    )
                elif method == "GetLive":
                    reply = parameter_server.live_replication_snapshot()
                elif method == "PreparePromotion":
                    reply = parameter_server.prepare_replica_promotion(
                        arg["failed_primary"], arg["partition_id"],
                        _unpack_table(arg["new_table"]),
                    )
                elif method == "CommitPromotion":
                    reply = parameter_server.commit_replica_promotion(
                        arg["failed_primary"], arg["partition_id"], arg["epoch"],
                    )
                elif method == "CancelPromotion":
                    reply = parameter_server.cancel_replica_promotion(
                        arg["failed_primary"], arg["partition_id"], arg["epoch"],
                    )
                elif method == "BeginRepair":
                    reply = parameter_server.begin_replication_repair(
                        _unpack_table(arg["new_table"]),
                    )
                elif method == "RepairSeed":
                    reply = parameter_server.install_replica_snapshot(
                        arg, allow_replace=True,
                    )
                elif method == "PrepareRepair":
                    targets = {
                        pid: tuple(ReplicaPeer(address) for address in addresses)
                        for pid, addresses in arg["targets"].items()
                    }
                    reply = parameter_server.prepare_repaired_replication(
                        targets, arg["epoch"],
                        batch_size=arg["batch_size"],
                        batch_timeout_s=arg["batch_timeout_s"],
                    )
                elif method == "ReleaseRepair":
                    reply = parameter_server.release_repaired_replication(arg)
                elif method == "BeginMigrationFence":
                    reply = parameter_server.begin_migration_fence(arg)
                elif method == "EndMigrationFence":
                    reply = parameter_server.end_migration_fence(arg)
                elif method == "BeginMigrationEpoch":
                    reply = parameter_server.begin_migration_epoch(_unpack_table(arg))
                elif method == "AbortUnpublishedMigration":
                    reply = parameter_server.abort_unpublished_migration()
                elif method == "BeginRangeTransfer":
                    reply = parameter_server.begin_range_transfer(*arg)
                elif method == "FreezeRangeTransfer":
                    reply = parameter_server.freeze_range_transfer()
                elif method == "CancelRangeTransfer":
                    reply = parameter_server.cancel_range_transfer_if_active()
                elif method == "StageIncomingTransfer":
                    reply = parameter_server.stage_incoming_transfer(
                        arg["partition_id"], arg["snapshot"],
                    )
                elif method == "ApplyIncomingDelta":
                    reply = parameter_server.apply_incoming_delta(arg)
                elif method == "InstallIncomingTransfer":
                    reply = parameter_server.install_incoming_transfer()
                elif method == "FinishOutgoingTransfer":
                    reply = parameter_server.finish_outgoing_transfer_if_active(_unpack_table(arg))
                else:
                    raise RuntimeError("Unknown replica operation")
                return pack(reply)
            except (ValueError, TypeError) as error:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
            except KeyError as error:
                context.abort(grpc.StatusCode.NOT_FOUND, str(error))
            except RuntimeError as error:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            except Exception as error:
                # Do not expose Python internals to callers.
                context.abort(grpc.StatusCode.INTERNAL, type(error).__name__)
        return grpc.unary_unary_rpc_method_handler(
            dispatch,
            request_deserializer=BytesValue.FromString,
            response_serializer=BytesValue.SerializeToString,
        )

    methods = {name: handler(name) for name in (
        "Export", "Seed", "GetReplica", "Update", "Batch", "Configure", "GetLive",
        "PreparePromotion", "CommitPromotion", "CancelPromotion",
        "BeginRepair", "RepairSeed", "PrepareRepair", "ReleaseRepair",
        "BeginMigrationFence", "EndMigrationFence", "BeginMigrationEpoch",
        "AbortUnpublishedMigration", "BeginRangeTransfer", "FreezeRangeTransfer",
        "CancelRangeTransfer", "StageIncomingTransfer", "ApplyIncomingDelta",
        "InstallIncomingTransfer", "FinishOutgoingTransfer",
    )}
    grpc_server.add_generic_rpc_handlers((
        grpc.method_handlers_generic_handler(_SERVICE, methods),
    ))


class ReplicaPeer:
    """Remote stand-in for methods used by F2.1–F2.3 manager/server code.

    Transport errors are deliberately propagated. The existing primary
    fails closed (fences affected ranges) on missing backup acknowledgements.
    """

    def __init__(self, address, timeout_s=2.0):
        if not isinstance(address, str) or not address.strip():
            raise ValueError("Replica peer address must be nonempty")
        if (type(timeout_s) not in (float, int) or not math.isfinite(timeout_s)
                or timeout_s <= 0):
            raise ValueError("Replica RPC timeout must be positive and finite")
        self.address = address
        self.owner = address
        self.timeout_s = float(timeout_s)
        self._channel = grpc.insecure_channel(
            address,
            options=[("grpc.max_send_message_length", _MAX_BYTES + 1024),
                     ("grpc.max_receive_message_length", _MAX_BYTES + 1024)],
        )

    def _call(self, method, value):
        stub = self._channel.unary_unary(
            f"/{_SERVICE}/{method}",
            request_serializer=BytesValue.SerializeToString,
            response_deserializer=BytesValue.FromString,
        )
        return unpack(stub(pack(value), timeout=self.timeout_s))

    def export_replica_snapshot(self, partition_id):
        return self._call("Export", partition_id)

    def install_replica_snapshot(self, snapshot):
        return self._call("Seed", snapshot)

    def get_replica_snapshot(self, primary_id, partition_id):
        return self._call("GetReplica", (primary_id, partition_id))

    def apply_replica_update(self, record):
        return self._call("Update", record)

    def apply_replica_batch(self, records):
        return self._call("Batch", records)

    def configure_live_replication(self, targets, batch_size=1, batch_timeout_s=0.05):
        addresses = {}
        for part_id, backups in targets.items():
            if not isinstance(backups, (tuple, list)) or not backups:
                raise ValueError("Missing replica targets")
            if any(not isinstance(p, ReplicaPeer) for p in backups):
                raise TypeError("Remote configuration expects ReplicaPeer backups")
            addresses[part_id] = tuple(p.address for p in backups)
        return self._call("Configure", {
            "targets": addresses,
            "batch_size": batch_size,
            "batch_timeout_s": batch_timeout_s,
        })

    def live_replication_snapshot(self):
        return self._call("GetLive", None)

    def prepare_replica_promotion(self, failed_primary, partition_id, new_table):
        return self._call("PreparePromotion", {
            "failed_primary": failed_primary,
            "partition_id": partition_id,
            "new_table": _pack_table(new_table),
        })

    def commit_replica_promotion(self, failed_primary, partition_id, epoch):
        return self._call("CommitPromotion", {
            "failed_primary": failed_primary,
            "partition_id": partition_id,
            "epoch": epoch,
        })

    def cancel_replica_promotion(self, failed_primary, partition_id, epoch):
        return self._call("CancelPromotion", {
            "failed_primary": failed_primary,
            "partition_id": partition_id,
            "epoch": epoch,
        })

    def begin_replication_repair(self, new_table):
        return self._call("BeginRepair", {"new_table": _pack_table(new_table)})

    def install_repair_replica_snapshot(self, snapshot):
        return self._call("RepairSeed", snapshot)

    def prepare_repaired_replication(
        self, targets, epoch, batch_size=1, batch_timeout_s=0.05,
    ):
        addresses = {}
        for part_id, backups in targets.items():
            if any(not isinstance(b, ReplicaPeer) for b in backups):
                raise TypeError("Repair requires ReplicaPeer backups")
            addresses[part_id] = tuple(b.address for b in backups)
        return self._call("PrepareRepair", {
            "targets": addresses, "epoch": epoch,
            "batch_size": batch_size, "batch_timeout_s": batch_timeout_s,
        })

    def release_repaired_replication(self, epoch):
        return self._call("ReleaseRepair", epoch)

    def begin_migration_fence(self, epoch):
        return self._call("BeginMigrationFence", epoch)

    def end_migration_fence(self, epoch):
        return self._call("EndMigrationFence", epoch)

    def begin_migration_epoch(self, new_table):
        return self._call("BeginMigrationEpoch", _pack_table(new_table))

    def abort_unpublished_migration(self):
        return self._call("AbortUnpublishedMigration", None)

    def begin_range_transfer(self, begin, end):
        return self._call("BeginRangeTransfer", (begin, end))

    def freeze_range_transfer(self):
        return self._call("FreezeRangeTransfer", None)

    def cancel_range_transfer_if_active(self):
        return self._call("CancelRangeTransfer", None)

    def stage_incoming_transfer(self, partition_id, snapshot):
        return self._call("StageIncomingTransfer", {
            "partition_id": partition_id, "snapshot": snapshot,
        })

    def apply_incoming_delta(self, delta):
        return self._call("ApplyIncomingDelta", delta)

    def install_incoming_transfer(self):
        return self._call("InstallIncomingTransfer", None)

    def finish_outgoing_transfer(self, new_table):
        return self._call("FinishOutgoingTransfer", _pack_table(new_table))

    def close(self):
        self._channel.close()
