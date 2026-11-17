
import grpc

import manager_pb2 as pb
import manager_pb2_grpc as pb_grpc

from routing import Partition, RoutingTable, KEY_SPACE_END


class ManagerService(pb_grpc.OwnershipManagerServicer):
    """Expose the authoritative ownership map over gRPC."""

    def __init__(self, manager):
        self.manager = manager

    def GetMap(self, request, context):
        table = self.manager.get_map()

        reply = pb.GetMapReply(epoch=table.epoch)

        for partition in table.partitions:
            at_end = partition.end == KEY_SPACE_END

            reply.partitions.add(
                partition_id=partition.partition_id,
                begin=partition.begin,
                end=0 if at_end else partition.end,
                end_is_keyspace_end=at_end,
                owner=partition.owner,
            )

        return reply
    
    def _get_consistency_gate(self, context):
        """Find the manager's configured consistency gate."""

        gate = self.manager.consistency_gate

        if gate is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Consistency has not been configured",
            )

        return gate

    def CanStart(self, request, context):
        """Check if an iteration may begin."""

        gate = self._get_consistency_gate(context)

        try:
            required = gate.required_iteration(
                request.iteration
            )

            allowed = gate.can_start(
                request.iteration
            )

        except ValueError as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(error),
            )

        return pb.CanStartReply(
            allowed=allowed,
            required_iteration=required,
        )

    def CompleteIteration(self, request, context):
        """Record a worker's completed iteration."""

        gate = self._get_consistency_gate(context)

        try:
            gate.complete(
                request.worker_id,
                request.iteration,
            )

        except ValueError as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(error),
            )

        return pb.CompleteIterationReply(
            completed_iteration=request.iteration,
        )

def fetch_ownership_map(address):
    """Retrieve and validate a manager's ownership map."""

    with grpc.insecure_channel(address) as channel:
        stub = pb_grpc.OwnershipManagerStub(channel)

        reply = stub.GetMap(
            pb.GetMapRequest(),
            timeout=5.0,
        )

    partitions = []

    for entry in reply.partitions:
        if entry.end_is_keyspace_end:
            if entry.end != 0:
                raise ValueError("Invalid maximum-key boundary")
            end = KEY_SPACE_END
        else:
            end = entry.end

        partitions.append(
            Partition(
                partition_id=entry.partition_id,
                begin=entry.begin,
                end=end,
                owner=entry.owner,
            )
        )

    return RoutingTable(partitions, epoch=reply.epoch)

