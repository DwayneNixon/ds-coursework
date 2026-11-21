"""F3.3a: safe retry of one *unchanged physical* Push across promotion.

The original serialized Push is replayed to a new owner with its SAME Task ID.
This helper deliberately rejects cross-partition requests, cached-key Pushes,
and partition boundary/ID changes. Those need integration with the existing
DistributedParameterClient, not an unsafe repartition-and-replay shortcut.
"""

import math
import time

import grpc


class FailoverPushClient:
    def __init__(
        self, manager_address, *, attempt_timeout_s=0.3,
        retry_interval_s=0.05, overall_timeout_s=10.0,
        on_retry=None,
    ):
        if not isinstance(manager_address, str) or not manager_address.strip():
            raise ValueError("Manager address must be nonempty")
        for name, value in (("attempt_timeout_s", attempt_timeout_s),
                            ("retry_interval_s", retry_interval_s),
                            ("overall_timeout_s", overall_timeout_s)):
            if (type(value) not in (float, int) or
                    not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite")
        if on_retry is not None and not callable(on_retry):
            raise TypeError("on_retry must be callable")
        self.manager_address = manager_address
        self.attempt_timeout_s = float(attempt_timeout_s)
        self.retry_interval_s = float(retry_interval_s)
        self.overall_timeout_s = float(overall_timeout_s)
        self.on_retry = on_retry

    @staticmethod
    def _target(table, request):
        """Return partition and owner without changing the request bytes."""
        if request.use_cached_keys or request.key_cache_id:
            raise ValueError("Failover helper requires uncached explicit keys")
        if request.HasField("key_range"):
            if request.keys:
                raise ValueError("Ambiguous dense and sparse Push")
            routes = table.split_range(
                request.key_range.begin, request.key_range.end,
            )
        else:
            if not request.keys:
                raise ValueError("An explicit nonempty key set is required")
            routes = table.split_keys(list(request.keys))
        if len(routes) != 1:
            raise ValueError("Only a one-partition physical Push is retryable")
        route = routes[0]
        partition = next(p for p in table.partitions
                         if p.partition_id == route.partition_id)
        if route.owner != partition.owner:
            raise ValueError("Inconsistent routing metadata")
        return partition

    @staticmethod
    def _retryable(error):
        code = error.code()
        return (
            code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
            or (code == grpc.StatusCode.FAILED_PRECONDITION
                and (error.details() or "").startswith("NOT_OWNER:"))
        )

    def push(self, request):
        """Retry a byte-identical physical Push until ACK or deadline.

        A successful ACK means the existing replicated commit/dedup protocol
        accepted the Push. Timeout means outcome is uncertain; preserve the
        Task ID and do NOT issue the logical update with a new ID.
        """
        if not request.HasField("task_id") or request.task_id.sequence == 0:
            raise ValueError("Stable nonzero Task ID is required")
        from parameter_server_pb2_grpc import ParameterServerStub
        from manager_rpc import fetch_ownership_map

        # Copy the original request so callers cannot mutate it mid-retry.
        stable = type(request)()
        stable.CopyFrom(request)
        deadline = time.monotonic() + self.overall_timeout_s
        pinned_partition = None
        attempts = 0
        last_error = None
        while time.monotonic() < deadline:
            # Manager is assumed available in the F3 crash-stop model.
            table = fetch_ownership_map(self.manager_address)
            partition = self._target(table, stable)
            signature = (partition.partition_id, partition.begin, partition.end)
            if pinned_partition is None:
                pinned_partition = signature
            elif signature != pinned_partition:
                raise RuntimeError(
                    "Partition shape changed; physical replay would be unsafe"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            attempts += 1
            try:
                with grpc.insecure_channel(partition.owner) as channel:
                    result = ParameterServerStub(channel).Push(
                        stable, timeout=min(self.attempt_timeout_s, remaining),
                    )
                if not result.accepted:
                    raise RuntimeError("Primary rejected Push without a gRPC error")
                return result
            except grpc.RpcError as error:
                if not self._retryable(error):
                    raise
                last_error = error
                if self.on_retry is not None:
                    self.on_retry(attempts, partition.owner, error.code())
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(self.retry_interval_s, remaining))
        reason = (last_error.code().name if last_error is not None
                  else "manager/worker deadline")
        raise TimeoutError(
            f"Push with Task ID ({stable.task_id.worker_id}, "
            f"{stable.task_id.sequence}) remains unacknowledged "
            f"after {attempts} attempt(s): {reason}"
        )
