
import threading
from concurrent import futures
import math
import grpc
import hashlib
import time
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc
from range_clocks import RangeClocks
from routing import KEY_SPACE_END
from collections import OrderedDict
from value_compression import compress_doubles, decompress_doubles

def sgd_update(weights, gradients, learning_rate):
    """Apply SGD to one parameter vector."""

    return [
        weight - learning_rate * gradient
        for weight, gradient in zip(weights, gradients)
    ]

class ParameterServer(pb_grpc.ParameterServerServicer):

    def __init__(self, value_width=2):
        if value_width < 1:
            raise ValueError("Vector width must be at least 1")

        self.value_width = value_width
        self.weights = {}
        self.learning_rate = 0.1
        self.lock = threading.Lock()
        # Maps completed task IDs to request fingerprints.
        self.completed_pushes = {}

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


    def set_ownership(self, owner, table):
        """Install an ownership map for this server."""

        with self.lock:
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

            return range(begin, end)

        # Existing sparse-key behaviour.
        keys = list(request.keys)

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

        with self.lock:
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

            # COMMIT PHASE:
            # Only reached when ALL calculations have succeeded.
            self.weights.update(prepared_updates)
            self.range_clocks.update(prepared_clocks)
            self.task_condition.notify_all()


            # Record the completed operation while still locked.
            if task_key is not None:
                self.completed_pushes[task_key] = fingerprint

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


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16)
    )

    pb_grpc.add_ParameterServerServicer_to_server(
        ParameterServer(),
        server
    )

    server.add_insecure_port("127.0.0.1:50051")

    server.start()
    print("Parameter server listening on port 50051")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
