
import threading
from concurrent import futures
import math
import grpc
import hashlib
import time
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc


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


    def _resolve_keys(self, request, context):
        # Check whether the request uses a dense range.
        if request.HasField("key_range"):

            # A request cannot specify both formats.
            if request.keys:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Specify either keys or key_range, not both"
                )

            begin = request.key_range.begin
            end = request.key_range.end

            if begin > end:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Range begin must not exceed end"
                )

            return range(begin, end)

        # Otherwise, use the explicit sparse key list.
        keys = list(request.keys)

        if len(keys) != len(set(keys)):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Duplicate keys are not supported"
            )

        return keys


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

        if len(gradients) != expected_length:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Incorrect number of gradient values"
            )
            
        # Reject gradients that could corrupt parameter values.
        if any(
            not math.isfinite(gradient)
            for gradient in request.gradients
        ):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Gradients must contain only finite numbers"
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
                    return pb.PushResponse(accepted=True)

            # Prepare every update before modifying stored parameters.
            # Keep this entire section inside the existing server lock.

            prepared_updates = {}

            rule = self.update_rules[self.active_update_rule]

            try:
                for index, key in enumerate(keys):
                    start = index * request.width
                    end = start + request.width

                    gradient_vector = list(
                        request.gradients[start:end]
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


            # Record the completed operation while still locked.
            if task_key is not None:
                self.completed_pushes[task_key] = fingerprint

        return pb.PushResponse(accepted=True)


    def _execute_pull(self, request, context):
        self._wait_for_dependencies(request, context)
        keys = self._resolve_keys(request, context)

        values = []

        with self.lock:
            for key in keys:
                vector = self.weights.get(
                    key,
                    [0.0] * self.value_width
                )
                values.extend(vector)

        return pb.PullResponse(
            keys=keys,
            values=values,
            width=self.value_width
        )


    def Push(self, request, context):
        try:
            response = self._execute_push(request, context)

        except Exception:
            self._finish_task(request, succeeded=False)
            raise

        self._finish_task(request, succeeded=True)
        return response


    def Pull(self, request, context):
        try:
            response = self._execute_pull(request, context)

        except Exception:
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
