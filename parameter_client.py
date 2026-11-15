
import grpc
import threading
import parameter_server_pb2 as pb
import parameter_server_pb2_grpc as pb_grpc

from concurrent.futures import Future, TimeoutError as FutureTimeoutError

class TaskWaitTimeout(TimeoutError):
    """Raised when waiting for a task takes too long."""
    pass



class DependencyFailedError(RuntimeError):
    """Raised when a prerequisite task fails."""
    pass


class Task:
    """An asynchronous operation that may be pending or running."""
    
    
    def __init__(self, task_id=None):
        self._future = Future()
        self.task_id = task_id

        # Used to retry an unsuccessful push.
        self._push_request_bytes = None
        self._owner = None

    def done(self):
        return self._future.done()

    def wait(self, timeout_ms=None):
        if timeout_ms is not None and timeout_ms < 0:
            raise ValueError("Timeout cannot be negative")

        timeout_seconds = (
            None if timeout_ms is None
            else timeout_ms / 1000
        )

        try:
            return self._future.result(
                timeout=timeout_seconds
            )
        except FutureTimeoutError as error:
            raise TaskWaitTimeout(
                "TIMEOUT: task has not completed"
            ) from error


class ParameterClient:
    """Client interface for accessing the parameter server."""

    
    def __init__(self, address="127.0.0.1:50051", worker_id=None):

        if worker_id is not None:
            if type(worker_id) is not int or not (0 <= worker_id < 2**32):
                raise ValueError("worker_id must be a uint32 integer")

        self.worker_id = worker_id

        # Sequence numbers start at 1 for each worker.
        self._sequence = 0
        self._pull_sequence = 0

        # Protect sequence allocation between threads.
        self._sequence_lock = threading.Lock()

        self.channel = grpc.insecure_channel(address)
        self.stub = pb_grpc.ParameterServerStub(self.channel)
    

    def _next_task_id(self):
        """Generate the next unique Push Task ID."""

        if self.worker_id is None:
            return None

        with self._sequence_lock:
            if self._sequence >= (1 << 63) - 1:
                raise OverflowError("Push task sequence exhausted")

            self._sequence += 1

            return (self.worker_id, self._sequence)


    def _target_fields(self, target):
        """Convert Python key targets into protobuf fields."""

        # Dense access: range(10, 13)
        if isinstance(target, range):
            if target.step != 1:
                raise ValueError("Range step must be 1")

            if target.start < 0 or target.stop < 0:
                raise ValueError("Keys cannot be negative")

            return {
                "key_range": pb.KeyRange(
                    begin=target.start,
                    end=target.stop
                )
            }

        # Sparse access: [10, 12, 15]
        if isinstance(target, (list, tuple)):
            return {"keys": list(target)}

        raise TypeError(
            "Target must be a list, tuple, or range"
        )

    
    
    def _schedule(self, rpc_method, request, deps=None, task_id=None):
        """Start an RPC after all prerequisite tasks succeed."""

        if deps is None:
            deps = []

        if any(not isinstance(dep, Task) for dep in deps):
            raise TypeError("Dependencies must be Task objects")

        if len({id(dep) for dep in deps}) != len(deps):
            raise ValueError("Duplicate dependencies are not allowed")

        task = Task(task_id=task_id)

        def rpc_finished(rpc_future):
            """Store the RPC result or failure in our Task."""
            try:
                response = rpc_future.result()
            except Exception as error:
                task._future.set_exception(error)
            else:
                task._future.set_result(response)

        def send_request():
            """Submit the RPC to the server."""
            try:
                rpc_future = rpc_method.future(request)
                rpc_future.add_done_callback(rpc_finished)
            except Exception as error:
                task._future.set_exception(error)

        # Independent tasks can start immediately.
        if not deps:
            send_request()
            return task

        # Shared state for all dependency callbacks.
        remaining = len(deps)
        resolved = False
        lock = threading.Lock()

        def dependency_finished(completed_future):
            nonlocal remaining, resolved

            # Determine whether this prerequisite succeeded.
            try:
                completed_future.result()
                succeeded = True
            except Exception:
                succeeded = False

            should_send = False
            should_fail = False

            with lock:
                # Another callback may have already decided
                # whether this task should start or fail.
                if resolved:
                    return

                if not succeeded:
                    resolved = True
                    should_fail = True

                else:
                    remaining -= 1

                    if remaining == 0:
                        resolved = True
                        should_send = True

            # Never execute these while holding the lock.
            if should_fail:
                task._future.set_exception(
                    DependencyFailedError(
                        "DEPENDENCY_FAILED: prerequisite failed"
                    )
                )

            elif should_send:
                send_request()

        # Register a callback on every prerequisite.
        for dependency in deps:
            dependency._future.add_done_callback(
                dependency_finished
            )

        return task
    
    
    
    def push(self, target, gradients, width, deps=None):
        """Submit a new logical push with its own Task ID."""

        fields = self._target_fields(target)

        request = pb.PushRequest(
            gradients=gradients,
            width=width,
            **fields
        )

        # Generate a unique Push Task ID.
        task_id = self._next_task_id()

        if task_id is not None:
            request.task_id.CopyFrom(
                pb.TaskId(
                    worker_id=task_id[0],
                    sequence=task_id[1]
                )
            )

        # Include prerequisite IDs for server validation.
        self._attach_dependency_ids(request, deps)

        # Preserve the complete request for safe retries.
        request_bytes = request.SerializeToString(
            deterministic=True
        )

        task = self._schedule(
            self.stub.Push,
            request,
            deps=deps,
            task_id=task_id
        )

        task._push_request_bytes = request_bytes
        task._owner = self

        return task



    def retry_push(self, original_task):
        """Retry an unsuccessful push using its original Task ID."""

        # Only retry pushes created by this client.
        if (
            not isinstance(original_task, Task)
            or original_task._owner is not self
            or original_task._push_request_bytes is None
            or original_task.task_id is None
        ):
            raise ValueError(
                "Expected a push task created by this client "
                "with a valid Task ID"
            )

        # Do not duplicate an RPC that is still running.
        if not original_task.done():
            raise ValueError(
                "Original push is still running; wait for completion"
            )

        # Inspect why the original attempt finished.
        failure = original_task._future.exception()

        if failure is None:
            raise ValueError(
                "Original push already succeeded"
            )

        # Initially support only potentially retryable RPC errors.
        retryable_codes = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        }

        if (
            not isinstance(failure, grpc.RpcError)
            or failure.code() not in retryable_codes
        ):
            raise ValueError(
                "Original push did not fail with a retryable RPC error"
            )

        # Reconstruct the exact original request.
        request = pb.PushRequest()
        request.ParseFromString(
            original_task._push_request_bytes
        )

        # Submit another RPC attempt with the SAME Task ID.
        retry_task = self._schedule(
            self.stub.Push,
            request,
            task_id=original_task.task_id
        )

        # Allow another explicit retry if this attempt also fails.
        retry_task._push_request_bytes = (
            original_task._push_request_bytes
        )
        retry_task._owner = self

        return retry_task
 
    def pull(self, target, deps=None):
        """Start a pull after its optional dependencies succeed."""

        fields = self._target_fields(target)
        request = pb.PullRequest(**fields)

        task_id = self._next_pull_task_id()

        if task_id is not None:
            request.task_id.CopyFrom(
                pb.TaskId(
                    worker_id=task_id[0],
                    sequence=task_id[1]
                )
            )

        self._attach_dependency_ids(request, deps)

        return self._schedule(
            self.stub.Pull,
            request,
            deps=deps,
            task_id=task_id
        )

    def _next_pull_task_id(self):
        """Allocate Pull IDs without changing Push numbering."""

        if self.worker_id is None:
            return None

        with self._sequence_lock:
            if self._pull_sequence >= (1 << 63) - 1:
                raise OverflowError("Pull task sequence exhausted")

            self._pull_sequence += 1

            return (
                self.worker_id,
                (1 << 63) + self._pull_sequence
            )


    def _attach_dependency_ids(self, request, deps):
        """Include known prerequisites in the RPC request."""

        for dependency in deps or []:
            if not isinstance(dependency, Task):
                raise TypeError("Dependencies must be Task objects")

            if dependency.task_id is not None:
                worker_id, sequence = dependency.task_id

                request.dependencies.add(
                    worker_id=worker_id,
                    sequence=sequence
                )

    def register_update_rule(self, name, gradient_scale):
        """Register a named linear rule on the server."""

        request = pb.RegisterUpdateRuleRequest(
            name=name,
            gradient_scale=gradient_scale
        )

        response = self.stub.RegisterUpdateRule(
            request,
            timeout=5.0
        )

        return response.registered

    def set_update_rule(self, name):
        """Select a registered server-side update rule."""

        request = pb.SetUpdateRuleRequest(
            name=name
        )

        response = self.stub.SetUpdateRule(
            request,
            timeout=5.0
        )

        return response.active_rule


    def close(self):
        """Close the connection to the server."""
        self.channel.close()
