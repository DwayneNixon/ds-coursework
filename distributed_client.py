
import threading
from types import SimpleNamespace
from parameter_client import normalize_required_clock
from parameter_client import (
    ParameterClient,
    Task,
    DependencyFailedError,
)
from routing import split_push, merge_pull
from manager_rpc import fetch_ownership_map
import grpc

def is_not_owner(error):
    return (
        isinstance(error, grpc.RpcError)
        and error.code() == grpc.StatusCode.FAILED_PRECONDITION
        and (error.details() or "").startswith("NOT_OWNER:")
    )

class DistributedTask(Task):
    """One logical task containing multiple server tasks."""

    def __init__(
        self, operation, target, routes, tasks, width,
        deps=None, client=None, chunks=None, iteration=None, required_clock=None
    ):
        super().__init__()

        self.operation = operation
        self.target = target
        self.routes = routes
        self.tasks = tasks
        self.width = width
        self._client = client
        self._chunks = chunks if chunks is not None else {}
        self._failures = {}
        self._iteration = iteration
        self._required_clock = dict(required_clock or {})

        self._remaining = len(tasks)
        self._replies = {}
        self._failure = None
        self._lock = threading.Lock()

        if not tasks:
            self._complete_empty(deps or [])
            return

        # A completed server task notifies the logical task.
        for route, task in tasks:
            task._future.add_done_callback(
                lambda future, r=route:
                    self._subtask_finished(r, future)
            )


    def _logical_result(self, replies):
        """Combine the completed subrequests into one result."""

        clocks = []

        # Every successful physical response contributes
        # its per-range clock information.
        for route, subtask in self.tasks:
            try:
                response = subtask.wait(timeout_ms=0)
            except Exception:
                # An original NOT_OWNER attempt may have
                # been recovered by a later subrequest.
                continue

            clocks.extend(response.clocks)

        if self.operation == "push":
            return SimpleNamespace(
                accepted=True,
                clocks=clocks,
            )

        values = merge_pull(
            self.target,
            self.routes,
            replies,
            self.width,
        )

        return SimpleNamespace(
            values=values,
            width=self.width,
            clocks=clocks,
        )

    def _finish_success(self, replies):
        try:
            result = self._logical_result(replies)
        except Exception as error:
            self._future.set_exception(error)
        else:
            self._future.set_result(result)

    
    def _subtask_finished(self, route, future):
        try:
            response = future.result()
            failure = None
        except Exception as error:
            response = None
            failure = error

        with self._lock:
            if failure is not None:
                self._failures[route] = failure

            elif self.operation == "pull":
                self._replies[route] = list(response.values)

            self._remaining -= 1

            if self._remaining != 0:
                return

            failures = dict(self._failures)
            replies = dict(self._replies)

        # All original subrequests succeeded.
        if not failures:
            self._finish_success(replies)
            return

        # Do not automatically retry uncertain failures.
        other_errors = [
            error for error in failures.values()
            if not is_not_owner(error)
        ]

        if other_errors:
            self._future.set_exception(other_errors[0])
            return

        # Static clients cannot refresh ownership.
        if (
            self._client is None
            or self._client.manager_address is None
        ):
            self._future.set_exception(
                next(iter(failures.values()))
            )
            return

        # Recovery requires another manager request,
        # so avoid blocking a gRPC completion callback.
        threading.Thread(
            target=self._retry_rejected,
            args=(tuple(failures),),
            daemon=True,
        ).start()


    def _retry_rejected(self, failed_routes):
        """Retry only confirmed NOT_OWNER subrequests."""

        try:
            changed = self._client.refresh_map()

            if not changed:
                raise RuntimeError(
                    "NOT_OWNER but no newer ownership map is available"
                )

            retry_groups = []

            # Submit every retry before waiting.
            for original_route in failed_routes:
                target = self._client._subtarget(original_route)
                new_routes = self._client._routes(target)

                if self.operation == "push":
                    chunks = split_push(
                        target,
                        self._chunks[original_route],
                        new_routes,
                        self.width,
                    )

                new_tasks = []

                for new_route in new_routes:
                    client = self._client.clients[new_route.owner]
                    subtarget = self._client._subtarget(new_route)

                    if self.operation == "push":
                        subtask = client.push(
                        subtarget,
                        chunks[new_route],
                        width=self.width,
                        iteration=self._iteration,
                        required_clock=self._required_clock,
                    )
                    else:
                        subtask = client.pull(
                        subtarget,
                        required_clock=self._required_clock,
                    )

                    # Keep the physical attempts for metrics.
                    self.tasks.append((new_route, subtask))
                    new_tasks.append((new_route, subtask))

                retry_groups.append(
                    (original_route, target, new_routes, new_tasks)
                )

            # Collect the retry results.
            for original_route, target, routes, tasks in retry_groups:
                replies = {}

                for route, task in tasks:
                    response = task.wait(timeout_ms=5000)

                    if self.operation == "pull":
                        replies[route] = list(response.values)

                if self.operation == "pull":
                    self._replies[original_route] = merge_pull(
                        target,
                        routes,
                        replies,
                        self.width,
                    )

            # Previously successful shards are preserved.
            self._finish_success(dict(self._replies))

        except Exception as error:
            self._future.set_exception(error)


    def _complete_empty(self, deps):
        """Empty operations still respect dependencies."""

        if not deps:
            self._finish_success({})
            return

        remaining = len(deps)
        failed = False
        lock = threading.Lock()

        def dependency_finished(future):
            nonlocal remaining, failed

            try:
                future.result()
                failed_now = False
            except Exception:
                failed_now = True

            with lock:
                remaining -= 1
                failed = failed or failed_now
                finished = remaining == 0
                any_failed = failed

            if finished:
                if any_failed:
                    self._future.set_exception(
                        DependencyFailedError(
                            "DEPENDENCY_FAILED: prerequisite failed"
                        )
                    )
                else:
                    self._finish_success({})

        for dep in deps:
            dep._future.add_done_callback(
                dependency_finished
            )



class DistributedParameterClient:
    def __init__(self, routing_table, worker_id, value_width=2, required_clock=None):
        if type(value_width) is not int or value_width < 1:
            raise ValueError("Invalid vector width")

        self.table = routing_table
        self.value_width = value_width
        self.worker_id = worker_id
        self.manager_address = None
        self._required_clock = dict(required_clock or {})

        # Reuse the already-tested Part A clients.
        owners = {
            partition.owner
            for partition in routing_table.partitions
        }

        self.clients = {
            owner: ParameterClient(
                owner, worker_id=worker_id
            )
            for owner in owners
        }

    def _routes(self, target):
        if isinstance(target, range):
            if target.step != 1:
                raise ValueError("Range step must be 1")

            return self.table.split_range(
                target.start, target.stop
            )

        return self.table.split_keys(target)

    def _subtarget(self, route):
        if hasattr(route, "keys"):
            return list(route.keys)

        return range(route.begin, route.end)


    def push(
        self, target, gradients, width, deps=None,
        iteration=None, required_clock=None,
    ):
        required_clock = normalize_required_clock(
            required_clock
        )
        deps = self._check_deps(deps)

        if iteration is not None:
            if (
                type(iteration) is not int
                or not 1 <= iteration < (1 << 64)
            ):
                raise ValueError(
                    "Iteration must be a positive uint64 integer"
                )

            if self.worker_id is None:
                raise ValueError(
                    "Explicit iterations require a worker ID"
                )
            
        if width != self.value_width:
            raise ValueError("Vector width mismatch")

        routes = self._routes(target)
        chunks = split_push(
            target, gradients, routes, width
        )

        tasks = []

        for route in routes:
            task = self.clients[route.owner].push(
                self._subtarget(route),
                chunks[route],
                width=width,
                deps=deps,
                iteration=iteration,
                required_clock=required_clock
            )
            tasks.append((route, task))

        return DistributedTask(
            "push",
            target,
            routes,
            tasks,
            width,
            deps,
            client=self,
            chunks=chunks,
            iteration=iteration,
            required_clock=required_clock,
        )

  
    def pull(self, target, deps=None, required_clock=None):
        required_clock = normalize_required_clock(required_clock)
        deps = self._check_deps(deps)

        routes = self._routes(target)
        tasks = []

        for route in routes:
            task = self.clients[route.owner].pull(
                self._subtarget(route),
                deps=deps,
                required_clock=required_clock,  # FIX
            )
            tasks.append((route, task))

        return DistributedTask(
            "pull",
            target,
            routes,
            tasks,
            self.value_width,
            deps,
            client=self,
            required_clock=required_clock,
        )

    def _check_deps(self, deps):
        deps = list(deps) if deps is not None else []

        if any(not isinstance(dep, Task) for dep in deps):
            raise TypeError("Dependencies must be Task objects")

        if len({id(dep) for dep in deps}) != len(deps):
            raise ValueError("Duplicate dependencies")

        # Avoid forwarding a server-specific Task ID
        # to another server that cannot recognize it.
        if any(dep.task_id is not None for dep in deps):
            raise ValueError(
                "Use logical distributed tasks as dependencies"
            )

        return deps


    @classmethod
    def from_manager(cls, manager_address, worker_id, value_width=2):
        """Create a distributed client using manager discovery."""

        table = fetch_ownership_map(manager_address)

        client = cls(
            table,
            worker_id=worker_id,
            value_width=value_width,
        )

        client.manager_address = manager_address
        return client

    def refresh_map(self):
        """Refresh the cached ownership map from the manager."""

        if self.manager_address is None:
            raise ValueError(
                "This client was not connected through a manager"
            )

        new_table = fetch_ownership_map(self.manager_address)

        # Never replace a newer cached map with an older one.
        if new_table.epoch < self.table.epoch:
            raise ValueError("Manager returned an older ownership map")

        if new_table.epoch == self.table.epoch:
            if new_table.partitions != self.table.partitions:
                raise ValueError(
                    "Ownership changed without an epoch increment"
                )

            return False

        # Open connections to newly discovered owners.
        for partition in new_table.partitions:
            owner = partition.owner

            if owner not in self.clients:
                self.clients[owner] = ParameterClient(
                    owner,
                    worker_id=self.worker_id,
                )

        # Publish the new map locally only after preparing clients.
        self.table = new_table

        return True

    def close(self):
        for client in self.clients.values():
            client.close()
