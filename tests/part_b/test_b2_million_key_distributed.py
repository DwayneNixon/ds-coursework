"""B2: run real gRPC worker traffic against a million-key logical model.

A million logical keys does NOT mean eager allocation of one million entries
in the sparse parameter server. This checks seeded Zipf-scatter sampling,
large hidden targets, two routed shards and cross-shard dense requests.
"""
import math
from concurrent.futures import ThreadPoolExecutor

from harness.local_servers import make_server
from distributed_client import DistributedParameterClient
from harness.oracle import TargetVector
from harness.worker import run_worker
from harness.workload import WorkloadGenerator
from routing import KEY_SPACE_END, Partition, RoutingTable


def main():
    num_keys, width, seed = 1_000_000, 2, 42
    left, left_service, left_addr = make_server()
    right, right_service, right_addr = make_server()
    table = RoutingTable([
        Partition("left", 0, 500_000, left_addr),
        Partition("right", 500_000, KEY_SPACE_END, right_addr),
    ])
    left_service.set_ownership(left_addr, table)
    right_service.set_ownership(right_addr, table)
    left.start()
    right.start()

    clients = [
        DistributedParameterClient(table, worker_id=6010 + i, value_width=width)
        for i in range(2)
    ]
    observer = DistributedParameterClient(table, worker_id=6099,
                                          value_width=width)
    try:
        targets = TargetVector(num_keys, width, seed)
        rows = []
        def worker(index):
            wid = 6010 + index
            workload = WorkloadGenerator(
                num_keys=num_keys, seed=seed, worker_id=wid,
                mode="sparse", keys_per_iter=128,
                scatter=True, zipf_s=1.1,
                compute_median_ms=0,
            )
            run_worker(
                client=clients[index], worker_id=wid,
                iterations=2, num_keys=num_keys, keys_per_iter=128,
                width=width, targets=targets, seed=seed,
                workload=workload, on_operation=rows.append,
            )
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker, i) for i in range(2)]
            for future in futures:
                future.result()
        assert len(rows) == 8, len(rows)
        assert all(r["status"] == "OK" for r in rows)
        assert left_service.weights and right_service.weights, (
            "Skew-scatter workload did not contact both shards"
        )
        assert all(0 <= key < num_keys for key in
                   (*left_service.weights, *right_service.weights))

        # Dense range spanning the 500,000 boundary must route to both shards
        # without materialising the million-key *model* into dense RPC lists.
        interval = range(499_900, 500_100)
        old = list(observer.pull(interval).wait(timeout_ms=5000).values)
        assert len(old) == len(interval) * width
        changes = [0.5, -0.25] * len(interval)
        task = observer.push(interval, changes, width=width)
        assert task.wait(timeout_ms=5000).accepted
        new = list(observer.pull(interval).wait(timeout_ms=5000).values)
        assert len(new) == len(old)
        assert all(math.isclose(got, before - 0.1 * grad, abs_tol=1e-10)
                   for got, before, grad in zip(new, old, changes))
        print("B2 1,000,000-key logical model, two gRPC shards: PASS")
    finally:
        for client in clients:
            client.close()
        observer.close()
        left.stop(0).wait()
        right.stop(0).wait()


if __name__ == "__main__":
    main()
