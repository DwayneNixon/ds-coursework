"""X2 real-process 1–8-server scaling with the shared Part B harness.

Run: python -m experiments.x2_scaling
Optional: --seeds 42 --output results/x2_smoke
Each server is an independent OS process with localhost gRPC; no replication.
"""
import argparse
import csv
import hashlib
import json
import math
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import manager_pb2_grpc as manager_grpc
from distributed_client import DistributedParameterClient
from harness.metrics import (make_summary, write_ops_csv, write_iterations_csv,
                             write_convergence_csv, write_summary_json)
from harness.observer import ConvergenceObserver
from harness.oracle import TargetVector
from harness.worker import run_worker, sample_keys
from manager_rpc import ManagerService
from routing import KEY_SPACE_END, Partition, RoutingTable
from server_manager import ServerManager

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "x2_scaling.json"


def free_address(used):
    for _ in range(20):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            address = f"127.0.0.1:{sock.getsockname()[1]}"
        if address not in used:
            used.add(address)
            return address
    raise RuntimeError("Could not allocate distinct localhost ports")


def workload_digest(cfg, seed):
    """Same worker/key samples for each server count (regardless of timing)."""
    digest = hashlib.sha256()
    for worker in cfg["workers"]["ids"]:
        rng = random.Random(f"{seed}:{worker}:keys")
        for iteration in range(cfg["workers"]["iterations"]):
            sampled = sample_keys(rng, cfg["model"]["num_keys"],
                                  cfg["access"]["keys_per_iter"])
            digest.update(json.dumps((worker, iteration, sampled)).encode())
    return digest.hexdigest()


def stop_cluster(control, processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=4)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=4)
    control.stop(0).wait()


def start_cluster(count, logs_dir, num_keys):
    if count > num_keys:
        raise ValueError("Need at least one measured key per server")
    used = set()
    addresses = [free_address(used) for _ in range(count)]
    partitions = [
        # Balance the *measured model keys* over the servers; dividing the
        # entire uint64 keyspace would put all low-ID keys on server zero.
        Partition(f"p{i}", (num_keys * i) // count,
                  (num_keys * (i + 1)) // count if i + 1 < count
                  else KEY_SPACE_END, address)
        for i, address in enumerate(addresses)
    ]
    table = RoutingTable(partitions)
    manager = ServerManager(partitions)
    control = grpc.server(ThreadPoolExecutor(max_workers=16))
    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager), control)
    port = control.add_insecure_port("127.0.0.1:0")
    if port <= 0:
        raise RuntimeError("Failed to bind ownership manager")
    control.start()
    processes = []
    try:
        for i, address in enumerate(addresses):
            logfile = Path(logs_dir) / f"server_{i}.log"
            with logfile.open("w", encoding="utf-8") as log:
                process = subprocess.Popen(
                    [sys.executable, "-u", str(ROOT / "server.py"),
                     "--listen", address, "--manager", f"127.0.0.1:{port}",
                     "--bootstrap-ownership"],
                    cwd=str(ROOT), stdin=subprocess.DEVNULL,
                    stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
        for i, address in enumerate(addresses):
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=12)
                if processes[i].poll() is not None:
                    raise RuntimeError("Server exited")
            except Exception as error:
                log = (Path(logs_dir) / f"server_{i}.log").read_text()
                raise RuntimeError(f"Server {i} startup failed: {log}") from error
        return table, control, processes
    except BaseException:
        stop_cluster(control, processes)
        raise


def run_trial(cfg, count, seed, root):
    with tempfile.TemporaryDirectory(prefix="x2_scaling_") as logs:
        table, control, processes = start_cluster(
            count, logs, cfg["model"]["num_keys"]
        )
        clients = []
        observer_client = None
        try:
            model = cfg["model"]
            width, num_keys = model["value_width"], model["num_keys"]
            workers = cfg["workers"]
            ids, iterations = workers["ids"], workers["iterations"]
            targets = TargetVector(num_keys, width, seed)
            clients = [DistributedParameterClient(table, worker_id=wid,
                       value_width=width) for wid in ids]
            observer_client = DistributedParameterClient(
                table, worker_id=150000, value_width=width)
            observer = ConvergenceObserver(observer_client, targets,
                                           num_keys, width, batch_size=128)
            observer.sample(0)
            operations, iteration_rows = [], []
            lock = threading.Lock()
            gate = threading.Barrier(len(ids))

            def on_operation(record):
                with lock:
                    operations.append(record)

            def on_iteration(record):
                with lock:
                    iteration_rows.append(record)

            def worker(wid, client):
                gate.wait(timeout=20)
                run_worker(
                    client=client, worker_id=wid, iterations=iterations,
                    num_keys=num_keys,
                    keys_per_iter=cfg["access"]["keys_per_iter"],
                    width=width, targets=targets, seed=seed,
                    compute_ms=workers["compute_ms"],
                    gradient_mode="target_diff",
                    on_operation=on_operation,
                    on_iteration_record=on_iteration,
                )

            before = time.perf_counter()
            with ThreadPoolExecutor(max_workers=len(ids)) as pool:
                futures = [pool.submit(worker, wid, client)
                           for wid, client in zip(ids, clients)]
                for future in futures:
                    future.result()
            elapsed = time.perf_counter() - before
            observer.sample(iterations)

            expected = len(ids) * iterations
            pushes = [r for r in operations if
                      r["op"] == "push" and r["status"] == "OK"]
            if (len(pushes) != expected
                    or len(operations) != expected * 2
                    or len(iteration_rows) != expected
                    or not all(r["status"] == "OK" for r in operations)):
                raise AssertionError("X2 operations/iterations incomplete")
            summary = make_summary(operations, observer.records)
            output = root / f"servers_{count:02d}_seed_{seed}"
            output.mkdir(parents=True, exist_ok=True)
            write_ops_csv(operations, output)
            write_iterations_csv(iteration_rows, output)
            write_convergence_csv(observer.records, output)
            row = {
                "servers": count, "seed": seed,
                "num_keys": num_keys,
                "keys_per_iter": cfg["access"]["keys_per_iter"],
                "workers": len(ids), "iterations_per_worker": iterations,
                "completed_pushes": expected,
                "elapsed_s": elapsed,
                "throughput_pushes_per_s": expected / elapsed,
                "p99_push_latency_ms": summary["p99_push_latency_ms"],
                "p99_pull_latency_ms": summary["p99_pull_latency_ms"],
                "total_payload_bytes": summary["total_bytes"],
                "final_distance": summary["final_distance"],
                "workload_sha256": workload_digest(cfg, seed),
            }
            summary.update(row)
            summary.update({
                "experiment": "X2_server_scaling", "replication": 0,
                "consistency": "eventual",
                "process_isolation": "independent_server_processes",
            })
            write_summary_json(summary, output)
            return row
        finally:
            for client in clients:
                client.close()
            if observer_client is not None:
                observer_client.close()
            stop_cluster(control, processes)


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot(rows, root):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    counts = [r["servers"] for r in rows]
    rates = [r["throughput_median"] for r in rows]
    latencies = [r["push_p99_median_ms"] for r in rows]
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.5))
    for ax, values, name, ylabel in (
        (axs[0], rates, "X2: throughput vs servers", "Completed Pushes / s"),
        (axs[1], latencies, "X2: p99 vs servers", "Push p99 (ms)"),
    ):
        ax.plot(counts, values, marker="o")
        ax.set(title=name, xlabel="Server processes", ylabel=ylabel,
               xticks=counts)
        ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(root / "scaling.png", dpi=160)
    plt.close(fig)


def run(config, output_dir, seeds=None):
    if (config.get("experiment") != "X2_server_scaling"
            or config["cluster"]["server_counts"] != list(range(1, 9))
            or config["cluster"]["replication"] != 0
            or config["consistency"]["mode"] != "eventual"
            or config["gradient"] != {"type": "target_diff", "noise_std": 0}):
        raise ValueError("Expected 1–8-server X2 configuration without failures")
    if (config["model"]["value_width"] != 2
            or config["model"]["learning_rate"] != 0.1
            or config["model"]["num_keys"] < 1
            or config["workers"]["iterations"] < 1):
        raise ValueError("Invalid model/worker configuration")
    ids = config["workers"]["ids"]
    nkeys = config["model"]["num_keys"]
    if (len(ids) < 1 or len(set(ids)) != len(ids)
            or not 1 <= config["access"]["keys_per_iter"] <= nkeys):
        raise ValueError("Invalid worker IDs or access configuration")
    seeds = list(seeds or config["seeds"])
    if not seeds or len(set(seeds)) != len(seeds):
        raise ValueError("Seeds must be nonempty and unique")
    root = Path(output_dir)
    if not root.is_absolute():
        root = ROOT / root
    root.mkdir(parents=True, exist_ok=True)
    trials = []
    for count in range(1, 9):
        for seed in seeds:
            print(f"X2 servers={count}, seed={seed}", flush=True)
            trials.append(run_trial(config, count, seed, root))
    for seed in seeds:
        digests = {r["workload_sha256"] for r in trials if r["seed"] == seed}
        if len(digests) != 1:
            raise AssertionError("Workload changed between server counts")
    aggregate = []
    for count in range(1, 9):
        group = [r for r in trials if r["servers"] == count]
        aggregate.append({
            "servers": count, "trials": len(group),
            "throughput_median": statistics.median(
                r["throughput_pushes_per_s"] for r in group),
            "push_p99_median_ms": statistics.median(
                r["p99_push_latency_ms"] for r in group),
            "pull_p99_median_ms": statistics.median(
                r["p99_pull_latency_ms"] for r in group),
        })
    write_csv(root / "trials.csv", trials)
    write_csv(root / "scaling.csv", aggregate)
    plot(aggregate, root)
    result = {
        "experiment": "X2_server_scaling",
        "server_counts": list(range(1, 9)), "seeds": seeds,
        "replication": 0, "consistency": "eventual",
        "cluster": "one localhost manager, independent Python server processes",
        "throughput_ratio_8_to_1": (
            aggregate[-1]["throughput_median"] /
            aggregate[0]["throughput_median"]),
        "data_files": ["trials.csv", "scaling.csv", "scaling.png"],
        "aggregate": aggregate,
        "caveat": (
            "Single-host shared CPU/memory and increasing RPC fan-out/"
            "reassembly cost limit interpretation as true multi-host scale."
        ),
    }
    (root / "summary.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=None)
    parser.add_argument("--seeds", nargs="+", type=int)
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    result = run(cfg, args.output or cfg["output"]["dir"], args.seeds)
    print("X2 scaling 1–8 independent server processes: PASS")
    print("8-to-1 throughput ratio:", result["throughput_ratio_8_to_1"])


if __name__ == "__main__":
    main()
