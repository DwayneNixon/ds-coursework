"""F5.1 / X6: fixed-gradient crash recovery with a live multi-process cluster.

Run from the repository root (after F4.2):
    python run_f51_x6.py --output results/f51_x6

This uses the existing F3.3b distributed worker, F3.3a controller, F4.1
manager, F3.4 servers and B3 fixed_gradient. It does not modify them.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import grpc
import manager_pb2_grpc as manager_grpc

from distributed_client import DistributedParameterClient
from failover_controller import AutomaticFailoverController
from Part_F_Tests.f5_evidence import (ExperimentEvidence, compare_weights,
                         fixed_gradient_oracle, recovery_decomposition,
                         throughput_recovery)
from harness.oracle import fixed_gradient
from manager_rpc import ManagerService
from replica_transport import ReplicaPeer
from routing import KEY_SPACE_END, Partition
from server_manager import ServerManager


def free_address(excluded):
    for _ in range(40):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            address = "127.0.0.1:" + str(sock.getsockname()[1])
        if address not in excluded:
            excluded.add(address)
            return address
    raise RuntimeError("Unable to reserve unique test ports")


def wait_until(predicate, seconds=8.0):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.025)
    return bool(predicate())


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    lower = int(position)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[min(lower + 1, len(ordered) - 1)] * fraction


def wire_bytes(task, operation):
    """Worker-side Protobuf payload sizes; excludes replication and TCP headers."""
    out_bytes = 0
    in_bytes = 0
    for _, physical in getattr(task, "tasks", []):
        attempts = getattr(physical, "_request_attempts", None)
        if attempts:
            out_bytes += sum(len(payload) for payload in attempts)
        else:
            request = getattr(physical, f"_{operation}_request_bytes", None)
            if request is not None:
                out_bytes += len(request)
        in_bytes += int(getattr(physical, "_response_wire_bytes", 0) or 0)
        in_bytes += int(getattr(physical, "_extra_response_bytes", 0) or 0)
    return out_bytes, in_bytes


def run(output_dir, *, seed=42, pre_fault_s=6.2, post_fault_s=6.2,
        compute_ms=25, workers=(101, 102), batch_size=1):
    """Run one X6 trial and return the tested summary, writing §5.7 evidence."""
    if pre_fault_s < 5.1 or post_fault_s < 3:
        raise ValueError("Need >=5 s pre-fault baseline and enough post-fault observations")
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("Invalid batch size")
    if len(set(workers)) != len(workers) or not workers:
        raise ValueError("Worker IDs must be unique and nonempty")

    # Independent fixed-gradient trial; the supplied seed is recorded. The
    # selected key set is deliberately constant so range ownership never
    # changes during this X6 failover-only experiment.
    third = KEY_SPACE_END // 3
    keys = (7 + seed % 11,
            third + 7 + (seed // 11) % 11,
            2 * third + 7 + (seed // 121) % 11)
    used = set()
    server_ids = tuple(free_address(used) for _ in range(3))
    primary, second, third_server = server_ids
    evidence = ExperimentEvidence()
    manager = ServerManager([
        Partition("p0", 0, third, primary),
        Partition("p1", third, 2 * third, second),
        Partition("p2", 2 * third, KEY_SPACE_END, third_server),
    ], heartbeat_timeout_s=0.50)
    control = grpc.server(ThreadPoolExecutor(max_workers=8))
    manager_grpc.add_OwnershipManagerServicer_to_server(
        ManagerService(manager), control)
    port = control.add_insecure_port("127.0.0.1:0")
    if not port:
        raise RuntimeError("Manager could not bind to a port")
    manager_address = f"127.0.0.1:{port}"
    processes = {}
    peers = {}
    controller = None
    clients = []
    stopped = threading.Event()
    arm_fault = threading.Event()
    mid_iteration = threading.Event()
    fault_injected = threading.Event()
    failures = []
    server_program = Path(__file__).resolve().parents[1] / "server.py"
    if not server_program.is_file():
        raise FileNotFoundError(f"Missing repository server entrypoint: {server_program}")

    # The initial nonfailed primary is deliberately killed between a
    # worker's Pull and Push, so this is genuinely mid-iteration.
    def record_operation(worker, iteration, op, task, t0, error=None):
        t1 = time.monotonic()
        out_bytes, in_bytes = wire_bytes(task, op)
        attempts = len(getattr(task, "tasks", ()))
        originals = len(getattr(task, "routes", ()))
        evidence.add_op(
            ts_ns=time.time_ns(), worker=worker, iteration=iteration,
            op=op, n_keys=len(keys), bytes_out=out_bytes,
            bytes_in=in_bytes, latency_ms=round((t1 - t0) * 1000, 5),
            status="ok" if error is None else f"error:{type(error).__name__}",
            retries=max(0, attempts - originals),
            monotonic_end_s=t1,
        )

    def run_worker(worker_id, start_s, stop_s):
        client = DistributedParameterClient.from_manager(
            manager_address, worker_id=worker_id, value_width=2,
            failover_retries=True, failover_timeout_s=18.0,
            failover_retry_interval_s=0.03,
            failover_attempt_timeout_s=0.25,
        )
        clients.append(client)
        iteration = 0
        try:
            while time.monotonic() < stop_s and not stopped.is_set():
                iteration += 1
                iteration_start_ns = time.time_ns()
                t0 = time.monotonic()
                task = client.pull(list(keys))
                try:
                    reply = task.wait(timeout_ms=25000)
                    if len(reply.values) != 6:
                        raise AssertionError("Incomplete Pull result")
                except Exception as error:
                    record_operation(worker_id, iteration, "pull", task, t0, error)
                    raise
                record_operation(worker_id, iteration, "pull", task, t0)
                # Kill the source at a precise iteration boundary AFTER Pull.
                if worker_id == workers[0] and arm_fault.is_set() and not fault_injected.is_set():
                    mid_iteration.set()
                    if not fault_injected.wait(timeout=8):
                        raise TimeoutError("Fault injection did not release paused worker")
                time.sleep(compute_ms / 1000.0)
                gradients = [fixed_gradient(worker_id, iteration, key, component)
                             for key in keys for component in range(2)]
                t0 = time.monotonic()
                push = client.push(list(keys), gradients, width=2,
                                   deps=[task])
                try:
                    accepted = push.wait(timeout_ms=25000)
                    if not accepted.accepted:
                        raise AssertionError("Push was not accepted")
                except Exception as error:
                    record_operation(worker_id, iteration, "push", push, t0, error)
                    raise
                record_operation(worker_id, iteration, "push", push, t0)
                evidence.add_iteration(worker_id, iteration, iteration_start_ns,
                                       time.time_ns(), keys)
        except Exception as error:
            stopped.set()
            failures.append((worker_id, f"{type(error).__name__}: {error}"))
            # Release fault-injection waiter if this was the designated worker.
            mid_iteration.set()
        finally:
            client.close()

    summary = None
    try:
        control.start()
        manager.start_heartbeat_monitor(interval_s=0.025)
        for address in server_ids:
            proc = subprocess.Popen([
                sys.executable, "-u", str(server_program),
                "--listen", address, "--manager", manager_address,
                "--bootstrap-ownership", "--heartbeat-interval", "0.05",
                "--heartbeat-rpc-timeout", "0.2",
            ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
               stderr=subprocess.STDOUT, text=True)
            processes[address] = proc
            peers[address] = ReplicaPeer(address, timeout_s=2)
        for address in server_ids:
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=8)
        if not wait_until(lambda: all(
                state["state"] == "ALIVE"
                for state in manager.heartbeat_snapshot().values())):
            raise RuntimeError("Not all servers are heartbeating")
        manager.configure_replication(server_ids, k=1)
        manager.seed_replicas(peers)
        manager.activate_live_replication(
            peers, batch_size=batch_size, batch_timeout_s=0.05)

        controller = AutomaticFailoverController(
            manager, peers,
            is_confirmed_dead=lambda sid: sid in processes and processes[sid].poll() is not None,
            scan_interval_s=0.025, retry_interval_s=0.06,
        )
        controller.start()
        started = time.monotonic()
        stop_at = started + pre_fault_s + post_fault_s
        with ThreadPoolExecutor(max_workers=len(workers)) as executor:
            jobs = [executor.submit(run_worker, wid, started, stop_at) for wid in workers]
            while time.monotonic() < started + pre_fault_s and not stopped.is_set():
                time.sleep(0.025)
            arm_fault.set()
            if not mid_iteration.wait(timeout=5):
                raise RuntimeError("Failed to reach worker's mid-iteration fault checkpoint")
            if failures:
                raise AssertionError(f"Training failed before fault injection: {failures}")
            t_fault = time.monotonic()
            evidence.add_event(time.time_ns(), "kill_server",
                               {"server": primary, "phase": "after_pull_before_push",
                                "confirmed_process_kill": True}, "injected")
            processes[primary].kill()
            processes[primary].wait(timeout=5)
            fault_injected.set()
            for future in jobs:
                future.result(timeout=45)
        if failures:
            raise AssertionError(f"Training workers failed: {failures}")
        if not wait_until(lambda: len(controller.snapshot()["recoveries"]) == 1, seconds=10):
            raise RuntimeError(f"Recovery not complete: {controller.snapshot()}")
        recovery = controller.snapshot()["recoveries"][0]
        table = manager.get_map()
        if table.epoch != 2 or table.partitions[0].owner != second:
            raise AssertionError("Primary was not promoted correctly")
        evidence.add_event(time.time_ns(), "recovery_ready",
                           {"new_owner": second, "epoch": table.epoch}, "success")
        # Fetch live values through the same ordinary distributed API.
        observer = DistributedParameterClient.from_manager(
            manager_address, worker_id=999001, value_width=2,
            failover_retries=True, failover_timeout_s=8.0)
        try:
            values = observer.pull(list(keys)).wait(timeout_ms=15000).values
        finally:
            observer.close()
        expected = fixed_gradient_oracle(evidence.completed, fixed_gradient,
                                         width=2, learning_rate=0.1)
        actual = {key: list(values[2 * i:2 * i + 2])
                  for i, key in enumerate(keys)}
        comparison = compare_weights(expected, actual)
        if not comparison["matches"]:
            raise AssertionError(f"X6 offline oracle mismatch: {comparison['mismatches'][:3]}")

        throughput = throughput_recovery(evidence.push_times_s, t_fault,
                                         last_time_s=max(time.monotonic(), stop_at),
                                         sample_step_s=0.1)
        if throughput["theta_pre"] <= 0:
            raise AssertionError("Insufficient nonzero pre-fault throughput")
        first_after_promotion = min((t for t in evidence.push_times_s
                                     if t >= recovery["ready_at_s"]), default=None)
        breakdown = recovery_decomposition(t_fault, recovery, first_after_promotion)
        if first_after_promotion is None:
            raise AssertionError("No successful Push was observed after recovery")
        if throughput["throughput_recovery_s"] is None:
            raise AssertionError("Throughput did not recover to 90% of baseline")

        successful = [row for row in evidence.operations if row["status"] == "ok"]
        latency = {op: [row["latency_ms"] for row in successful if row["op"] == op]
                   for op in ("push", "pull")}
        summary = {
            "experiment": "X6_fixed_gradient_server_kill",
            "seed": seed,
            "fault_phase": "after_pull_before_push",
            "replication_k": 1,
            "batch_size": batch_size,
            "worker_ids": list(workers),
            "duration_pre_fault_s": pre_fault_s,
            "duration_post_fault_s": post_fault_s,
            "completed_pushes": len(evidence.completed),
            "throughput": len(evidence.completed) / (time.monotonic() - started),
            "p50_push_latency_ms": percentile(latency["push"], 0.5),
            "p99_push_latency_ms": percentile(latency["push"], 0.99),
            "p50_pull_latency_ms": percentile(latency["pull"], 0.5),
            "p99_pull_latency_ms": percentile(latency["pull"], 0.99),
            "total_bytes": sum((row["bytes_out"] or 0) + (row["bytes_in"] or 0)
                               for row in successful),
            "bytes_scope": "worker Protobuf payload only (not backup replication/TCP)",
            "final_distance": math.sqrt(sum(
                (actual[key][i] - expected[key][i]) ** 2
                for key in keys for i in range(2))),
            "final_distance_reference": "independent fixed-gradient offline oracle",
            "offline_oracle": comparison,
            "recovery_times": {"throughput": throughput,
                               "decomposition": breakdown},
            "limit": "confirmed crash-stop, one server failure, live manager",
        }
        evidence.write(output_dir, summary)
        return summary
    finally:
        fault_injected.set()
        stopped.set()
        if controller is not None:
            controller.stop()
        for proc in processes.values():
            if proc.poll() is None:
                proc.kill()
        for proc in processes.values():
            try:
                proc.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=3)
        for peer in peers.values():
            peer.close()
        manager.stop_heartbeat_monitor()
        control.stop(0).wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", default=str(Path(__file__).resolve().parents[1] / "Part_F_Tests" / "f51_x6_config.json")
    )
    parser.add_argument("--output", default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--pre-fault-s", type=float, default=None)
    parser.add_argument("--post-fault-s", type=float, default=None)
    parser.add_argument("--compute-ms", type=float, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if config.get("experiment") != "X6_fixed_gradient_server_kill":
        raise ValueError("Expected an X6 fixed-gradient configuration")
    if config.get("cluster") != {"servers": 3, "replication": 1}:
        raise ValueError("This X6 runner requires three servers and k=1")
    if config.get("width") != 2 or config.get("gradient") != "fixed":
        raise ValueError("This X6 runner requires width=2 and fixed gradients")
    if config.get("fault") != {"action": "kill_server",
                               "target": "first-primary",
                               "phase": "after_pull_before_push"}:
        raise ValueError("Unexpected fault scenario in X6 configuration")
    params = {
        "seed": config["seed"],
        "pre_fault_s": config["pre_fault_s"],
        "post_fault_s": config["post_fault_s"],
        "compute_ms": config["compute_ms"],
        "workers": tuple(config["workers"]),
        "batch_size": config["batch_size"],
    }
    for name in ("seed", "pre_fault_s", "post_fault_s", "compute_ms",
                 "batch_size"):
        value = getattr(args, name)
        if value is not None:
            params[name] = value
    output = args.output if args.output is not None else config["output_dir"]
    result = run(output, **params)
    print("X6 fixed-gradient server crash: PASS")
    print("Completed Pushes:", result["completed_pushes"])
    print("Maximum oracle error:", result["offline_oracle"]["max_absolute_error"])
    print("Detection/promotion/resume:", result["recovery_times"]["decomposition"])
    print("Throughput-based recovery:",
          result["recovery_times"]["throughput"]["throughput_recovery_s"])
    print("Results:", output)


if __name__ == "__main__":
    main()
