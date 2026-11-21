import json
import queue
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from parameter_client import ParameterClient
from harness.cluster import LocalCluster
from harness.faults import FaultInjectingClient, LinkFaultController
from harness.metrics import (
    make_summary,
    write_convergence_csv,
    write_events_csv,
    write_iterations_csv,
    write_ops_csv,
    write_summary_json,
)
from harness.observer import ConvergenceObserver
from harness.oracle import TargetVector
from harness.scenarios import ScenarioEngine
from harness.worker import run_worker

CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "b5_integrated.json"
)

with CONFIG_PATH.open("r", encoding="utf-8") as file:
    CONFIG = json.load(file)

SERVER_ADDRESS = CONFIG["server_address"]
WORKER_IDS = CONFIG["workers"]["ids"]
ITERATIONS = CONFIG["workers"]["iterations"]
NUM_KEYS = CONFIG["model"]["num_keys"]
WIDTH = CONFIG["model"]["value_width"]
SEED = CONFIG["seed"]
COMPUTE_MS = CONFIG["workers"]["compute_ms"]



def worker_process(worker_id, state_file):
    """Run a real parameter-server worker in a child process."""

    base_client = ParameterClient(
        SERVER_ADDRESS,
        worker_id=worker_id,
    )

    client = FaultInjectingClient(
        client=base_client,
        worker_id=worker_id,
        server_id="server1",
        state_file=state_file,
        seed=SEED,
    )

    try:
        targets = TargetVector(NUM_KEYS, WIDTH, SEED)

        def report_progress(iteration):
            print(
                f"PROGRESS {worker_id} {iteration}",
                flush=True,
            )

        def report_operation(record):
            print(
                "METRIC_OP " + json.dumps(record),
                flush=True,
            )


        def report_iteration(record):
            print(
                "METRIC_ITER " + json.dumps(record),
                flush=True,
            )

        run_worker(
            client=client,
            worker_id=worker_id,
            iterations=ITERATIONS,
            num_keys=NUM_KEYS,
            keys_per_iter=NUM_KEYS,
            width=WIDTH,
            targets=targets,
            seed=SEED,
            compute_ms=COMPUTE_MS,
            on_iteration=report_progress,
            on_operation=report_operation,
            on_iteration_record=report_iteration,
        )

    finally:
        client.close()


def read_output(worker_id, output_stream, messages):
    """Forward worker output into a thread-safe queue."""

    try:
        for line in output_stream:
            messages.put((worker_id, line.strip()))
    finally:
        output_stream.close()


def main():
    """Launch workers and execute the configured scenario."""

    cluster = LocalCluster()

    # Shared fault configuration for worker processes
    state_dir = tempfile.TemporaryDirectory()
    state_file = Path(state_dir.name) / "links.json"

    link_controller = LinkFaultController(state_file)
    fault_messages = []
    operation_records = []
    iteration_records = []

    messages = queue.Queue()
    progress = {}

    worker_nodes = {}
    readers = []


    # Dedicated read-only client for convergence measurements
    observer_client = ParameterClient(
        SERVER_ADDRESS,
        worker_id=999,
    )

    targets = TargetVector(NUM_KEYS, WIDTH, SEED)

    observer = ConvergenceObserver(
        client=observer_client,
        targets=targets,
        num_keys=NUM_KEYS,
        width=WIDTH,
        batch_size=NUM_KEYS,
    )

    observer_stop = threading.Event()
    observer_lock = threading.Lock()
    observer_progress = {"iteration": 0}

    def get_observer_iteration():
        with observer_lock:
            return observer_progress["iteration"]

    observer_executor = ThreadPoolExecutor(max_workers=1)
    observer_future = None

    try:

        # --------------------------------------------------
        # 1. Start and register workers
        # --------------------------------------------------

        def start_worker(worker_id):
            """Launch and track a worker subprocess."""

            if worker_id in worker_nodes:
                raise ValueError("Worker already exists")

            command = [
                sys.executable,
                "-u",
                "-m",
                "experiments.b4_scenarios",
                "--worker",
                str(worker_id),
                str(state_file),
            ]

            node_id = cluster.start_worker(
                command,
                capture_output=True,
            )

            worker_nodes[worker_id] = node_id
            progress[worker_id] = 0

            process = cluster.nodes[node_id]["process"]

            reader = threading.Thread(
                target=read_output,
                args=(worker_id, process.stdout, messages),
                daemon=True,
            )

            reader.start()
            readers.append(reader)

        # Sample before launching workers, so iteration 0 really is the
        # initial server state (start the server fresh before this run).
        observer.sample(iteration=0)

        # Monitor concurrently while worker subprocesses are running.
        observer_future = observer_executor.submit(
            observer.monitor,
            observer_stop,
            get_observer_iteration,
            CONFIG["output"]["observer_interval_s"]
        )

        for worker_id in WORKER_IDS:
            start_worker(worker_id)

        print("Started workers:", WORKER_IDS)

        # --------------------------------------------------
        # 2. Scenario event execution
        # --------------------------------------------------

        next_worker_id = max(WORKER_IDS) + 1

        def execute_event(action, args):
            """Execute scenario actions."""

            nonlocal next_worker_id

            # Apply communication faults
            if action in ("delay_link", "drop_link"):
                return link_controller.execute(action, args)

            # Add new workers
            if action == "add_worker":
                count = args.get("count", 1)

                if type(count) is not int or count < 1:
                    raise ValueError("Invalid worker count")

                added = []

                for _ in range(count):
                    worker_id = next_worker_id

                    start_worker(worker_id)

                    added.append(worker_id)
                    next_worker_id += 1

                print("Added workers:", added)

                return f"ADDED workers {added}"

            # Other supported cluster actions
            return cluster.execute(action, args)

        # --------------------------------------------------
        # 3. Define the scenario
        # --------------------------------------------------
        
        events = []

        for configured_event in CONFIG["scenario"]:
            event = configured_event.copy()

            # Convert logical worker ID to subprocess node ID.
            if event["action"] == "kill_worker":
                worker_id = event["target"]
                event["target"] = worker_nodes[worker_id]

            events.append(event)

        engine = ScenarioEngine(events, execute_event)

        # --------------------------------------------------
        # 4. Collect worker progress and fault messages
        # --------------------------------------------------

        
        def collect_messages():
            """Collect progress, metrics, and fault messages."""

            while True:
                try:
                    worker_id, message = messages.get_nowait()

                except queue.Empty:
                    break

                # 1. Operation metrics
                if message.startswith("METRIC_OP "):
                    record = json.loads(
                        message[len("METRIC_OP "):]
                    )

                    assert record["worker"] == worker_id
                    operation_records.append(record)

                # 2. Iteration metrics
                elif message.startswith("METRIC_ITER "):
                    record = json.loads(
                        message[len("METRIC_ITER "):]
                    )

                    assert record["worker"] == worker_id
                    iteration_records.append(record)

                # 3. Worker progress
                elif message.startswith("PROGRESS "):
                    _, reported_id, iteration = message.split()

                    assert int(reported_id) == worker_id
                    progress[worker_id] = int(iteration)

                # 4. Fault messages and normal output
                else:
                    if message.startswith("FAULT_DELAY "):
                        fault_messages.append(
                            (worker_id, message)
                        )

                    print(message)

        # --------------------------------------------------
        # 5. Run the scenario
        # --------------------------------------------------

        start_time = time.monotonic()

        while True:
            collect_messages()

            # Only active workers participate in the
            # slowest-worker iteration calculation.
            active = {
                worker_id: progress[worker_id]
                for worker_id, node_id in worker_nodes.items()
                if cluster.is_running(node_id)
            }

            # The observer's label is approximate under concurrency:
            # an update may be visible before its progress message arrives.
            if active:
                with observer_lock:
                    observer_progress["iteration"] = min(active.values())

            elapsed = time.monotonic() - start_time

            # Execute any events whose triggers are due
            engine.tick(elapsed, active)

            # Stop when all worker processes have exited
            if not any(
                cluster.is_running(node_id)
                for node_id in worker_nodes.values()
            ):
                break

            if elapsed > 15:
                raise TimeoutError(
                    "Experiment exceeded 15 seconds"
                )

            time.sleep(0.02)

        # Collect output remaining after workers exit
        for reader in readers:
            reader.join(timeout=1)

        collect_messages()

        # Stop periodic sampling, then take a final sample once no
        # workers are updating the server.
        observer_stop.set()
        if observer_future is not None:
            observer_future.result(timeout=15)
        observer.sample(iteration=ITERATIONS)
        print("Convergence monitoring: PASS")

        # --------------------------------------------------
        # 6. Verify worker completion
        # --------------------------------------------------

        # Worker 701 and newly added worker 703
        # must complete all iterations.
        for worker_id in [701, 703]:
            process = cluster.nodes[
                worker_nodes[worker_id]
            ]["process"]

            assert process.returncode == 0
            assert progress[worker_id] == ITERATIONS

        print("\nOriginal and added workers completed: PASS")

        # Worker 702 must have been terminated
        killed = cluster.nodes[
            worker_nodes[702]
        ]["process"]

        assert killed.returncode is not None
        assert killed.returncode != 0
        assert progress[702] >= 2

        print("Worker termination: PASS")

        # --------------------------------------------------
        # 7. Verify scenario events
        # --------------------------------------------------

        assert len(engine.records) == 3

        outcomes = {
            record["action"]: record["outcome"]
            for record in engine.records
        }

        assert outcomes["delay_link"].startswith("APPLIED")
        assert outcomes["add_worker"].startswith("ADDED")
        assert outcomes["kill_worker"].startswith("KILLED")

        print("Scenario event execution: PASS")

        # --------------------------------------------------
        # 8. Verify live delay injection
        # --------------------------------------------------

        assert any(
            worker_id == 701 and "ms=80.0" in message
            for worker_id, message in fault_messages
        )

        print("Live link delay injection: PASS")

        # --------------------------------------------------
        # 9. Display scenario records
        # --------------------------------------------------

        print("\nScenario events:")

        for record in engine.records:
            print(record)

        print("\nB4 final integration: PASS")


        # --------------------------------------------------
        # Export real worker metrics
        # --------------------------------------------------

        output_dir = Path(CONFIG["output"]["dir"])

        ops_path = write_ops_csv(
            operation_records,
            output_dir,
        )

        iterations_path = write_iterations_csv(
            iteration_records,
            output_dir,
        )

        convergence_path = write_convergence_csv(
            observer.records,
            output_dir,
        )

        events_path = write_events_csv(
            engine.records,
            output_dir,
        )

        summary = make_summary(operation_records, observer.records)
        summary_path = write_summary_json(summary, output_dir)

        assert len(observer.records) >= 2
        assert len(engine.records) == 3
        assert summary["final_distance"] is not None
        print("Convergence/event exports: PASS")
        print("Summary JSON export: PASS")

        # Verify records from the original and joined workers.
        for worker_id in [701, 703]:
            worker_iterations = [
                record
                for record in iteration_records
                if record["worker"] == worker_id
            ]

            assert len(worker_iterations) == ITERATIONS

        print("Completed-worker metrics: PASS")

        # Verify that records from the killed worker survived.
        killed_worker_records = [
            record
            for record in iteration_records
            if record["worker"] == 702
        ]

        assert len(killed_worker_records) >= 2

        print("Killed-worker metrics preserved: PASS")

        # Every completed iteration should have a
        # successful pull and push operation recorded.
        for iteration_record in iteration_records:
            matching_ops = [
                record["op"]
                for record in operation_records
                if record["worker"] == iteration_record["worker"]
                and record["iteration"] == iteration_record["iteration"]
                and record["status"] == "OK"
            ]

            assert "pull" in matching_ops
            assert "push" in matching_ops

        print("Operation/iteration consistency: PASS")

        print("\nCollected records:")
        print("Operations:", len(operation_records))
        print("Iterations:", len(iteration_records))

        print("\nGenerated files:")
        print(ops_path)
        print(iterations_path)
        print(convergence_path)
        print(events_path)
        print(summary_path)

        print("\nB5 live metrics integration: PASS")
        print("Five-file integration: PASS")

    finally:
        # Stop the observer before closing its gRPC client. Always stop
        # launched child processes, including when a test raises.
        observer_stop.set()
        try:
            cluster.close()
        finally:
            try:
                observer_executor.shutdown(wait=True)
            finally:
                observer_client.close()
                state_dir.cleanup()


if __name__ == "__main__":

    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        worker_process(
            int(sys.argv[2]),
            sys.argv[3],
        )
    else:
        main()
