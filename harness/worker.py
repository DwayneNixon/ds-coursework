
import random
import time
from harness.oracle import fixed_gradient
from harness.metrics import measure_operation

def sample_keys(rng, num_keys, keys_per_iter):
    """Select sorted, unique parameter keys."""
    return sorted(rng.sample(range(num_keys), keys_per_iter))


def run_worker(
    client,
    worker_id,
    iterations,
    num_keys,
    keys_per_iter,
    width,
    targets,
    seed,
    compute_ms=0,
    workload=None,
    gradient_mode="target_diff",
    on_iteration=None,
    on_operation=None,
    on_iteration_record=None,
):
    """Run a simulated parameter-server worker."""

    if workload is None:
        if not 1 <= keys_per_iter <= num_keys:
            raise ValueError("Invalid keys_per_iter")

    rng = random.Random(f"{seed}:{worker_id}:keys")

    for iteration in range(1, iterations + 1):
        iteration_start_ns = time.time_ns()

        # 1. Select parameter keys
        if workload is None:
            keys = sample_keys(rng, num_keys, keys_per_iter)
            delay_ms = compute_ms
        else:
            keys = list(workload.sample_keys())
            delay_ms = workload.sample_compute_ms()

        # 2. Pull current parameters
        pull_task, result = measure_operation(
            create_task=lambda: client.pull(keys),
            worker_id=worker_id,
            iteration=iteration,
            op="pull",
            n_keys=len(keys),
            record_callback=on_operation,
            target=keys,
        )

        weights = list(result.values)

        if len(weights) != len(keys) * width:
            raise ValueError("Unexpected number of parameter values")

        # 3. Simulate computation
        time.sleep(delay_ms / 1000.0)

        # 4. Calculate target-difference gradients
        gradients = []

        for index, key in enumerate(keys):
            for component in range(width):
                position = index * width + component

                if gradient_mode == "fixed":
                    gradient = fixed_gradient(
                        worker_id,
                        iteration,
                        key,
                        component,
                    )
                else:
                    gradient = (
                        weights[position] - targets[key][component]
                    )

                gradients.append(gradient)

        # 5. Push gradients to the server
        push_task, _ = measure_operation(
            create_task=lambda: client.push(
                keys,
                gradients,
                width=width,
                deps=[pull_task],
            ),
            worker_id=worker_id,
            iteration=iteration,
            op="push",
            n_keys=len(keys),
            record_callback=on_operation,
        )

        push_task.wait(timeout_ms=5000)


        iteration_end_ns = time.time_ns()

        if on_iteration_record is not None:
            on_iteration_record({
                "worker": worker_id,
                "iteration": iteration,
                "t_start": iteration_start_ns,
                "t_end": iteration_end_ns,
                "staleness": None,
            })

        if on_iteration is not None:
            on_iteration(iteration)

        print(
            f"Worker {worker_id} | "
            f"Iteration {iteration}/{iterations} | "
            f"Keys: {keys} | Completed"
        )
