
import math

from harness.workload import WorkloadGenerator


def main():
    # Test 1: Reproducibility
    a = WorkloadGenerator(
        1000, 42, 101,
        compute_median_ms=20,
        compute_sigma=0.4,
    )

    b = WorkloadGenerator(
        1000, 42, 101,
        compute_median_ms=20,
        compute_sigma=0.4,
    )

    times_a = [a.sample_compute_ms() for _ in range(20)]
    times_b = [b.sample_compute_ms() for _ in range(20)]

    assert times_a == times_b
    assert all(t > 0 for t in times_a)
    assert len(set(times_a)) > 1

    print("Compute reproducibility: PASS")
    print("Positive heterogeneous delays: PASS")

    # Test 2: Straggler slowdown
    normal = WorkloadGenerator(
        1000, 42, 101,
        is_straggler=False,
    )

    slow = WorkloadGenerator(
        1000, 42, 101,
        is_straggler=True,
        straggler_slowdown=5,
    )

    for _ in range(20):
        normal_time = normal.sample_compute_ms()
        slow_time = slow.sample_compute_ms()

        assert math.isclose(
            slow_time,
            normal_time * 5,
            rel_tol=1e-12,
        )

    print("Straggler slowdown: PASS")

    # Test 3: Computation sampling must not
    # affect the generated parameter keys
    worker_a = WorkloadGenerator(
        1000, 42, 101,
        keys_per_iter=20,
    )

    worker_b = WorkloadGenerator(
        1000, 42, 101,
        keys_per_iter=20,
    )

    worker_a.sample_compute_ms()
    worker_a.sample_compute_ms()

    assert worker_a.sample_keys() == worker_b.sample_keys()

    print("Independent random streams: PASS")

    # Test 4: Zero computation delay
    zero = WorkloadGenerator(
        1000, 42, 101,
        compute_median_ms=0,
    )

    assert zero.sample_compute_ms() == 0.0

    print("Zero computation delay: PASS")


if __name__ == "__main__":
    main()
