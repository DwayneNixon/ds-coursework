
from harness.workload import WorkloadGenerator


def main():
    # Same seed and worker ID must produce
    # identical sparse key sequences.
    a = WorkloadGenerator(1000, 42, 101, keys_per_iter=20)
    b = WorkloadGenerator(1000, 42, 101, keys_per_iter=20)

    for _ in range(10):
        keys_a = a.sample_keys()
        keys_b = b.sample_keys()

        assert keys_a == keys_b
        assert keys_a == sorted(set(keys_a))
        assert all(0 <= k < 1000 for k in keys_a)

    print("Sparse reproducibility: PASS")
    print("Sparse ordering and bounds: PASS")

    # Dense samples must be contiguous.
    dense = WorkloadGenerator(
        1000, 42, 101,
        mode="dense",
        range_size=25,
    )

    for _ in range(10):
        keys = dense.sample_keys()

        assert len(keys) == 25
        assert keys.step == 1
        assert 0 <= keys.start < keys.stop <= 1000

    print("Dense contiguous ranges: PASS")

    # Verify that a million-key space is supported.
    large = WorkloadGenerator(
        1_000_000, 42, 101,
        keys_per_iter=100,
    )

    keys = large.sample_keys()

    assert len(keys) <= 100
    assert all(0 <= k < 1_000_000 for k in keys)

    print("Million-key configuration: PASS")


if __name__ == "__main__":
    main()
