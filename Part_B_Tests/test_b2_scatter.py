
from harness.workload import WorkloadGenerator


def main():
    a = WorkloadGenerator(
        1000, 42, 101,
        keys_per_iter=20,
        scatter=True,
    )

    b = WorkloadGenerator(
        1000, 42, 101,
        keys_per_iter=20,
        scatter=True,
    )

    c = WorkloadGenerator(
        1000, 42, 202,
        keys_per_iter=20,
        scatter=True,
    )

    # Same seed and worker must reproduce the keys.
    for _ in range(10):
        assert a.sample_keys() == b.sample_keys()

    print("Scattered reproducibility: PASS")

    # All workers should share the same key mapping.
    assert list(a.permutation) == list(c.permutation)

    print("Shared seeded permutation: PASS")

    # Every key must appear exactly once in the mapping.
    assert sorted(a.permutation) == list(range(1000))

    print("Permutation validity: PASS")


if __name__ == "__main__":
    main()
