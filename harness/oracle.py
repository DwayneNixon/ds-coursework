
import random
from array import array


class TargetVector:
    """Reproducible hidden parameter target."""

    def __init__(
        self,
        num_keys,
        width,
        seed,
        mean=0.0,
        std=1.0,
    ):
        if num_keys < 1 or width < 1:
            raise ValueError("Invalid model dimensions")

        if std < 0:
            raise ValueError("Standard deviation cannot be negative")

        self.num_keys = num_keys
        self.width = width

        rng = random.Random(f"{seed}:target")

        self.values = array(
            "d",
            (
                rng.gauss(mean, std)
                for _ in range(num_keys * width)
            ),
        )

    def __getitem__(self, key):
        """Return the target vector for a parameter key."""

        if not 0 <= key < self.num_keys:
            raise KeyError(key)

        start = key * self.width
        return self.values[start:start + self.width]


def expected_weight(target, learning_rate, iterations):
    """Expected weight after repeated single-worker SGD."""

    factor = 1.0 - (1.0 - learning_rate) ** iterations

    return target * factor


def fixed_gradient(worker_id, iteration, key, component):
    """Generate a deterministic gradient for one vector component."""

    value = (
        worker_id * 3
        + iteration * 5
        + key * 7
        + component * 11
    ) % 17

    return float(value + 1)


def expected_fixed_weights(
    worker_ids,
    iterations,
    num_keys,
    width,
    learning_rate,
):
    """Calculate final weights independently of the server."""

    expected = []

    for key in range(num_keys):
        for component in range(width):

            total_gradient = 0.0

            for worker_id in worker_ids:
                for iteration in range(1, iterations + 1):
                    total_gradient += fixed_gradient(
                        worker_id,
                        iteration,
                        key,
                        component,
                    )

            expected.append(-learning_rate * total_gradient)

    return expected
