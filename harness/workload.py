
import bisect
import random
from array import array
import math

class WorkloadGenerator:
    def __init__(
        self,
        num_keys,
        seed,
        worker_id,
        mode="sparse",
        keys_per_iter=10,
        range_size=10,
        zipf_s=1.1,
        scatter=False,
        compute_median_ms=20.0,
        compute_sigma=0.4,
        is_straggler=False,
        straggler_slowdown=5.0,
    ):
        if num_keys < 1:
            raise ValueError("num_keys must be positive")

        if mode not in ("sparse", "dense"):
            raise ValueError("mode must be sparse or dense")

        self.num_keys = num_keys
        self.mode = mode
        self.keys_per_iter = keys_per_iter
        self.range_size = range_size

        self.rng = random.Random(
            f"{seed}:{worker_id}:keys"
        )


        # Validate computation-time settings
        if compute_median_ms < 0:
            raise ValueError("Compute median cannot be negative")

        if compute_sigma < 0:
            raise ValueError("Compute sigma cannot be negative")

        if straggler_slowdown < 1:
            raise ValueError("Straggler slowdown must be at least 1")

        # Store computation settings
        self.compute_median_ms = compute_median_ms
        self.compute_sigma = compute_sigma
        self.is_straggler = is_straggler
        self.straggler_slowdown = straggler_slowdown

        # Separate random stream for computation times
        self.compute_rng = random.Random(
            f"{seed}:{worker_id}:compute"
        )


        if mode == "sparse":
            if keys_per_iter < 1 or zipf_s <= 0:
                raise ValueError("Invalid sparse configuration")

            # Build the cumulative Zipf distribution once.
            self.cdf = []
            total = 0.0

            for rank in range(1, num_keys + 1):
                total += 1.0 / (rank ** zipf_s)
                self.cdf.append(total)

            self.total = total

            self.permutation = None

            if scatter:
                scatter_rng = random.Random(f"{seed}:scatter")

                self.permutation = array("Q", range(num_keys))
                scatter_rng.shuffle(self.permutation)

        else:
            if not 1 <= range_size <= num_keys:
                raise ValueError("Invalid dense range size")

    def sample_keys(self):
        if self.mode == "dense":
            start = self.rng.randrange(
                self.num_keys - self.range_size + 1
            )
            return range(start, start + self.range_size)

        # Zipf sampling with replacement.
        keys = set()

        for _ in range(self.keys_per_iter):
            probability = self.rng.random() * self.total

            rank = bisect.bisect_left(
                self.cdf, probability
            )

            key = rank

            if self.permutation is not None:
                key = self.permutation[rank]

            keys.add(key)

        return sorted(keys)

    
    def sample_compute_ms(self):
        """Generate a reproducible computation delay in milliseconds."""

        # Allow zero computation time for simple tests
        if self.compute_median_ms == 0:
            return 0.0

        # Generate a lognormal computation time
        compute_time = self.compute_rng.lognormvariate(
            math.log(self.compute_median_ms),
            self.compute_sigma,
        )

        # Apply slowdown to designated stragglers
        if self.is_straggler:
            compute_time *= self.straggler_slowdown

        return compute_time

