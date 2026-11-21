
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


OUTPUT = Path("results/c5_evaluation")

MODES = [
    "sequential",
    "bounded_1",
    "bounded_2",
    "bounded_4",
    "eventual",
]


def read_csv(filename):
    with (OUTPUT / filename).open(
        newline="", encoding="utf-8"
    ) as file:
        return list(csv.DictReader(file))


def main():
    summaries = read_csv("summary.csv")
    convergence = read_csv("convergence.csv")

    assert len(summaries) == 30

    # Group the convergence measurements by scenario.
    traces = defaultdict(list)

    for row in convergence:
        key = (
            int(row["straggler_ms"]),
            row["mode"],
            int(row["seed"]),
        )
        traces[key].append(row)

    seeds = sorted({
        int(row["seed"]) for row in summaries
    })

    assert seeds == [42, 43, 44]

    for slow_ms in (0, 60):
        print(f"\n=== Straggler: {slow_ms} ms ===")

        print(
            f"{'Mode':12} | {'p50 Push':>9} | "
            f"{'p99 Push':>9} | {'Total bytes':>12}"
        )

        # 1. Latency and communication summary.
        for mode in MODES:
            rows = [
                row for row in summaries
                if int(row["straggler_ms"]) == slow_ms
                and row["mode"] == mode
            ]

            assert len(rows) == 3

            p50 = median(
                float(row["p50_push_ms"])
                for row in rows
            )

            p99 = median(
                float(row["p99_push_ms"])
                for row in rows
            )

            sent_bytes = median(
                int(row["total_bytes"])
                for row in rows
            )

            print(
                f"{mode:12} | "
                f"{p50:9.3f} | "
                f"{p99:9.3f} | "
                f"{sent_bytes:12.0f}"
            )

        # 2. Convergence versus completed Pushes.
        fig, ax = plt.subplots(figsize=(9, 5))

        for mode in MODES:
            trial_curves = []

            for seed in seeds:
                rows = traces[(slow_ms, mode, seed)]
                assert rows

                # A final observation may repeat count=48.
                # Keep the latest sample for each count.
                by_count = {}

                for row in rows:
                    count = int(row["completed_pushes"])
                    by_count[count] = float(
                        row["distance"]
                    )

                trial_curves.append(by_count)

            common_counts = sorted(
                set.intersection(
                    *(set(t) for t in trial_curves)
                )
            )

            assert 0 in common_counts
            assert 48 in common_counts

            median_distances = [
                median(t[count] for t in trial_curves)
                for count in common_counts
            ]

            line, = ax.plot(
                common_counts,
                median_distances,
                linewidth=2,
                label=mode,
            )

            # Show individual trials faintly.
            for trial in trial_curves:
                counts = sorted(trial)

                ax.plot(
                    counts,
                    [trial[n] for n in counts],
                    color=line.get_color(),
                    alpha=0.2,
                    linewidth=1,
                )

        ax.set(
            title=f"Convergence by updates — {slow_ms} ms straggler",
            xlabel="Completed logical Pushes",
            ylabel="Distance to target (L2)",
        )

        ax.grid(alpha=0.2)
        ax.legend()
        fig.tight_layout()

        filename = OUTPUT / (
            f"convergence_by_updates_{slow_ms}ms.png"
        )

        fig.savefig(filename, dpi=180)
        plt.close(fig)

        print("Convergence-by-updates chart: PASS")

        # 3. Convergence versus elapsed wall-clock time.
        fig, ax = plt.subplots(figsize=(9, 5))

        for mode in MODES:
            mode_color = None

            for index, seed in enumerate(seeds):
                rows = sorted(
                    traces[(slow_ms, mode, seed)],
                    key=lambda row: float(
                        row["elapsed_s"]
                    ),
                )

                times = [
                    float(row["elapsed_s"])
                    for row in rows
                ]

                distances = [
                    float(row["distance"])
                    for row in rows
                ]

                options = {
                    "label": mode if index == 0 else "_nolegend_",
                    "linewidth": 1.7,
                    "alpha": 0.7,
                }

                if mode_color is not None:
                    options["color"] = mode_color

                line, = ax.plot(
                    times, distances, **options
                )

                if mode_color is None:
                    mode_color = line.get_color()

        ax.set(
            title=f"Convergence by elapsed time — {slow_ms} ms straggler",
            xlabel="Elapsed time (seconds)",
            ylabel="Distance to target (L2)",
        )

        ax.grid(alpha=0.2)
        ax.legend()
        fig.tight_layout()

        filename = OUTPUT / (
            f"convergence_by_time_{slow_ms}ms.png"
        )

        fig.savefig(filename, dpi=180)
        plt.close(fig)

        print("Convergence-by-time chart: PASS")

    print("\nAll convergence curves generated: PASS")
    print("Latency and byte analysis: PASS")
    print("C5.4 analysis: PASS")


if __name__ == "__main__":
    main()
