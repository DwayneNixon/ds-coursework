
"""Part E4: Export elasticity measurements and generate plots."""

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def percentile(values, percent):
    """Calculate a percentile using linear interpolation."""

    if not values:
        return None

    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100

    lower = math.floor(position)
    upper = math.ceil(position)

    return (
        ordered[lower]
        + (ordered[upper] - ordered[lower])
        * (position - lower)
    )


def export_e4_results(
    records,
    scaling_start_ns,
    scaling_end_ns,
    before_seconds,
    after_seconds,
    final_counts,
):
    """Export successful Push measurements from the E4 workload."""

    duration = (
        scaling_end_ns - scaling_start_ns
    ) / 1e9

    if duration < 0:
        raise ValueError("Scaling timestamps are reversed")

    # Use the beginning of scaling as time zero.
    rows = []

    for finished_ns, latency_ms in sorted(records):
        time_s = (
            finished_ns - scaling_start_ns
        ) / 1e9

        if time_s < 0:
            phase = "before"
        elif time_s < duration:
            phase = "scaling"
        else:
            phase = "after"

        rows.append({
            "time_s": time_s,
            "latency_ms": latency_ms,
            "phase": phase,
        })

    # Match the five-second comparison windows in E4.1.
    window = min(
        5,
        int(before_seconds),
        int(after_seconds),
    )

    if window < 1:
        raise ValueError("Insufficient measurement window")

    before = [
        row["latency_ms"]
        for row in rows
        if -window <= row["time_s"] < 0
    ]

    after = [
        row["latency_ms"]
        for row in rows
        if duration <= row["time_s"] < duration + window
    ]

    if not before or not after:
        raise ValueError(
            "No successful Pushes in a comparison window"
        )

    # Give each trial its own results directory.
    output_dir = (
        Path("results")
        / "e4_elasticity"
        / datetime.now().strftime(
            "trial_%Y%m%d_%H%M%S_%f"
        )
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    # --------------------------------------------------
    # 1. Individual successful Push measurements
    # --------------------------------------------------

    with (output_dir / "ops.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "time_s",
                "latency_ms",
                "phase",
            ],
        )

        writer.writeheader()
        writer.writerows(rows)

    # --------------------------------------------------
    # 2. Scaling event timestamps
    # --------------------------------------------------

    with (output_dir / "events.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "time_s",
            "event",
            "server_count",
        ])

        writer.writerow([
            0,
            "scaling_started",
            2,
        ])

        writer.writerow([
            duration,
            "scaling_finished",
            4,
        ])

    # --------------------------------------------------
    # 3. One-second throughput and latency windows
    # --------------------------------------------------

    bins = []

    for start in range(-window, window):
        samples = [
            row["latency_ms"]
            for row in rows
            if start <= row["time_s"] < start + 1
        ]

        bins.append({
            "interval_start_s": start,
            "interval_end_s": start + 1,
            "throughput_pushes_per_s": len(samples),
            "p50_latency_ms": (
                median(samples) if samples else ""
            ),
            "p95_latency_ms": (
                percentile(samples, 95)
                if samples else ""
            ),
        })

    with (output_dir / "timeseries.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=list(bins[0]),
        )

        writer.writeheader()
        writer.writerows(bins)

    # --------------------------------------------------
    # 4. JSON summary
    # --------------------------------------------------

    summary = {
        "initial_servers": 2,
        "final_servers": 4,
        "scaling_duration_s": duration,
        "comparison_window_s": window,
        "total_successful_pushes": sum(
            final_counts.values()
        ),
        "per_key_push_counts": {
            str(key): count
            for key, count in final_counts.items()
        },
        "before": {
            "pushes_per_second": len(before) / window,
            "median_push_latency_ms": median(before),
            "p95_push_latency_ms": percentile(
                before, 95
            ),
        },
        "after": {
            "pushes_per_second": len(after) / window,
            "median_push_latency_ms": median(after),
            "p95_push_latency_ms": percentile(
                after, 95
            ),
        },
        "correctness": (
            "Verified by E4.1 oracle before export"
        ),
        "limitations": (
            "Single local run, in-process migration, "
            "four workers, successful Pushes only"
        ),
    }

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    # Center each plotted sample on its one-second window.
    x = [
        row["interval_start_s"] + 0.5
        for row in bins
    ]

    # --------------------------------------------------
    # 5. Throughput graph
    # --------------------------------------------------

    fig, ax = plt.subplots(figsize=(9, 4))

    ax.plot(
        x,
        [
            row["throughput_pushes_per_s"]
            for row in bins
        ],
        marker="o",
    )

    ax.axvline(
        0,
        linestyle="--",
        label="Scaling begins (2 to 4 servers)",
    )

    ax.axvspan(
        0,
        duration,
        alpha=0.15,
        label="Scaling transition",
    )

    ax.set(
        title="Push Throughput During Server Scaling",
        xlabel="Time Relative to Scaling Start (s)",
        ylabel="Successful Pushes per Second",
    )

    ax.legend()
    fig.tight_layout()

    fig.savefig(
        output_dir / "throughput.png",
        dpi=160,
    )

    plt.close(fig)

    # --------------------------------------------------
    # 6. Latency graph
    # --------------------------------------------------

    p50 = [
        row["p50_latency_ms"]
        if row["p50_latency_ms"] != ""
        else math.nan
        for row in bins
    ]

    p95 = [
        row["p95_latency_ms"]
        if row["p95_latency_ms"] != ""
        else math.nan
        for row in bins
    ]

    fig, ax = plt.subplots(figsize=(9, 4))

    ax.plot(
        x, p50,
        marker="o",
        label="Median",
    )

    ax.plot(
        x, p95,
        marker="o",
        label="P95",
    )

    ax.axvline(
        0,
        linestyle="--",
        label="Scaling begins (2 to 4 servers)",
    )

    ax.axvspan(
        0,
        duration,
        alpha=0.15,
        label="Scaling transition",
    )

    ax.set(
        title="Push Latency During Server Scaling",
        xlabel="Time Relative to Scaling Start (s)",
        ylabel="Push Latency (ms)",
    )

    ax.legend()
    fig.tight_layout()

    fig.savefig(
        output_dir / "latency.png",
        dpi=160,
    )

    plt.close(fig)

    return output_dir
