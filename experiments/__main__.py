"""Run an experiment from one consistent command: python -m experiments xN."""
import subprocess
import sys
from pathlib import Path

MODULES = {
    "x1": "experiments.x1_aggregation",
    "x2": "experiments.x2_scaling",
    "x3": "experiments.x3_consistency",
    "x4": "experiments.x4_comparison",
    "x5": "experiments.x5_elasticity",
    "x6": "experiments.x6_failover",
    "x7": "experiments.x7_failures",
    "c3": "experiments.c3_clock_growth",
    "d3": "experiments.d3_benchmark",
}
def main():
    if len(sys.argv) < 2 or sys.argv[1] not in MODULES:
        raise SystemExit("Usage: python -m experiments {"
                         + "|".join(MODULES) + "} [experiment options]")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", MODULES[sys.argv[1]], *sys.argv[2:]],
        cwd=str(root), check=False,
    )
    raise SystemExit(result.returncode)

if __name__ == "__main__":
    main()
