"""F5 / X7: reproducible failure matrix over F3/F4 integration tests.

Run from project root:
    python run_f52_x7.py --config f52_x7_config.json

Requires an already completed X6 run at results/x6_failover. Each non-X6 row
runs a REAL, isolated unittest with actual server/manager gRPC endpoints.
The saved matrix is evidence from executed assertions, not predicted results.
One expected FAIL_CLOSED row explicitly records a safety/availability limit.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from experiments.f5_evidence import ExperimentEvidence
from experiments.f52_x7_matrix import (MATRIX_FIELDS, check_x6, make_row, read_json,
                            validate_config, write_csv)


def run_case(case, root, evidence_dir, x6_summary, timeout_s=90, expected_seed=None):
    """Returns (passed, metrics, log, evidence_source)."""
    if case["kind"] == "x6":
        metrics = check_x6(x6_summary)
        if expected_seed is not None and metrics["seed"] != expected_seed:
            raise ValueError("X6 and X7 must use the same recorded seed")
        return True, metrics, (
            "Verified X6 summary, oracle, events and successful ops.csv count.\n"
            + json.dumps(metrics, indent=2, sort_keys=True)
        ), str(x6_summary)

    metrics_path = evidence_dir / f"{case['id']}.json"
    # Never consume stale metrics from an earlier successful run.
    metrics_path.unlink(missing_ok=True)
    command = [
        sys.executable, "-m", "unittest", "-v",
        "tests.part_f." + case["test"],
    ]
    environment = dict(os.environ)
    environment["DS_X7_CASE_DIR"] = str(evidence_dir.resolve())
    environment["DS_X7_CASE_ID"] = case["id"]
    try:
        done = subprocess.run(
            command, cwd=str(root), env=environment,
            capture_output=True, text=True, timeout=timeout_s,
            encoding="utf-8", errors="replace",
        )
        log = f"COMMAND: {' '.join(command)}\n\nSTDOUT:\n{done.stdout}\n\nSTDERR:\n{done.stderr}"
        # `unittest` exits 0 even if a test was skipped. Require a real PASS.
        passed = (done.returncode == 0
                  and re.search(r"Ran 1 test in", log) is not None
                  and re.search(r"\bOK\b", log) is not None
                  and "skipped=" not in log)
    except subprocess.TimeoutExpired as error:
        return False, {}, f"TIMEOUT after {timeout_s}s: {error}", case["test"]

    metrics = read_json(metrics_path) if metrics_path.is_file() else {}
    if case["test"].startswith("test_f52_x7_faults."):
        # New tests must record their own observed values; no silent PASS.
        passed = passed and bool(metrics)
    return passed, metrics, log, case["test"]


def run(config_path, *, output_dir=None, x6_summary=None, one_case=None,
        timeout_s=90):
    cfg_path = Path(config_path).resolve()
    root = Path(__file__).resolve().parents[1]
    config = read_json(cfg_path)
    cases = validate_config(config)
    if one_case is not None:
        if one_case not in {c["id"] for c in cases}:
            raise ValueError(f"Unknown X7 case: {one_case}")
        selected = [case for case in cases if case["id"] == one_case]
    else:
        selected = cases
    output = Path(output_dir or config["output_dir"])
    if not output.is_absolute():
        output = root / output
    output.mkdir(parents=True, exist_ok=True)
    metrics_dir = output / "case_metrics"
    logs_dir = output / "case_logs"
    metrics_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)
    x6_file = Path(x6_summary or config["x6_summary"])
    if not x6_file.is_absolute():
        x6_file = root / x6_file

    evidence = ExperimentEvidence()
    matrix = []
    raw_results = []
    started = time.monotonic()
    for index, case in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {case['id']} ...", flush=True)
        began_ns = time.time_ns()
        t0 = time.monotonic()
        evidence.add_event(began_ns, "scenario_begin", {
            "case": case["id"], "fault": case["fault"],
            "phase": case["phase"], "kind": case["kind"],
        }, "started")
        try:
            passed, metrics, log, location = run_case(
                case, root, metrics_dir, x6_file, timeout_s=timeout_s,
                expected_seed=config["seed"])
        except Exception as error:
            passed, metrics, log, location = (
                False, {}, f"{type(error).__name__}: {error}",
                str(x6_file) if case["kind"] == "x6" else case["test"],
            )
        duration = time.monotonic() - t0
        (logs_dir / f"{case['id']}.txt").write_text(log + "\n", encoding="utf-8")
        row = make_row(case, passed=passed, evidence=location,
                       seed=config["seed"], metrics=metrics)
        matrix.append(row)
        outcome = row["result"]
        evidence.add_event(time.time_ns(), "scenario_result", {
            "case": case["id"], "expected_classification": case["classification"],
            "elapsed_test_s": round(duration, 5), "metrics": metrics,
        }, outcome)
        raw_results.append({"case_id": case["id"], "result": outcome,
                            "test_duration_s": round(duration, 6),
                            "metrics": metrics,
                            "test_log": str(logs_dir / f"{case['id']}.txt")})
        print(f"    {outcome}  ({duration:.2f}s)", flush=True)
        if not passed:
            print("    See case log for the test traceback", flush=True)

    full = len(selected) == len(cases)
    all_verified = all(row["result"] in {"PASS", "EXPECTED_FAIL_CLOSED"}
                       for row in matrix)
    status = "PASS" if full and all_verified else (
        "INCOMPLETE" if not full and all_verified else "FAIL")
    summary = {
        "experiment": "X7_failure_matrix",
        "seed": config["seed"],
        "status": status,
        "full_matrix": full,
        "verified_rows": sum(row["result"] in {"PASS", "EXPECTED_FAIL_CLOSED"}
                             for row in matrix),
        "total_rows": len(matrix),
        "failed_rows": [row["case_id"] for row in matrix if row["result"] == "FAIL"],
        "expected_fail_closed_rows": [row["case_id"] for row in matrix
                                     if row["result"] == "EXPECTED_FAIL_CLOSED"],
        "run_duration_s": time.monotonic() - started,
        "cases": raw_results,
        "guarantee_scope": "Confirmed single-process crash-stop / test-controlled one-shot RPC faults, live manager",
        "known_limits": [
            "Partial published E1 transfer with crashed source may remain write-fenced",
            "Worker timeout exclusion does not fence old in-flight Push at parameter servers",
            "One-shot RPC shims do not prove availability under arbitrary packet loss or partitions",
            "E1 replicated migration uses a global write fence rather than concurrent writes",
            "Only X6 provides a continuous, five-second-prebaseline throughput experiment here",
        ],
        "x6_evidence": str(x6_file),
        "matrix_file": str(output / "matrix.csv"),
    }
    write_csv(output / "matrix.csv", matrix)
    # Export harness §5.7-compatible events and summary. Unlike continuous
    # X6, these focused failure cases do not generate op/iteration samples.
    evidence.write(output, summary)
    print("\nX7 matrix status:", status)
    print("Verified:", summary["verified_rows"], "/", summary["total_rows"])
    print("Expected fail-closed:", summary["expected_fail_closed_rows"])
    print("Results:", output)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", default=str(Path(__file__).resolve().parents[1] / "configs" / "x7_fault_matrix.json")
    )
    parser.add_argument("--output", default=None)
    parser.add_argument("--x6-summary", default=None)
    parser.add_argument("--case", default=None, help="Run one case; result is INCOMPLETE")
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    result = run(args.config, output_dir=args.output, x6_summary=args.x6_summary,
                 one_case=args.case, timeout_s=args.timeout)
    if result["status"] == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
