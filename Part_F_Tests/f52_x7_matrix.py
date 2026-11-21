"""X7 failure-matrix evidence validation and tabulation (no server changes).

The matrix runner uses *real* focused tests from F3.4 and F4.2 plus new
fault-injection tests. It never presents an expected fail-closed outcome as
transparent recovery, nor a test name as empirical proof without execution.
"""

import csv
import hashlib
import json
import math
from pathlib import Path
import re

MATRIX_FIELDS = (
    "case_id", "category", "fault", "phase", "classification", "result",
    "observed_outcome", "tested_guarantee", "known_limit", "recovery_s",
    "evidence", "seed",
)
REQUIRED_CATEGORIES = frozenset({
    "server_mid_iteration", "server_during_transfer", "worker_bounded",
    "delayed_or_dropped_messages",
})
REQUIRED_CASE_IDS = frozenset({
    "x6_primary_mid_iteration", "migration_source_during_staging",
    "migration_partial_postpublication", "worker_bounded_crash",
    "delayed_push_request", "dropped_push_request", "dropped_push_ack",
    "dropped_transfer_stage",
})
VALID_CLASSIFICATIONS = {"RECOVERED", "FAIL_CLOSED"}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_x6(summary_path):
    """Validate primary-crash evidence instead of trusting a PASS string."""
    path = Path(summary_path)
    if not path.is_file():
        raise FileNotFoundError(
            f"Missing X6 results at {path}. Run: python run_f51_x6.py --config f51_x6_config.json"
        )
    summary = read_json(path)
    if summary.get("experiment") != "X6_fixed_gradient_server_kill":
        raise ValueError("X6 summary identifies a different experiment")
    if summary.get("fault_phase") != "after_pull_before_push":
        raise ValueError("X6 summary did not specify a mid-iteration crash")
    if not isinstance(summary.get("completed_pushes"), int) or summary["completed_pushes"] < 1:
        raise ValueError("X6 has no acknowledged logical Pushes")
    oracle = summary.get("offline_oracle", {})
    if oracle.get("matches") is not True or oracle.get("mismatches"):
        raise ValueError("X6 final parameter values did not match offline oracle")
    error = oracle.get("max_absolute_error")
    if type(error) not in (int, float) or not math.isfinite(error) or error > 1e-8:
        raise ValueError("X6 oracle error invalid or too large")
    times = summary.get("recovery_times", {})
    phases = times.get("decomposition") or {}
    for name in ("T_detect_s", "T_promote_s", "T_resume_to_first_push_s",
                 "T_rec_first_push_s"):
        val = phases.get(name)
        if type(val) not in (int, float) or not math.isfinite(val) or val < 0:
            raise ValueError(f"X6 missing valid measured {name}")
    if not math.isclose(
        phases["T_rec_first_push_s"],
        phases["T_detect_s"] + phases["T_promote_s"] + phases["T_resume_to_first_push_s"],
        rel_tol=1e-6, abs_tol=0.005,
    ):
        raise ValueError("X6 recovery decomposition does not add up")
    rates = times.get("throughput") or {}
    for name in ("theta_pre", "threshold_90pct", "postfault_only_recovery_s"):
        v = rates.get(name)
        if type(v) not in (int, float) or not math.isfinite(v) or v <= 0:
            raise ValueError(f"X6 lacks meaningful throughput measure {name}")
    if not math.isclose(rates["threshold_90pct"], .9 * rates["theta_pre"], rel_tol=1e-8):
        raise ValueError("X6 recovery threshold is not 90% of the baseline")
    events_path = path.with_name("events.csv")
    if not events_path.is_file():
        raise FileNotFoundError("X6 events.csv is required for crash provenance")
    with events_path.open(newline="", encoding="utf-8") as handle:
        events = list(csv.DictReader(handle))
    injections = []
    for event in events:
        if event.get("action") != "kill_server":
            continue
        try:
            args = json.loads(event.get("args", "{}"))
        except json.JSONDecodeError:
            continue
        if (args.get("phase") == "after_pull_before_push"
                and args.get("confirmed_process_kill") is True):
            injections.append(event)
    if len(injections) != 1:
        raise ValueError("X6 must include exactly one verified mid-iteration server kill event")
    for required in ("ops.csv", "iterations.csv", "convergence.csv"):
        if not path.with_name(required).is_file():
            raise FileNotFoundError(f"X6 evidence missing {required}")
    with path.with_name("ops.csv").open(newline="", encoding="utf-8") as handle:
        pushes = sum(1 for row in csv.DictReader(handle)
                     if row.get("op") == "push" and row.get("status") == "ok")
    if pushes != summary["completed_pushes"]:
        raise ValueError("X6 completed Push count does not match ops.csv")
    return {
        "x6_summary_sha256": file_hash(path),
        "x6_events_sha256": file_hash(events_path),
        "completed_pushes": pushes,
        "oracle_max_error": error,
        "recovery_s": phases["T_rec_first_push_s"],
        "T_detect_s": phases["T_detect_s"],
        "T_promote_s": phases["T_promote_s"],
        "T_resume_s": phases["T_resume_to_first_push_s"],
        "theta_pre": rates["theta_pre"],
        "threshold_90pct": rates["threshold_90pct"],
        "sliding_recovery_s": rates.get("throughput_recovery_s"),
        "postfault_only_recovery_s": rates["postfault_only_recovery_s"],
        "seed": summary.get("seed"),
        "file": str(path),
    }


def validate_config(config):
    if config.get("experiment") != "X7_failure_matrix":
        raise ValueError("Expected X7_failure_matrix configuration")
    seed = config.get("seed")
    if type(seed) is not int or seed < 0:
        raise ValueError("Seed must be a nonnegative integer")
    cases = config.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("X7 cases must be a nonempty list")
    ids = set()
    categories = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("Malformed X7 case")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9_]+", case_id):
            raise ValueError("Invalid case ID")
        if case_id in ids:
            raise ValueError(f"Duplicate X7 case ID: {case_id}")
        ids.add(case_id)
        categories.add(case.get("category"))
        if case.get("classification") not in VALID_CLASSIFICATIONS:
            raise ValueError(f"Invalid classification for {case_id}")
        for key in ("fault", "phase", "observed_on_pass", "guarantee", "limit"):
            if not isinstance(case.get(key), str) or not case[key]:
                raise ValueError(f"Missing {key} for {case_id}")
        if case.get("kind") == "x6":
            if case_id != "x6_primary_mid_iteration":
                raise ValueError("X6 evidence can only be used for the X6 crash case")
        elif case.get("kind") == "unittest":
            spec = case.get("test")
            if not isinstance(spec, str) or not re.fullmatch(
                r"test_[a-z0-9_]+\.[A-Za-z][A-Za-z0-9_]*\.test_[a-z0-9_]+", spec,
            ):
                raise ValueError(f"Invalid unittest target for {case_id}")
        else:
            raise ValueError(f"Invalid kind for {case_id}")
    if not REQUIRED_CATEGORIES.issubset(categories):
        raise ValueError(f"Missing required F5 categories: {REQUIRED_CATEGORIES - categories}")
    if not REQUIRED_CASE_IDS.issubset(ids):
        raise ValueError(f"Missing required X7 cases: {REQUIRED_CASE_IDS - ids}")
    if next(c for c in cases if c["id"] == "migration_partial_postpublication")[
        "classification"] != "FAIL_CLOSED":
        raise ValueError("Partial postpublication crash must be labelled FAIL_CLOSED")
    return cases


def make_row(case, *, passed, evidence, seed, metrics=None):
    """Only successful real tests give evidence of a guarantee."""
    if passed:
        classification = case["classification"]
        result = "PASS" if classification == "RECOVERED" else "EXPECTED_FAIL_CLOSED"
        observation = case["observed_on_pass"]
        guarantee = case["guarantee"]
    else:
        classification = "UNVERIFIED"
        result = "FAIL"
        observation = "Test failed; no guarantee established"
        guarantee = "UNVERIFIED"
    observed_time = (metrics or {}).get("crash_exclusion_latency_s")
    if observed_time is None:
        observed_time = (metrics or {}).get("recovery_s")
    return dict(zip(MATRIX_FIELDS, (
        case["id"], case["category"], case["fault"], case["phase"],
        classification, result, observation, guarantee, case["limit"],
        observed_time if observed_time is not None else "", evidence, seed,
    )))


def write_csv(path, rows):
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MATRIX_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
