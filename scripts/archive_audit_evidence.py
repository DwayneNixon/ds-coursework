"""Validate and fingerprint reproducible evidence for the final audit branch.

Run AFTER X1–X4, X6, X7 and the report PDF build. The E4/X5 trial is
historical and stays explicitly labelled as an in-process migration trial.
Output: results/ci_evidence_manifest.json.
"""
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
GROUPS = (
    "x1_aggregation", "x2_scaling", "c5_evaluation",
    "d3_benchmark", "x4_comparison", "audit_x6", "audit_x7",
)


def read_json(relative):
    path = ROOT / relative
    if not path.exists():
        raise FileNotFoundError(relative)
    return json.loads(path.read_text(encoding="utf-8"))


def validate():
    x1 = read_json("results/x1_aggregation/summary.json")
    x2 = read_json("results/x2_scaling/summary.json")
    x4 = read_json("results/x4_comparison/summary.json")
    x6 = read_json("results/audit_x6/summary.json")
    x7 = read_json("results/audit_x7/summary.json")

    if x1.get("oracle_matches") is not True:
        raise AssertionError("X1 independent mathematical oracle did not pass")
    if (x2.get("server_counts") != list(range(1, 9))
            or x2.get("seeds") != [42, 43, 44]
            or len(x2.get("aggregate", [])) != 8):
        raise AssertionError("X2 must have 8 counts x 3 seeds")
    with (ROOT / "results/x2_scaling/trials.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        trials = list(csv.DictReader(handle))
    if len(trials) != 24:
        raise AssertionError("X2 raw trial evidence requires 24 rows")
    with (ROOT / "results/c5_evaluation/summary.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        c5_rows = list(csv.DictReader(handle))
    if len(c5_rows) != 30:
        raise AssertionError("X3/C5 requires 30 configured trials")
    if len(x4.get("rows", [])) != 4:
        raise AssertionError("X4 requires all three optimizations and combined result")
    if not x6.get("offline_oracle", {}).get("matches"):
        raise AssertionError("X6 offline oracle mismatch")
    if x6.get("completed_pushes", 0) <= 0:
        raise AssertionError("X6 has no acknowledged Pushes")
    cases = x7.get("cases", [])
    if (x7.get("status") != "PASS" or len(cases) != 9
            or sum(c["result"] == "EXPECTED_FAIL_CLOSED" for c in cases) != 1):
        raise AssertionError("X7 must show nine cases including a fail-closed row")
    report = ROOT / "submission/parameter_server_report.pdf"
    rendered = ROOT / "submission/parameter_server_report.md"
    if not report.is_file() or not rendered.is_file():
        raise FileNotFoundError("Generated PDF or full Markdown report is missing")
    reader = PdfReader(str(report))
    if not (1 <= len(reader.pages) <= 8):
        raise AssertionError("Report exceeds the eight-page hand-in limit")
    text = " ".join((page.extract_text() or "") for page in reader.pages)
    for required in ("X7: Failure matrix", "Safe refusal (fail-closed)",
                     "Evidence revision", "X2: Does server count"):
        if required not in text:
            raise AssertionError("Report missing " + required)
    if "informal report draft" in text or "{{X2_TABLE}}" in text:
        raise AssertionError("Report contains a draft marker")
    return {
        "x1_max_absolute_error": x1["maximum_absolute_oracle_error"],
        "x2_trials": len(trials),
        "x2_throughput_8_to_1": x2["throughput_ratio_8_to_1"],
        "x3_scenarios": len(c5_rows),
        "x4_optimizations_compared": len(x4["rows"]),
        "x5_scope": "committed E4 in-process elasticity trials",
        "x6_acknowledged_pushes": x6["completed_pushes"],
        "x6_max_oracle_error": x6["offline_oracle"]["max_absolute_error"],
        "x7_matrix_rows": len(cases),
        "x7_fail_closed_rows": 1,
        "report_pdf_pages": len(reader.pages),
    }


def main():
    stats = validate()
    sha = os.environ.get("GITHUB_SHA")
    if not sha:
        try:
            sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            sha = "unknown"
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    entries = {}
    for name in GROUPS:
        directory = ROOT / "results" / name
        if not directory.is_dir():
            raise FileNotFoundError(directory)
        for path in sorted(directory.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            rel = str(path.relative_to(ROOT)).replace(os.sep, "/")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            entries[rel] = {
                "size_bytes": path.stat().st_size,
                "sha256": digest,
            }
    for relative in (
        "submission/parameter_server_report.pdf",
        "submission/parameter_server_report.md",
    ):
        file = ROOT / relative
        entries[relative] = {
            "size_bytes": file.stat().st_size,
            "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
        }
    manifest = {
        "experiment_package": "Distributed Systems coursework A–F X1–X7",
        "source_revision": sha,
        "ci_run_id": run_id,
        "ci_run_url": (
            f"https://github.com/DwayneNixon/ds-coursework/actions/runs/{run_id}"
            if run_id != "local" else None
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope_note": (
            "Tested loopback processes for X2/X6, in-process E4 migration, "
            "one X7 EXPECTED_FAIL_CLOSED case; no arbitrary partition tolerance"
        ),
        "verification": stats,
        "file_digests": entries,
    }
    target = ROOT / "results/ci_evidence_manifest.json"
    target.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Audit evidence manifest: PASS ({len(entries)} files)")
    print(f"Code SHA: {sha}; report pages: {stats['report_pdf_pages']}")


if __name__ == "__main__":
    main()
