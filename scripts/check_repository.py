"""Check audit-branch repository layout and all local Markdown links.

Run from the repository root: python -m scripts.check_repository
This is static repository hygiene, not a substitute for A–F integration tests.
"""
import json
import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]


def main():
    expected = [
        "README.md", "requirements.txt", "requirements-report.txt",
        "parameter_server.proto", "manager.proto", "ds/server.py",
        "ds/server_manager.py", "ds/parameter_client.py",
        "harness/worker.py", "harness/oracle.py", "harness/metrics.py",
        "experiments/__main__.py",
        "experiments/x1_aggregation.py", "experiments/x2_scaling.py",
        "experiments/x3_consistency.py", "experiments/x4_comparison.py",
        "experiments/x5_elasticity.py", "experiments/x6_failover.py",
        "experiments/x7_failures.py",
        "docs/ARCHITECTURE.md", "docs/EXPERIMENTS.md",
        "docs/TESTING.md", "docs/REQUIREMENTS_AUDIT.md",
        "docs/REPORT_DRAFT.md", "docs/DEMO.md",
        "scripts/run_all_regressions.py",
        ".github/workflows/audit-smoke.yml",
        ".github/workflows/audit-full.yml",
        "configs/x1_aggregation.json", "configs/x2_scaling.json",
        "configs/x4_comparison.json",
    ]
    errors = [
        f"Missing required repository path: {path}"
        for path in expected if not (ROOT / path).exists()
    ]
    markdown_files = [ROOT / "README.md"] + list((ROOT / "docs").glob("*.md"))
    markdown_files += list(ROOT.glob("Part_*_README.md"))
    local_link_count = 0
    for source in markdown_files:
        content = source.read_text(encoding="utf-8")
        # Exclude links inside inline backticks and external GitHub URLs.
        for match in re.finditer(r"(?<!!)\[[^\]]+\]\(([^)]+)\)", content):
            link = unquote(match.group(1).strip().split(" ")[0])
            if link.startswith(("https://", "http://", "mailto:", "#")):
                continue
            bare = link.split("#", 1)[0].strip("<>")
            if not bare:
                continue
            local_link_count += 1
            target = (source.parent / bare).resolve()
            if not target.exists():
                errors.append(
                    f"Broken Markdown link in {source.relative_to(ROOT)}: {bare}"
                )
    for path in (*ROOT.glob("configs/*.json"), *ROOT.glob("Part_F_Tests/*config.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as error:
            errors.append(f"Invalid JSON {path.relative_to(ROOT)}: {error}")
    for part in "ABCDEF":
        if not list((ROOT / f"Part_{part}_Tests").glob("test_*.py")):
            errors.append(f"Missing test scripts for part {part}")
    if errors:
        print("\n".join(errors))
        raise SystemExit(1)
    print(
        "Repository layout and docs links: PASS"
        f" ({len(expected)} required paths, {local_link_count} local Markdown links)"
    )


if __name__ == "__main__":
    main()
