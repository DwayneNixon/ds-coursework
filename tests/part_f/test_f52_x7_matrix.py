"""Pure X7 evidence validation tests; no Protobuf or running server needed."""

import csv
import json
from pathlib import Path
import tempfile
import unittest

from experiments.f52_x7_matrix import (check_x6, make_row, read_json, validate_config,
                            write_csv)


class MatrixEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.config = read_json(Path(__file__).resolve().parents[2] / "configs" / "x7_fault_matrix.json")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dir = Path(self.temp.name)
        self.summary = {
            "experiment": "X6_fixed_gradient_server_kill",
            "fault_phase": "after_pull_before_push",
            "completed_pushes": 2,
            "seed": 42,
            "offline_oracle": {"matches": True, "max_absolute_error": 1e-13,
                               "mismatches": []},
            "recovery_times": {
                "decomposition": {
                    "T_detect_s": .5, "T_promote_s": .1,
                    "T_resume_to_first_push_s": 1.2,
                    "T_rec_first_push_s": 1.8,
                },
                "throughput": {"theta_pre": 70, "threshold_90pct": 63,
                               "postfault_only_recovery_s": 3.0,
                               "throughput_recovery_s": .1},
            },
        }
        (self.dir / "ops.csv").write_text(
            "ts_ns,op,status\n100,push,ok\n200,push,ok\n", encoding="utf-8")
        (self.dir / "iterations.csv").write_text("worker,iteration\n", encoding="utf-8")
        (self.dir / "convergence.csv").write_text("iteration,dist_to_target\n", encoding="utf-8")
        with (self.dir / "events.csv").open("w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=("ts_ns", "action", "args", "outcome"))
            writer.writeheader()
            writer.writerow({"ts_ns": 100, "action": "kill_server",
                             "args": json.dumps({"phase": "after_pull_before_push",
                                                 "confirmed_process_kill": True}),
                             "outcome": "injected"})
        self.write_summary()

    def write_summary(self):
        (self.dir / "summary.json").write_text(
            json.dumps(self.summary), encoding="utf-8")

    def test_config_includes_required_rows_and_categories(self):
        cases = validate_config(self.config)
        self.assertGreaterEqual(len(cases), 8)
        self.assertEqual({c["classification"] for c in cases},
                         {"RECOVERED", "FAIL_CLOSED"})

    def test_missing_case_or_category_fails_validation(self):
        self.config["cases"] = self.config["cases"][:-1]
        with self.assertRaisesRegex(ValueError, "Missing required"):
            validate_config(self.config)

    def test_invalid_fail_closed_classification_is_rejected(self):
        for case in self.config["cases"]:
            if case["id"] == "migration_partial_postpublication":
                case["classification"] = "RECOVERED"
        with self.assertRaisesRegex(ValueError, "FAIL_CLOSED"):
            validate_config(self.config)

    def test_x6_validation_uses_actual_oracle_events_and_ops(self):
        result = check_x6(self.dir / "summary.json")
        self.assertEqual(result["completed_pushes"], 2)
        self.assertEqual(result["recovery_s"], 1.8)
        self.assertEqual(result["postfault_only_recovery_s"], 3.0)
        self.assertEqual(len(result["x6_summary_sha256"]), 64)

    def test_bad_oracle_cannot_be_counted_as_pass(self):
        self.summary["offline_oracle"]["matches"] = False
        self.write_summary()
        with self.assertRaisesRegex(ValueError, "oracle"):
            check_x6(self.dir / "summary.json")

    def test_missing_kill_event_cannot_be_counted_as_pass(self):
        (self.dir / "events.csv").write_text("ts_ns,action,args,outcome\n")
        with self.assertRaisesRegex(ValueError, "kill event"):
            check_x6(self.dir / "summary.json")

    def test_missing_or_duplicate_ops_rejected(self):
        (self.dir / "ops.csv").write_text("ts_ns,op,status\n100,push,ok\n")
        with self.assertRaisesRegex(ValueError, "ops.csv"):
            check_x6(self.dir / "summary.json")

    def test_recovery_decomposition_must_add_up(self):
        self.summary["recovery_times"]["decomposition"]["T_rec_first_push_s"] = .2
        self.write_summary()
        with self.assertRaisesRegex(ValueError, "add up"):
            check_x6(self.dir / "summary.json")

    def test_expected_fail_closed_is_not_misreported_as_recovered(self):
        target = next(c for c in self.config["cases"]
                      if c["id"] == "migration_partial_postpublication")
        row = make_row(target, passed=True, seed=42, evidence="test_f34")
        self.assertEqual(row["result"], "EXPECTED_FAIL_CLOSED")
        self.assertIn("NOT availability", row["tested_guarantee"])
        self.assertEqual(row["recovery_s"], "")

    def test_failing_test_never_advertises_a_guarantee(self):
        target = self.config["cases"][0]
        row = make_row(target, passed=False, seed=42, evidence="failed test")
        self.assertEqual(row["result"], "FAIL")
        self.assertEqual(row["tested_guarantee"], "UNVERIFIED")

    def test_csv_matrix_schema_is_stable(self):
        rows = [make_row(self.config["cases"][0], passed=True, seed=42,
                         evidence="X6", metrics={"recovery_s": 2.1})]
        write_csv(self.dir / "matrix.csv", rows)
        with (self.dir / "matrix.csv").open(newline="", encoding="utf-8") as inp:
            row = next(csv.DictReader(inp))
        self.assertEqual(row["case_id"], "x6_primary_mid_iteration")
        self.assertEqual(row["result"], "PASS")
        self.assertEqual(row["recovery_s"], "2.1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
