"""Unit tests for F5/X6 evidence mathematics; no gRPC cluster required."""

import csv
import json
from pathlib import Path
import tempfile
import unittest

from Part_F_Tests.f5_evidence import (
    ExperimentEvidence, compare_weights, fixed_gradient_oracle,
    recovery_decomposition, throughput_recovery,
)


class EvidenceTests(unittest.TestCase):
    def test_independent_oracle_ignores_retry_counts(self):
        # Two distinct worker updates; retries are not distinct logical Pushes.
        completed = [(11, 1, (7, 9)), (22, 1, (7,))]
        def gradient(worker, iteration, key, component):
            return worker / 11 + component + (key - 7) / 2
        actual = fixed_gradient_oracle(completed, gradient, learning_rate=.1)
        for value, target in zip(actual[7], [-.3, -.5]):
            self.assertAlmostEqual(value, target)
        for value, target in zip(actual[9], [-.2, -.3]):
            self.assertAlmostEqual(value, target)

    def test_oracle_rejects_invalid_records(self):
        def gradient(*_): return 1.0
        with self.assertRaises(ValueError):
            fixed_gradient_oracle([(1, 1, (7, 7))], gradient)
        with self.assertRaises(ValueError):
            fixed_gradient_oracle([(1, 0, (7,))], gradient)
        with self.assertRaises(TypeError):
            fixed_gradient_oracle([(1, 1, (7,))], None)

    def test_compare_weights_reports_disagreements(self):
        correct = compare_weights({7: [-.1, -.2]}, {7: [-.1, -.2]})
        self.assertTrue(correct["matches"])
        mismatch = compare_weights({7: [-.1, -.2]}, {7: [-.2, -.2]})
        self.assertFalse(mismatch["matches"])
        self.assertEqual(mismatch["mismatches"][0]["key"], 7)
        with self.assertRaises(ValueError):
            compare_weights({7: [1]}, {8: [1]})

    def test_throughput_pre_five_seconds_median(self):
        # Counts in [5,6), [6,7), ... [9,10) are 2, 4, 3, 5, 1.
        times = ([5.1, 5.2] + [6.1, 6.2, 6.3, 6.4] +
                 [7.1, 7.2, 7.3] + [8.1, 8.2, 8.3, 8.4, 8.5] + [9.1] +
                 [10.6, 10.7, 10.8, 11.1, 11.2, 11.3])
        m = throughput_recovery(times, 10.0, last_time_s=12.5)
        self.assertEqual(m["baseline_pushes_per_second"], [2, 4, 3, 5, 1])
        self.assertEqual(m["theta_pre"], 3.0)
        self.assertAlmostEqual(m["threshold_90pct"], 2.7)
        self.assertIsNotNone(m["throughput_recovery_s"])
        self.assertIsNotNone(m["postfault_only_recovery_s"])

    def test_sliding_window_carryover_is_visible(self):
        # Pre-fault rate=2. All postfault completions cease until t=11.5.
        times = [i + x for i in range(5, 10) for x in (.7, .8)] + [11.5, 11.6]
        m = throughput_recovery(times, 10.0, last_time_s=12.6)
        self.assertEqual(m["theta_pre"], 2)
        self.assertAlmostEqual(m["throughput_recovery_s"], .1)  # carry-over
        self.assertGreater(m["postfault_only_recovery_s"], 1.5)

    def test_zero_baseline_is_not_false_recovery(self):
        m = throughput_recovery([], 10.0, last_time_s=12)
        self.assertIsNone(m["throughput_recovery_s"])
        self.assertIsNone(m["postfault_only_recovery_s"])

    def test_recovery_components_are_ordered(self):
        record = {"detected_at_s": 10.5, "promoted_at_s": 11.0,
                  "ready_at_s": 11.4}
        r = recovery_decomposition(10, record, 11.6)
        self.assertAlmostEqual(r["T_detect_s"], .5)
        self.assertAlmostEqual(r["T_promote_s"], .5)
        self.assertAlmostEqual(r["T_resume_to_first_push_s"], .6)
        self.assertAlmostEqual(r["T_rec_first_push_s"], 1.6)
        self.assertAlmostEqual(r["controller_ready_s"], 1.4)
        with self.assertRaises(ValueError):
            recovery_decomposition(10, {"detected_at_s": 9.9,
                "promoted_at_s": 11.0, "ready_at_s": 11.4})

    def test_five_file_output_contains_actual_schema(self):
        evidence = ExperimentEvidence()
        evidence.add_op(ts_ns=1000, worker=11, iteration=1, op="push",
                        n_keys=2, bytes_out=20, bytes_in=5, latency_ms=1.2,
                        status="ok", retries=1, monotonic_end_s=10.0)
        evidence.add_iteration(11, 1, 100, 1000, [7, 8])
        evidence.add_event(1200, "kill_server", {"server": "s1"}, "injected")
        with tempfile.TemporaryDirectory() as directory:
            path = evidence.write(directory, {"oracle": "passed"})
            self.assertEqual({p.name for p in path.iterdir()}, {
                "ops.csv", "iterations.csv", "convergence.csv", "events.csv",
                "summary.json"})
            with (path / "ops.csv").open(newline="") as handle:
                row = next(csv.DictReader(handle))
            self.assertEqual(row["retries"], "1")
            self.assertEqual(row["bytes_out"], "20")
            with (path / "events.csv").open(newline="") as handle:
                ev = next(csv.DictReader(handle))
            self.assertEqual(json.loads(ev["args"])["server"], "s1")
            self.assertEqual(json.loads((path / "summary.json").read_text()),
                             {"oracle": "passed"})
        self.assertEqual(evidence.push_times_s, [10.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
