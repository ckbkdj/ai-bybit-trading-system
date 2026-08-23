from __future__ import annotations

import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from soak_monitor import SoakMonitor, evaluate_soak
from ticket_store import ExecutionStore


class SoakMonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ExecutionStore(Path(self.temp.name) / "execution.sqlite3")

    def tearDown(self):
        self.temp.cleanup()

    def test_short_observation_is_blocked_and_shutdown_is_recorded(self):
        monitor = SoakMonitor(self.store, run_id="run-short")
        self.assertFalse(monitor.start())
        metrics = monitor.sample()
        self.assertIn("process_rss_bytes", metrics)
        monitor.stop()
        result = evaluate_soak(self.store.db_path)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("duration", " ".join(result["failures"]))
        with closing(self.store.connect()) as connection:
            row = connection.execute(
                "SELECT clean_shutdown FROM service_runs WHERE run_id='run-short'"
            ).fetchone()
        self.assertEqual(row["clean_shutdown"], 1)

    def test_unclean_previous_run_is_counted_once(self):
        first = SoakMonitor(self.store, run_id="run-abandoned")
        self.assertFalse(first.start())
        replacement = SoakMonitor(self.store, run_id="run-replacement")
        self.assertTrue(replacement.start())
        replacement.stop()
        third = SoakMonitor(self.store, run_id="run-third")
        self.assertFalse(third.start())
        third.stop()
        with closing(self.store.connect()) as connection:
            count = connection.execute(
                """SELECT COALESCE(SUM(metric_value),0) FROM runtime_metrics
                   WHERE metric_name='unexpected_restart'"""
            ).fetchone()[0]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
