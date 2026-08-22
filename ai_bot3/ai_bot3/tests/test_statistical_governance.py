from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.statistical_governance import (
    TrialLedger,
    TrialRecord,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
)


class StatisticalGovernanceTests(unittest.TestCase):
    def test_multiple_trials_deflate_the_same_sharpe(self):
        one = deflated_sharpe_ratio(0.4, 200, number_of_trials=1)
        many = deflated_sharpe_ratio(0.4, 200, number_of_trials=100)
        self.assertGreater(one, many)
        self.assertGreater(probabilistic_sharpe_ratio(0.4, 0.0, 200), 0.5)

    def test_trial_ledger_is_idempotent_and_counts_rejected_trials(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = TrialLedger(Path(directory) / "trials.sqlite3")
            record = TrialRecord(
                "trial_001",
                "brain_hist_gradient_boosting",
                "data_signature",
                TrialLedger.parameter_hash({"max_iter": 240}),
                "commit",
                "rejected",
                {"sharpe": -0.1},
            )
            self.assertTrue(ledger.append(record))
            self.assertFalse(ledger.append(record))
            self.assertEqual(ledger.trial_count(), 1)
            with self.assertRaises(ValueError):
                ledger.append(
                    TrialRecord(**{**record.__dict__, "status": "completed"})
                )


if __name__ == "__main__":
    unittest.main()
