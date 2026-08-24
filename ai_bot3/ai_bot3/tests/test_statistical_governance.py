from __future__ import annotations

import sys
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.statistical_governance import (
    TrialLedger,
    TrialRecord,
    aligned_daily_mark_to_market_returns,
    cscv_probability_of_backtest_overfitting,
    deflated_sharpe_evidence,
    deflated_sharpe_ratio,
    final_evaluation_statistical_evidence,
    probabilistic_sharpe_ratio,
    statistical_overfit_evidence,
)


class StatisticalGovernanceTests(unittest.TestCase):
    def test_multiple_trials_deflate_the_same_sharpe(self):
        one = deflated_sharpe_ratio(0.4, 200, number_of_trials=1)
        many = deflated_sharpe_ratio(0.4, 200, number_of_trials=100)
        self.assertGreater(one, many)
        self.assertGreater(probabilistic_sharpe_ratio(0.4, 0.0, 200), 0.5)

    def test_deflated_sharpe_evidence_uses_cluster_returns_and_all_trials(self):
        selected = np.asarray([0.003 if index % 5 else -0.001 for index in range(80)])
        alternatives = np.column_stack(
            [selected, selected * 0.7, np.roll(selected, 1) * 0.5]
        )

        evidence = deflated_sharpe_evidence(
            selected,
            number_of_trials=25,
            trial_return_matrix=alternatives,
        )

        self.assertTrue(evidence["complete"])
        self.assertEqual(evidence["sample_count"], 80)
        self.assertEqual(evidence["number_of_trials"], 25)
        self.assertGreaterEqual(
            evidence["sharpe_std_used"],
            evidence["sampling_error_sharpe_std_floor"],
        )
        self.assertGreater(evidence["deflated_sharpe_probability"], 0.5)

    def test_cscv_distinguishes_persistent_edge_from_partition_overfit(self):
        rows_per_partition = 10
        persistent = np.column_stack(
            [
                np.full(80, 0.004),
                np.full(80, 0.002),
                np.full(80, -0.001),
            ]
        )
        stable = cscv_probability_of_backtest_overfitting(persistent)
        self.assertTrue(stable["complete"])
        self.assertEqual(stable["probability_of_backtest_overfitting"], 0.0)

        overfit = np.empty((80, 8), dtype=float)
        baselines = np.arange(8, dtype=float) * 1e-6
        overfit[:] = baselines
        for partition in range(8):
            start = partition * rows_per_partition
            overfit[start : start + rows_per_partition, partition] += 0.10
        unstable = cscv_probability_of_backtest_overfitting(overfit)
        self.assertTrue(unstable["complete"])
        self.assertGreater(unstable["probability_of_backtest_overfitting"], 0.05)

    def test_cscv_fails_closed_on_zero_trade_ties(self):
        result = cscv_probability_of_backtest_overfitting(np.zeros((80, 2)))
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "ambiguous_in_sample_strategy_winner")

    def test_daily_mtm_returns_align_zero_days_and_open_position_marks(self):
        @dataclass(frozen=True)
        class Point:
            observed_at: datetime
            equity_usdt: float

        @dataclass(frozen=True)
        class Report:
            initial_equity_usdt: float
            equity_curve: tuple[Point, ...]

        start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        first = Report(
            100.0,
            (
                Point(start, 101.0),
                Point(start + timedelta(days=2), 102.01),
            ),
        )
        second = Report(100.0, (Point(start + timedelta(days=1), 99.0),))

        dates, matrix = aligned_daily_mark_to_market_returns(
            (first, second),
            (start, start + timedelta(days=2)),
        )

        self.assertEqual(dates, ("2026-01-01", "2026-01-02", "2026-01-03"))
        np.testing.assert_allclose(matrix[:, 0], [0.01, 0.0, 0.01])
        np.testing.assert_allclose(matrix[:, 1], [0.0, -0.01, 0.0])

    def test_combined_statistical_evidence_fails_closed_when_variants_tie(self):
        @dataclass(frozen=True)
        class Report:
            initial_equity_usdt: float = 100.0
            equity_curve: tuple[object, ...] = ()

        timestamps = tuple(
            datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=index)
            for index in range(30)
        )
        result = statistical_overfit_evidence(
            Report(),
            (Report(), Report()),
            timestamps,
            number_of_trials=10,
        )

        self.assertFalse(result["complete"])
        self.assertIsNone(result["deflated_sharpe_probability"])
        self.assertIsNone(result["probability_of_backtest_overfitting"])

    def test_final_evaluation_reuses_development_pbo_without_variant_scoring(self):
        @dataclass(frozen=True)
        class Point:
            observed_at: datetime
            equity_usdt: float

        @dataclass(frozen=True)
        class Report:
            initial_equity_usdt: float
            equity_curve: tuple[Point, ...]

        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        points = tuple(
            Point(
                start + timedelta(days=index),
                100.0 + index * 0.4 + (-0.1 if index % 5 == 0 else 0.1),
            )
            for index in range(30)
        )
        development = {
            "complete": True,
            "probability_of_backtest_overfitting": 0.01,
            "strategy_count": 2,
            "combination_count": 70,
            "cscv": {"method": "combinatorially_symmetric_cross_validation"},
        }

        result = final_evaluation_statistical_evidence(
            Report(100.0, points),
            tuple(point.observed_at for point in points),
            number_of_trials=100,
            frozen_development_evidence=development,
        )

        self.assertTrue(result["complete"])
        self.assertEqual(result["probability_of_backtest_overfitting"], 0.01)
        self.assertEqual(
            result["cscv"]["source"],
            "frozen_development_only_no_alternative_lockbox_scoring",
        )

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
