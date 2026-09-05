from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.backtest import CostAwareBacktest, TradeIntent
from core.evaluation.ablation import compare_factor_groups
from core.evaluation.metrics import prediction_metrics, trading_metrics
from core.evaluation.time_series_split import PurgedWalkForwardSplit
from core.features.cross_asset import macro_surprise, regime_weighted_rotation


class EvaluationProtocolTests(unittest.TestCase):
    def test_walk_forward_split_has_purge_and_embargo(self):
        splitter = PurgedWalkForwardSplit(
            train_size=10, test_size=3, purge_size=2, embargo_size=2
        )
        splits = list(splitter.split(list(range(30))))
        self.assertGreaterEqual(len(splits), 2)
        for split in splits:
            self.assertGreater(min(split.test_indices) - max(split.train_indices), 2)
            self.assertTrue(set(split.train_indices).isdisjoint(split.test_indices))
        self.assertGreater(min(splits[1].test_indices) - max(splits[0].test_indices), 2)

    def test_costs_and_partial_fills_reduce_backtest_result(self):
        engine = CostAwareBacktest()
        full = engine.simulate(TradeIntent("BUY", 1000, 100, 101))
        partial = engine.simulate(
            TradeIntent(
                "BUY", 1000, 100, 101, fill_fraction=0.5,
                entry_slippage_bps=3, exit_slippage_bps=3, funding_bps=2,
            )
        )
        self.assertLess(full.net_pnl, full.gross_pnl)
        self.assertLess(partial.net_pnl, full.net_pnl)
        metrics = trading_metrics([full, partial], 10000)
        self.assertIn("max_drawdown", metrics)
        self.assertGreater(metrics["cost_to_gross_profit"], 0)

    def test_prediction_metrics_include_calibration_sensitive_scores(self):
        metrics = prediction_metrics(
            [0.8, 0.2], [1, 0], [0.01, -0.01], [0.012, -0.008],
            lower_interval=[0.0, -0.02], upper_interval=[0.02, 0.0],
            quantile_predictions={0.5: [0.01, -0.01]},
        )
        self.assertIn("log_loss", metrics)
        self.assertIn("brier_score", metrics)
        self.assertIn("calibration_error", metrics)
        self.assertIn("auc", metrics)
        self.assertIn("pinball_loss", metrics)
        self.assertEqual(metrics["prediction_interval_coverage"], 1.0)
        self.assertEqual(metrics["direction_accuracy"], 1.0)

    def test_cross_asset_direction_comes_from_regime_weights(self):
        inputs = {"gold": 1.0, "real_yield": -0.5}
        positive = regime_weighted_rotation(
            inputs, regime="debasement", trained_weights={"debasement": {"gold": 1, "real_yield": -1}}
        )
        negative = regime_weighted_rotation(
            inputs, regime="liquidity_crisis", trained_weights={"liquidity_crisis": {"gold": -1, "real_yield": 1}}
        )
        self.assertGreater(positive.value, 0)
        self.assertLess(negative.value, 0)
        self.assertEqual(positive.semantics, "inferred_rotation_proxy")
        self.assertIsNone(macro_surprise(3.1, 3.0, None)["actual_minus_consensus"])

    def test_factor_group_ablation_requires_stable_out_of_sample_improvement(self):
        baseline = [{"net_return": 0.01}, {"net_return": 0.02}, {"net_return": -0.01}]
        results = compare_factor_groups(
            baseline,
            {
                "crypto.derivatives.v1": [
                    {"net_return": 0.02}, {"net_return": 0.025}, {"net_return": 0.0}
                ],
                "commodity.gold.v1": [
                    {"net_return": 0.03}, {"net_return": 0.01}, {"net_return": -0.02}
                ],
                "weak.noisy.v1": [
                    {"net_return": 0.015},
                    {"net_return": 0.025},
                    {"net_return": -0.0119},
                ],
            },
            primary_metric="net_return",
            higher_is_better=True,
            minimum_mean_improvement=0.002,
            minimum_improved_fold_ratio=2 / 3,
        )
        by_group = {item.factor_group: item for item in results}
        self.assertTrue(by_group["crypto.derivatives.v1"].retained)
        self.assertGreater(
            by_group["crypto.derivatives.v1"].bootstrap_lower_mean_improvement,
            0.002,
        )
        self.assertFalse(by_group["commodity.gold.v1"].retained)
        self.assertLessEqual(
            by_group["commodity.gold.v1"].bootstrap_lower_mean_improvement,
            0.002,
        )
        # Mean, 2/3-fold ratio and worst-fold rules alone would retain this arm;
        # the paired bootstrap lower bound correctly rejects its uncertainty.
        self.assertGreater(by_group["weak.noisy.v1"].mean_improvement, 0.002)
        self.assertEqual(by_group["weak.noisy.v1"].improved_fold_ratio, 2 / 3)
        self.assertGreaterEqual(by_group["weak.noisy.v1"].worst_fold_improvement, -0.002)
        self.assertFalse(by_group["weak.noisy.v1"].retained)


if __name__ == "__main__":
    unittest.main()
