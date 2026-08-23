from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    _ablation_execution_evidence,
    _ablation_signals_from_predictions,
    _failed_ablation_execution_result,
)
from core.models.two_stage import TwoStagePrediction


def test_zero_trade_ablation_is_not_accepted_as_evaluated_evidence():
    evidence = _ablation_execution_evidence(
        [
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
        ],
        [
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
        ],
    )
    assert evidence["passed"] is False
    assert evidence["baseline"]["trades"] == 0
    assert evidence["augmented"]["trades"] == 0

    result = _failed_ablation_execution_result(
        cadence="short",
        group="bybit_orderbook",
        factors=("ofi_1m",),
        fold_evidence=(
            {"status": "EVALUATED_OOS", "test_rows": 200},
            {"status": "EVALUATED_OOS", "test_rows": 200},
        ),
        baseline_folds=(
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
        ),
        augmented_folds=(
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
            {"net_return": 0.0, "signal_count": 0, "trade_count": 0},
        ),
    )
    assert result is not None
    assert result["oos_ablation_status"] == "FAILED_INSUFFICIENT_OOS_TRADES"
    assert result["evaluated"] is False
    assert result["formal_feature_set"] is False


def test_ablation_requires_real_trades_across_multiple_oos_folds_in_both_arms():
    evidence = _ablation_execution_evidence(
        [
            {"net_return": 0.01, "signal_count": 20, "trade_count": 15},
            {"net_return": 0.01, "signal_count": 20, "trade_count": 15},
        ],
        [
            {"net_return": 0.02, "signal_count": 22, "trade_count": 16},
            {"net_return": 0.01, "signal_count": 20, "trade_count": 14},
        ],
    )
    assert evidence["passed"] is True
    assert evidence["baseline"]["traded_folds"] == 2
    assert evidence["augmented"]["traded_folds"] == 2


def test_ablation_research_budget_measures_rankings_without_faking_deployable_edge():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    predictions = []
    for decision in range(100):
        decision_at = start + timedelta(minutes=3 * decision)
        for side in ("BUY", "SELL"):
            rows.append(
                {
                    "symbol": "BTCUSDT",
                    "decision_at": decision_at,
                    "available_at": decision_at,
                    "side": side,
                    "reference_price": 100.0,
                    "take_profit_bps": 100.0,
                    "stop_loss_bps": 75.0,
                    "regime": "neutral",
                }
            )
            predictions.append(
                TwoStagePrediction(
                    p_down=0.2 if side == "BUY" else 0.6,
                    p_flat=0.2,
                    p_up=0.6 if side == "BUY" else 0.2,
                    expected_net_return=decision / 100_000.0,
                    return_p10=-0.01,
                    return_p50=0.0,
                    return_p90=0.01,
                    expected_mae=0.001,
                    expected_mfe=0.001,
                    uncertainty=0.5,
                    meta_trade_probability=0.2,
                    lower_bound_net_edge=-0.001,
                    decision="NO_TRADE",
                )
            )

    signals = _ablation_signals_from_predictions(
        pd.DataFrame(rows), predictions, horizon_sec=180
    )

    assert len(signals) == 2
    assert all(signal.lower_bound_net_edge < 0 for signal in signals)
    assert all(signal.signal_id.startswith("ablation_") for signal in signals)
    assert {signal.decision_at for signal in signals} == {
        start + timedelta(minutes=3 * 98),
        start + timedelta(minutes=3 * 99),
    }
