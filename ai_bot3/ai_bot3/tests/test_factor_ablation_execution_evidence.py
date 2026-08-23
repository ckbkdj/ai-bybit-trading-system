from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    _ablation_execution_evidence,
    _failed_ablation_execution_result,
)


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
