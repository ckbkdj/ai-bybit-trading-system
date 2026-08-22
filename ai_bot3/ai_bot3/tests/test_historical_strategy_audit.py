from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.historical_strategy_audit import (
    HistoricalPrediction,
    PortfolioAuditConfig,
    audit_portfolio,
)


def _row(identifier: int, created: int, predicted: float, actual: float) -> HistoricalPrediction:
    return HistoricalPrediction(
        identifier,
        created,
        created + 60,
        "BTCUSDT",
        "1m",
        "scalping",
        predicted,
        None,
        None,
        None,
        actual,
    )


def test_strict_replay_rejects_legacy_rows_missing_governance_fields():
    result = audit_portfolio([_row(1, 0, 0.01, 0.02)])
    assert result["eligible_trades"] == 0
    assert result["rejections"]["missing_recorded_direction"] == 1


def test_diagnostic_is_cost_aware_deoverlapped_and_reports_drawdown_limit():
    config = PortfolioAuditConfig(
        initial_equity_usdt=1000,
        target_exposure_pct=0.1,
        max_gross_exposure_pct=0.1,
        minimum_signal_return=0.0,
        round_trip_fee_bps=10,
        round_trip_slippage_bps=0,
        require_recorded_direction=False,
        require_confidence=False,
        require_model_version=False,
    )
    rows = [
        _row(1, 0, 0.01, 0.02),
        _row(2, 30, 0.01, 0.50),  # overlaps and must not be cherry-picked
        _row(3, 61, -0.01, 0.02),
    ]
    result = audit_portfolio(rows, config)
    assert result["eligible_trades"] == 2
    assert result["rejections"]["overlapping_same_signal"] == 1
    assert result["net_profit_usdt"] < 0.0  # second accepted trade is a losing short after costs
    assert result["realized_close_to_close_max_drawdown"] > 0.0
    assert result["intratrade_drawdown_available"] is False
