from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.calibration_coverage import (
    directional_calibration_rows,
    evaluate_quantile_coverage,
)
from core.models.two_stage import TwoStagePrediction


def _records(*, shift: float = 0.0) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    output = []
    for regime_index, regime in enumerate(("normal", "risk_off")):
        for index in range(100):
            decision_at = start + timedelta(
                days=regime_index * 100,
                minutes=index,
            )
            actual = (index + 0.5) / 100.0
            for symbol in ("BTCUSDT", "ETHUSDT"):
                output.append(
                    {
                        "horizon_sec": 180,
                        "symbol": symbol,
                        "regime": regime,
                        "decision_at": decision_at,
                        "actual_net_return": actual,
                        "return_p10": 0.10 + shift,
                        "return_p50": 0.50 + shift,
                        "return_p90": 0.90 + shift,
                    }
                )
    return output


def _prediction(*, p_up: float, p_down: float) -> TwoStagePrediction:
    return TwoStagePrediction(
        p_down=p_down,
        p_flat=0.1,
        p_up=p_up,
        expected_net_return=0.001,
        return_p10=-0.01,
        return_p50=0.001,
        return_p90=0.01,
        expected_mae=0.01,
        expected_mfe=0.02,
        uncertainty=0.01,
        meta_trade_probability=0.8,
        lower_bound_net_edge=0.0001,
        decision="TRADE",
    )


def test_outer_oos_quantile_coverage_passes_all_preregistered_scopes():
    evidence = evaluate_quantile_coverage(_records(), required_horizons=[180])

    assert evidence["status"] == "PASSED"
    assert evidence["failed_group_count"] == 0
    assert {
        group["scope"] for group in evidence["groups"]
    } == {
        "horizon",
        "horizon_symbol",
        "horizon_regime",
        "horizon_symbol_regime",
    }
    assert all(
        group["prediction_count"] >= group["independent_timestamp_count"]
        for group in evidence["groups"]
    )


def test_miscalibrated_outer_oos_quantiles_fail_closed():
    evidence = evaluate_quantile_coverage(
        _records(shift=-0.20), required_horizons=[180]
    )

    assert evidence["status"] == "FAILED"
    assert evidence["failed_group_count"] > 0


def test_directional_calibration_counts_paired_actions_once():
    decision_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "horizon_sec": 180,
                "decision_at": decision_at,
                "regime": "normal",
                "side": "BUY",
                "net_return": 0.01,
            },
            {
                "symbol": "BTCUSDT",
                "horizon_sec": 180,
                "decision_at": decision_at,
                "regime": "normal",
                "side": "SELL",
                "net_return": -0.01,
            },
        ]
    )
    predictions = [
        _prediction(p_up=0.8, p_down=0.1),
        _prediction(p_up=0.8, p_down=0.1),
    ]

    rows = directional_calibration_rows(frame, predictions)

    assert len(rows) == 1
    assert rows[0]["selected_side"] == "BUY"
    assert rows[0]["actual_net_return"] == 0.01
