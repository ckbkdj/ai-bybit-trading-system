from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import (
    MINIMUM_COVERAGE_DAYS,
    ProfitabilityRebuildConfig,
    _engineer_features,
    _market_bars,
    _panel_rows,
    validate_source_coverage,
)


def _frame(days: int) -> pd.DataFrame:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return pd.DataFrame(
        {
            "open_at": [start, start + timedelta(days=days)],
            "close_at": [start + timedelta(minutes=3), start + timedelta(days=days, minutes=3)],
        }
    )


def test_short_horizon_coverage_cannot_silently_collapse_to_six_or_31_days(tmp_path):
    with pytest.raises(ValueError, match="coverage"):
        validate_source_coverage(_frame(6), "3m")
    with pytest.raises(ValueError, match="coverage"):
        validate_source_coverage(_frame(31), "15m")
    evidence = validate_source_coverage(_frame(181), "3m")
    assert evidence["coverage_days"] >= MINIMUM_COVERAGE_DAYS["3m"]

    config = ProfitabilityRebuildConfig(
        feature_store_path=tmp_path / "features.sqlite3",
        output_dir=tmp_path / "reports",
        trial_ledger_path=tmp_path / "trials.sqlite3",
        model_output_dir=tmp_path / "models",
        code_commit="1" * 40,
    )
    assert config.max_bars_per_symbol >= 175_200


def test_label_decisions_do_not_treat_overlapping_execution_windows_as_independent():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(120):
        open_at = start + timedelta(seconds=180 * index)
        price = 100.0 + index * 0.01
        rows.append(
            {
                "symbol": "BTCUSDT",
                "open_at": open_at,
                "close_at": open_at + timedelta(seconds=179),
                "open": price,
                "high": price * 1.002,
                "low": price * 0.998,
                "close": price * 1.0002,
                "volume": 1_000.0 + index,
            }
        )
    frame = pd.DataFrame(rows)
    bars = _market_bars(_engineer_features(frame))
    labels = _panel_rows(frame, 180, bars)
    decision_times = sorted({row["decision_at"] for row in labels})

    assert decision_times
    assert all(
        later - earlier >= timedelta(seconds=270)
        for earlier, later in zip(decision_times, decision_times[1:])
    )
    for decision_at in decision_times:
        alternatives = [row for row in labels if row["decision_at"] == decision_at]
        assert {row["side"] for row in alternatives} == {"BUY", "SELL"}
