from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.features.profitability_technical import (
    LEGACY_BRAIN_FEATURE_COLUMNS,
    engineer_profitability_features,
)


def _frame(rows: int = 140) -> pd.DataFrame:
    close = 100.0 + np.linspace(0.0, 3.0, rows) + np.sin(np.arange(rows) / 7.0)
    return pd.DataFrame(
        {
            "open": close * (1.0 - 0.0005),
            "high": close * 1.002,
            "low": close * 0.998,
            "close": close,
            "volume": 1_000.0 + np.arange(rows) * 3.0,
        }
    )


def test_legacy_brain_features_are_causal_and_finite_after_warmup():
    original = _frame()
    revised_future = original.copy()
    revised_future.loc[100:, ["open", "high", "low", "close", "volume"]] *= 5.0

    first = engineer_profitability_features(original)
    second = engineer_profitability_features(revised_future)

    assert set(LEGACY_BRAIN_FEATURE_COLUMNS).issubset(first.columns)
    assert np.isfinite(first.loc[60:, list(LEGACY_BRAIN_FEATURE_COLUMNS)]).all().all()
    pd.testing.assert_frame_equal(
        first.loc[:99, list(LEGACY_BRAIN_FEATURE_COLUMNS)],
        second.loc[:99, list(LEGACY_BRAIN_FEATURE_COLUMNS)],
    )
