from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.models.two_stage import TwoStageConfig
from core.training.nested_walk_forward import NestedWalkForwardSelector


def _frame(rows: int = 320) -> pd.DataFrame:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    signal = np.sin(np.arange(rows) / 12.0)
    net = signal * 0.0015 - 0.0002
    decision = [start + timedelta(minutes=5 * index) for index in range(rows)]
    return pd.DataFrame(
        {
            "decision_at": decision,
            "label_available_at": [value + timedelta(minutes=3) for value in decision],
            "symbol": np.where(np.arange(rows) % 2, "BTCUSDT", "ETHUSDT"),
            "side": np.where(signal >= 0, "BUY", "SELL"),
            "signal": signal,
            "net_return": net,
            "mae": 0.0008 + np.maximum(0.0, -net),
            "mfe": 0.001 + np.maximum(0.0, net),
            "direction_label": np.where(net > 0.0001, "up", np.where(net < -0.0001, "down", "flat")),
        }
    )


def test_nested_selector_uses_inner_oos_only_and_fits_once_for_outer_test():
    frame = _frame()
    outer_train = frame.iloc[:260].reset_index(drop=True)
    outer_test = frame.iloc[260:].reset_index(drop=True)
    selector = NestedWalkForwardSelector(
        (
            TwoStageConfig(direction_iterations=30, meta_iterations=30, ridge=0.5),
            TwoStageConfig(direction_iterations=30, meta_iterations=30, ridge=2.0),
        ),
        inner_folds=3,
    )
    selection = selector.select_and_fit(outer_train, ["signal", "symbol"])
    predictions = selection.model.predict(outer_test)
    assert len(predictions) == len(outer_test)
    assert selection.audit["selection_data"] == "inner_walk_forward_oos_only"
    assert selection.audit["outer_oos_used_for_selection"] is False
    assert selection.audit["inner_fold_count"] >= 2
    assert len(selection.candidate_results) == 2
    assert all(result["outer_oos_rows_seen"] == 0 for result in selection.candidate_results)
    assert selection.model.training_audit["level_two_training_source"] == "out_of_fold_level_one"
