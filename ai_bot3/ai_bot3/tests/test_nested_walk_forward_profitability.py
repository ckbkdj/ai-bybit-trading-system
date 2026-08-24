from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.models.two_stage import TwoStageConfig, TwoStagePrediction
from core.training.nested_walk_forward import NestedWalkForwardSelector, _selected_rows


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
    selection = selector.select_and_fit(
        outer_train,
        ["signal", "symbol"],
        score_calibration_quantile=0.98,
        score_calibration_tail_penalty=0.5,
    )
    predictions = selection.model.predict(outer_test)
    assert len(predictions) == len(outer_test)
    assert selection.audit["selection_data"] == "inner_walk_forward_oos_only"
    assert selection.audit["outer_oos_used_for_selection"] is False
    assert selection.audit["inner_oos_overlap_policy"].startswith(
        "one_active_position_per_symbol"
    )
    assert selection.audit["inner_fold_count"] >= 2
    assert selection.audit["inner_purge_sec"] == 180
    assert selection.audit["inner_embargo_sec"] == 45
    assert len(selection.candidate_results) == 2
    assert selection.oof_score_threshold is not None
    assert np.isfinite(selection.oof_score_threshold)
    assert selection.audit["score_calibration"]["source"] == (
        "inner_walk_forward_oos_predictions_only"
    )
    assert selection.audit["score_calibration"]["quantile"] == 0.98
    assert all(result["outer_oos_rows_seen"] == 0 for result in selection.candidate_results)
    for result in selection.candidate_results:
        for fold in result["inner_folds"]:
            train_end = pd.Timestamp(fold["train_decision_end"])
            validation_start = pd.Timestamp(fold["validation_start"])
            assert validation_start - train_end >= timedelta(seconds=180)
            assert pd.Timestamp(fold["train_label_available_max"]) < validation_start
    assert selection.model.training_audit["level_two_training_source"] == "out_of_fold_level_one"


def test_inner_oos_scoring_does_not_count_overlapping_labels_as_trades():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 6,
            "side": ["BUY"] * 6,
            "decision_at": [start + timedelta(minutes=index) for index in range(6)],
            "label_available_at": [
                start + timedelta(minutes=index + 3) for index in range(6)
            ],
            "net_return": [0.001] * 6,
            "mae": [0.0002] * 6,
        }
    )
    prediction = TwoStagePrediction(
        p_down=0.1,
        p_flat=0.1,
        p_up=0.8,
        expected_net_return=0.001,
        return_p10=0.0002,
        return_p50=0.001,
        return_p90=0.002,
        expected_mae=0.0002,
        expected_mfe=0.002,
        uncertainty=0.2,
        meta_trade_probability=0.8,
        lower_bound_net_edge=0.0001,
        decision="TRADE",
    )

    selected = _selected_rows(frame, [prediction] * len(frame))

    assert selected["decision_at"].tolist() == [
        start,
        start + timedelta(minutes=3),
    ]
