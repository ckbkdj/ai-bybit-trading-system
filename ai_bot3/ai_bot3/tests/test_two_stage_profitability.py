from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.models.two_stage import TwoStageAlphaModel, TwoStageConfig
from core.models import two_stage


def _training_frame(rows: int = 160) -> pd.DataFrame:
    x = np.linspace(-2.0, 2.0, rows)
    net = 0.002 * x + 0.0002 * np.sin(np.arange(rows))
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    decision_at = [start + timedelta(minutes=5 * index) for index in range(rows)]
    return pd.DataFrame(
        {
            "signal": x,
            "symbol": np.where(np.arange(rows) % 2, "BTCUSDT", "ETHUSDT"),
            "net_return": net,
            "mae": 0.0005 + 0.0002 * np.abs(x),
            "mfe": 0.001 + 0.001 * np.maximum(x, 0),
            "direction_label": np.where(net > 0.0003, "up", np.where(net < -0.0003, "down", "flat")),
            "decision_at": decision_at,
            "label_available_at": [value + timedelta(minutes=3) for value in decision_at],
        }
    )


def test_two_stage_model_predicts_distribution_meta_label_and_never_self_promotes(tmp_path):
    frame = _training_frame()
    model = TwoStageAlphaModel(
        TwoStageConfig(direction_iterations=80, meta_iterations=80, learning_rate=0.04)
    ).fit(frame, ["signal", "symbol"])
    predictions = model.predict(frame.tail(8))
    assert len(predictions) == 8
    assert all(abs(item.p_up + item.p_flat + item.p_down - 1.0) < 1e-8 for item in predictions)
    assert all(item.return_p10 <= item.return_p50 <= item.return_p90 for item in predictions)
    assert all(item.decision in {"TRADE", "NO_TRADE"} for item in predictions)
    assert all(item.release_stage == "rejected" for item in predictions)
    assert model.training_audit["level_two_training_source"] == "out_of_fold_level_one"
    assert model.training_audit["return_calibration_source"] == "out_of_fold_residuals"
    assert model.training_audit["oof_rows"] > 0
    assert model.training_audit["oof_rows"] < len(frame)
    assert model.training_audit["pit_label_cutoff_enforced"] is True
    assert all(
        fold["purge_sec"] == 180 and fold["embargo_sec"] == 45
        for fold in model.training_audit["oof_folds"]
    )

    artifact = tmp_path / "two-stage.json"
    model.save(artifact)
    loaded = TwoStageAlphaModel.load(artifact)
    replay = loaded.predict(frame.tail(8))
    assert np.allclose(
        [item.expected_net_return for item in predictions],
        [item.expected_net_return for item in replay],
    )
    assert all(item.release_stage == "rejected" for item in replay)
    assert loaded.training_audit == model.training_audit


def test_direction_model_is_side_invariant_for_paired_action_alternatives():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(120):
        signal = float(np.sin(index / 8.0))
        decision_at = start + timedelta(days=index)
        direction = "up" if signal > 0.1 else "down" if signal < -0.1 else "flat"
        for side in ("BUY", "SELL"):
            aligned = (side == "BUY" and direction == "up") or (
                side == "SELL" and direction == "down"
            )
            rows.append(
                {
                    "signal": signal,
                    "symbol": "BTCUSDT",
                    "side": side,
                    "net_return": 0.002 if aligned else -0.001,
                    "mae": 0.0008,
                    "mfe": 0.0015,
                    "direction_label": direction,
                    "decision_at": decision_at,
                    "label_available_at": decision_at + timedelta(hours=1),
                }
            )
    frame = pd.DataFrame(rows)
    model = TwoStageAlphaModel(
        TwoStageConfig(direction_iterations=80, meta_iterations=40)
    ).fit(frame, ["signal", "symbol", "side"])
    predictions = model.predict(frame.tail(20))

    assert "side" not in model.direction_feature_columns
    assert model.training_audit["direction_training_paired_decisions_deduplicated"] is True
    assert model.training_audit["action_outcome_side_interactions"] == ["signal"]
    for buy, sell in zip(predictions[::2], predictions[1::2]):
        assert np.allclose(
            [buy.p_down, buy.p_flat, buy.p_up],
            [sell.p_down, sell.p_flat, sell.p_up],
        )
        assert buy.expected_net_return != sell.expected_net_return

    positive = pd.DataFrame(
        [
            {
                "signal": 1.0,
                "symbol": "BTCUSDT",
                "side": side,
            }
            for side in ("BUY", "SELL")
        ]
    )
    positive_predictions = model.predict(positive)
    assert positive_predictions[0].expected_net_return > positive_predictions[1].expected_net_return


def test_pooled_model_learns_regularized_symbol_specific_signal_response(tmp_path):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(180):
        signal = float(np.sin(index / 7.0))
        decision_at = start + timedelta(days=index)
        for symbol, symbol_sign in (("BTCUSDT", 1.0), ("ETHUSDT", -1.0)):
            market_edge = symbol_sign * signal
            direction = (
                "up" if market_edge > 0.1 else "down" if market_edge < -0.1 else "flat"
            )
            for side, side_sign in (("BUY", 1.0), ("SELL", -1.0)):
                rows.append(
                    {
                        "signal": signal,
                        "symbol": symbol,
                        "side": side,
                        "net_return": 0.004 * side_sign * market_edge - 0.0003,
                        "mae": 0.001 + 0.0002 * max(0.0, -side_sign * market_edge),
                        "mfe": 0.001 + 0.003 * max(0.0, side_sign * market_edge),
                        "direction_label": direction,
                        "decision_at": decision_at,
                        "label_available_at": decision_at + timedelta(hours=1),
                    }
                )
    frame = pd.DataFrame(rows)
    model = TwoStageAlphaModel(
        TwoStageConfig(
            direction_iterations=120,
            meta_iterations=60,
            minimum_symbol_head_rows=100,
        )
    ).fit(frame, ["signal", "symbol", "side"])
    alternatives = pd.DataFrame(
        [
            {"signal": 1.0, "symbol": symbol, "side": side}
            for symbol in ("BTCUSDT", "ETHUSDT")
            for side in ("BUY", "SELL")
        ]
    )
    predictions = model.predict(alternatives)

    assert sorted(model.symbol_net_weights) == ["BTCUSDT", "ETHUSDT"]
    assert sorted(model.symbol_direction_weights) == ["BTCUSDT", "ETHUSDT"]
    assert predictions[0].expected_net_return > predictions[1].expected_net_return
    assert predictions[3].expected_net_return > predictions[2].expected_net_return
    assert predictions[0].p_up > predictions[0].p_down
    assert predictions[2].p_down > predictions[2].p_up
    assert model.training_audit["pooled_model_structure"].endswith(
        "regularized_symbol_residual_heads"
    )

    artifact = tmp_path / "symbol-heads.json"
    model.save(artifact)
    loaded = TwoStageAlphaModel.load(artifact)
    replay = loaded.predict(alternatives)
    assert np.allclose(
        [item.expected_net_return for item in predictions],
        [item.expected_net_return for item in replay],
    )


def test_encoder_fits_from_one_matrix_and_standardizes_in_place():
    frame = pd.DataFrame(
        {
            "numeric": [1.0, float("nan"), 3.0, 5.0],
            "symbol": ["BTC", "ETH", "BTC", "SOL"],
        }
    )
    encoder = two_stage._Encoder()
    original_raw = encoder._raw
    calls = 0

    def counted_raw(value: pd.DataFrame) -> np.ndarray:
        nonlocal calls
        calls += 1
        return original_raw(value)

    encoder._raw = counted_raw  # type: ignore[method-assign]
    encoded = encoder.fit(frame, ["numeric", "symbol"])

    assert calls == 1
    assert encoded.shape == (4, 4)
    assert np.isfinite(encoded).all()
    assert np.allclose(encoded.mean(axis=0), 0.0, atol=1e-12)


def test_memory_efficient_ridge_matches_explicit_intercept_solution():
    rng = np.random.default_rng(20260824)
    x = rng.normal(size=(400, 12))
    y = rng.normal(size=400)
    penalty = 0.75
    design = np.column_stack([np.ones(len(x)), x])
    regularizer = np.eye(design.shape[1]) * penalty
    regularizer[0, 0] = 0.0
    expected = np.linalg.solve(
        design.T @ design + regularizer,
        design.T @ y,
    )

    actual = two_stage._fit_ridge(x, y, penalty)

    assert np.allclose(actual, expected, atol=1e-12, rtol=1e-12)
    assert np.allclose(two_stage._ridge_predict(x, actual), design @ expected)
