from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.models.two_stage import TwoStageAlphaModel, TwoStageConfig


def _training_frame(rows: int = 160) -> pd.DataFrame:
    x = np.linspace(-2.0, 2.0, rows)
    net = 0.002 * x + 0.0002 * np.sin(np.arange(rows))
    return pd.DataFrame(
        {
            "signal": x,
            "symbol": np.where(np.arange(rows) % 2, "BTCUSDT", "ETHUSDT"),
            "net_return": net,
            "mae": 0.0005 + 0.0002 * np.abs(x),
            "mfe": 0.001 + 0.001 * np.maximum(x, 0),
            "direction_label": np.where(net > 0.0003, "up", np.where(net < -0.0003, "down", "flat")),
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

    artifact = tmp_path / "two-stage.json"
    model.save(artifact)
    loaded = TwoStageAlphaModel.load(artifact)
    replay = loaded.predict(frame.tail(8))
    assert np.allclose(
        [item.expected_net_return for item in predictions],
        [item.expected_net_return for item in replay],
    )
    assert all(item.release_stage == "rejected" for item in replay)
