from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.features.profitability_technical import TECHNICAL_FEATURE_COLUMNS
from core.models.profitability_runtime import generate_profitability_alpha_prediction
from core.models.two_stage import TwoStageAlphaModel, TwoStageConfig


FEATURES = (
    "symbol",
    "horizon_sec",
    "side",
    "liquidity",
    "volatility",
    "session",
    "regime",
) + TECHNICAL_FEATURE_COLUMNS


def _training_frame() -> pd.DataFrame:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(120):
        decision_at = start + timedelta(days=index)
        signal = float(np.sin(index / 7.0))
        direction = "up" if signal > 0.1 else "down" if signal < -0.1 else "flat"
        for side in ("BUY", "SELL"):
            aligned = (side == "BUY" and direction == "up") or (
                side == "SELL" and direction == "down"
            )
            row = {
                "symbol": "BTCUSDT",
                "horizon_sec": 180,
                "side": side,
                "liquidity": 1_000_000.0 + index,
                "volatility": 0.002 + abs(signal) * 0.001,
                "session": "asia",
                "regime": "normal",
                "net_return": 0.002 if aligned else -0.001,
                "mae": 0.0008,
                "mfe": 0.0015,
                "direction_label": direction,
                "decision_at": decision_at,
                "label_available_at": decision_at + timedelta(hours=1),
            }
            row.update({name: signal * (position + 1) / 100 for position, name in enumerate(TECHNICAL_FEATURE_COLUMNS)})
            rows.append(row)
    return pd.DataFrame(rows)


def _market_frame() -> pd.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(100):
        price = 100.0 + 0.02 * index + np.sin(index / 8.0)
        rows.append(
            {
                "open": price,
                "high": price * 1.002,
                "low": price * 0.998,
                "close": price * 1.0002,
                "volume": 1_000.0 + index,
            }
        )
    return pd.DataFrame(
        rows,
        index=pd.date_range(start, periods=len(rows), freq="3min"),
    )


def _bundle(tmp_path: Path) -> Path:
    model = TwoStageAlphaModel(
        TwoStageConfig(
            direction_iterations=30,
            meta_iterations=30,
            minimum_expectancy_clusters=5,
        )
    ).fit(_training_frame(), FEATURES)
    model_path = tmp_path / "horizon_180.json"
    model.save(model_path)
    model_sha = hashlib.sha256(model_path.read_bytes()).hexdigest()
    bundle = tmp_path / "model_bundle.json"
    bundle.write_text(
        json.dumps(
            {
                "schema_version": "profitability-model-bundle.v2",
                "trial_id": "shadow_trial",
                "model_family": "profitability_two_stage",
                "release_stage": "rejected",
                "profitability_gate": "FAILED",
                "models": {"180": model_path.name},
                "model_sha256": {"180": model_sha},
                "formal_feature_columns": list(FEATURES),
                "retained_factor_groups": [],
                "lockbox_fingerprint": None,
                "lockbox_consumed": False,
                "code_commit": "1" * 40,
            }
        ),
        encoding="utf-8",
    )
    return bundle


def test_rejected_shadow_bundle_runs_real_alpha_but_cannot_be_actionable(tmp_path):
    bundle = _bundle(tmp_path)
    alpha = generate_profitability_alpha_prediction(
        _market_frame(),
        symbol="BTCUSDT",
        mode="scalping",
        model_bundle_path=bundle,
    )

    assert alpha["status"] == "ok"
    assert alpha["model_family"] == "profitability_two_stage"
    assert alpha["model_bundle_id"] == "shadow_trial"
    assert alpha["release_stage"] == "rejected"
    assert alpha["profitability_gate"] == "FAILED"
    assert alpha["actionable"] is False
    assert alpha["decision"] in {"TRADE", "NO_TRADE"}
    assert alpha["feature_evidence"]["feature_snapshot_sha256"]


def test_runtime_fails_closed_when_horizon_model_is_modified(tmp_path):
    bundle = _bundle(tmp_path)
    model_path = tmp_path / "horizon_180.json"
    model_path.write_text(model_path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    alpha = generate_profitability_alpha_prediction(
        _market_frame(),
        symbol="BTCUSDT",
        mode="scalping",
        model_bundle_path=bundle,
    )

    assert alpha["status"] == "blocked"
    assert alpha["release_stage"] == "rejected"
    assert alpha["actionable"] is False
    assert "hash mismatch" in alpha["reason"]
