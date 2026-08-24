from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.training.pooled_panel import PooledPanelBuilder, dataset_manifest


def _panel(rows: int = 320) -> pd.DataFrame:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    output = []
    for index in range(rows):
        decision = start + timedelta(minutes=5 * index)
        for symbol, offset in (("BTCUSDT", 0.0), ("ETHUSDT", 0.0002)):
            output.append(
                {
                    "symbol": symbol,
                    "horizon_sec": 180,
                    "decision_at": decision,
                    "available_at": decision,
                    "label_available_at": decision + timedelta(seconds=180),
                    "liquidity": 1_000_000 + index,
                    "volatility": 0.01 + offset,
                    "session": "asia",
                    "regime": "normal",
                    "net_return": 0.001 if index % 3 else -0.0005,
                    "mae": 0.0008,
                    "mfe": 0.0015,
                }
            )
    return pd.DataFrame(output)


def test_pooled_panel_lockbox_and_walk_forward_are_disjoint_and_index_safe():
    dataset = PooledPanelBuilder(
        lockbox_fraction=0.15,
        minimum_train_rows=100,
        minimum_test_rows=20,
        maximum_folds=3,
    ).build_horizon(_panel(), 180)
    lockbox_start = pd.Timestamp(dataset.lockbox_start)
    assert dataset.development["symbol"].nunique() == 2
    assert dataset.lockbox["symbol"].nunique() == 2
    assert dataset.development["label_available_at"].max() < lockbox_start
    assert dataset.lockbox["decision_at"].min() >= lockbox_start
    assert len(dataset.development_fingerprint) == 64
    assert len(dataset.lockbox_fingerprint) == 64
    for fold in dataset.folds:
        assert max(fold.train_indices + fold.test_indices) < len(dataset.development)
        train = dataset.development.iloc[list(fold.train_indices)]
        test = dataset.development.iloc[list(fold.test_indices)]
        assert train["label_available_at"].max() < test["decision_at"].min()
        assert set(fold.train_indices).isdisjoint(fold.test_indices)


def test_panel_fingerprint_includes_feature_values_not_only_labels():
    original = _panel()
    original["candidate_factor"] = 0.0
    changed = original.copy()
    changed.loc[changed.index[-1], "candidate_factor"] = 1.0
    builder = PooledPanelBuilder(
        lockbox_fraction=0.15,
        minimum_train_rows=100,
        minimum_test_rows=20,
        maximum_folds=3,
    )
    first = builder.build_horizon(original, 180)
    second = builder.build_horizon(changed, 180)

    assert first.development_fingerprint == second.development_fingerprint
    assert first.lockbox_fingerprint != second.lockbox_fingerprint


def test_sealed_development_never_materializes_lockbox_labels():
    panel = _panel()
    last_pre_boundary_decision = (
        panel["decision_at"].drop_duplicates().sort_values().iloc[-40]
    )
    boundary = last_pre_boundary_decision + timedelta(minutes=2)
    # An early-exit label can be available before the boundary even though its
    # maximum holding window overlaps the lockbox. Fixed purge, not outcome
    # availability, must exclude it.
    panel.loc[
        panel["decision_at"] == last_pre_boundary_decision,
        "label_available_at",
    ] = last_pre_boundary_decision + timedelta(seconds=1)
    dataset = PooledPanelBuilder(
        minimum_train_rows=100,
        minimum_test_rows=20,
        maximum_folds=3,
    ).build_sealed_development(panel, 180, lockbox_start=boundary)
    manifest = dataset_manifest(dataset)

    assert dataset.lockbox.empty
    assert dataset.lockbox_fingerprint is None
    assert dataset.lockbox_labels_materialized is False
    assert dataset.development["label_available_at"].max() < boundary
    assert dataset.development["decision_at"].max() < boundary - timedelta(seconds=180)
    assert last_pre_boundary_decision not in set(dataset.development["decision_at"])
    assert manifest["lockbox_status"] == "SEALED_UNLABELED"
    assert manifest["lockbox_rows"] == 0


def test_regime_is_causal_and_future_rows_cannot_rewrite_past_labels():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    prefix = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 80,
            "decision_at": [start + timedelta(minutes=index) for index in range(80)],
            "volatility": np.linspace(0.001, 0.010, 80),
        }
    )
    future = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 40,
            "decision_at": [start + timedelta(minutes=80 + index) for index in range(40)],
            "volatility": np.linspace(0.10, 0.50, 40),
        }
    )
    before = PooledPanelBuilder.enrich_context(prefix)["regime"].tolist()
    after = PooledPanelBuilder.enrich_context(pd.concat([prefix, future], ignore_index=True))[
        "regime"
    ].iloc[: len(prefix)].tolist()
    assert before == after
    assert before[:8] == ["insufficient_history"] * 8
