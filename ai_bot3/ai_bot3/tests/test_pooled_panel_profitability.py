from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.training.pooled_panel import PooledPanelBuilder


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
    assert len(dataset.lockbox_fingerprint) == 64
    for fold in dataset.folds:
        assert max(fold.train_indices + fold.test_indices) < len(dataset.development)
        train = dataset.development.iloc[list(fold.train_indices)]
        test = dataset.development.iloc[list(fold.test_indices)]
        assert train["label_available_at"].max() < test["decision_at"].min()
        assert set(fold.train_indices).isdisjoint(fold.test_indices)
