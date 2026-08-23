from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_public_pit import BybitPublicPITIngestor, BybitPublicPITStore
from core.evaluation.profitability_rebuild import (
    SHORT_FACTOR_GROUPS,
    _evaluate_bybit_pit_ablation,
)
from core.training.bybit_pit_panel import BybitPITFeatureSource


def _snapshot(symbol: str, event_time: datetime, bid_size: int) -> dict:
    timestamp = int(event_time.timestamp() * 1000)
    return {
        "topic": f"orderbook.50.{symbol}",
        "type": "snapshot",
        "ts": timestamp,
        "cts": timestamp,
        "data": {
            "s": symbol,
            "u": 1,
            "seq": 1,
            "b": [["99.9", str(bid_size)]],
            "a": [["100.1", "2"]],
        },
    }


def test_symbol_partitioned_bybit_history_joins_only_fresh_available_values(tmp_path):
    database = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(database)
    ingestor = BybitPublicPITIngestor(store, session_id="panel-session")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    received = event_time + timedelta(milliseconds=250)
    ingestor.ingest(_snapshot("BTCUSDT", event_time, 8), received_at=received)
    ingestor.ingest(_snapshot("ETHUSDT", event_time, 3), received_at=received)

    source = BybitPITFeatureSource(database)
    history, evidence = source.load(
        ["orderbook_depth_usdt_l5", "orderbook_spread_bps"]
    )
    assert evidence["symbol_count"] == 2
    assert evidence["observation_count"] == 4
    decisions = pd.DataFrame(
        {
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT"],
            "decision_at": [
                received + timedelta(seconds=20),
                received + timedelta(seconds=20),
                received + timedelta(seconds=31),
            ],
        }
    )
    joined = source.join(
        decisions,
        names=["orderbook_depth_usdt_l5", "orderbook_spread_bps"],
        history=history,
    )
    assert joined.loc[0, "orderbook_depth_usdt_l5"] > joined.loc[1, "orderbook_depth_usdt_l5"]
    assert pd.isna(joined.loc[2, "orderbook_depth_usdt_l5"])
    assert joined.loc[0, "orderbook_spread_bps__available_at"] <= joined.loc[0, "decision_at"]
    latest, latest_evidence = source.latest(
        "BTCUSDT",
        ["orderbook_spread_bps"],
        decision_at=received + timedelta(seconds=20),
    )
    assert latest["orderbook_spread_bps"] > 0
    assert latest_evidence["symbol"] == "BTCUSDT"
    stale, _ = source.latest(
        "BTCUSDT",
        ["orderbook_spread_bps"],
        decision_at=received + timedelta(seconds=31),
    )
    assert stale == {}

    all_names = tuple(
        dict.fromkeys(name for columns in SHORT_FACTOR_GROUPS.values() for name in columns)
    )
    _, collecting_evidence = source.load(all_names)
    report = _evaluate_bybit_pit_ablation(
        {}, {}, None, None, collecting_evidence  # type: ignore[arg-type]
    )
    assert set(report) == set(SHORT_FACTOR_GROUPS)
    assert all(
        item["oos_ablation_status"] == "COLLECTING_INSUFFICIENT_PIT_HISTORY"
        for item in report.values()
    )
