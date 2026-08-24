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


def test_settled_derivatives_and_forward_only_liquidations_are_separate_ablations():
    assert SHORT_FACTOR_GROUPS["basis_funding_oi"] == (
        "perpetual_basis_bps",
        "funding_rate",
        "open_interest_change_1h",
    )
    assert SHORT_FACTOR_GROUPS["liquidations"] == (
        "liquidation_imbalance_5m",
    )


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
    assert all(
        item["minimum_required_history_days"] == 180.0
        for item in report.values()
    )
    liquidation_evidence = report["liquidations"]["collection_evidence"]
    assert liquidation_evidence["data_mode"] == "forward_only_public_websocket"
    assert liquidation_evidence["source_topic"] == "allLiquidation.{symbol}"
    assert liquidation_evidence["historical_backfill_supported"] is False
    assert liquidation_evidence["official_rest_history_endpoint"] is None


def test_ofi_source_contract_keeps_legacy_trade_semantics_for_audit_only(tmp_path):
    database = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(database)
    ingestor = BybitPublicPITIngestor(store, session_id="source-contract")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    received = event_time + timedelta(milliseconds=250)
    ingestor.ingest(_snapshot("BTCUSDT", event_time, 8), received_at=received)
    store.append_feature(
        event_id="legacy-trade-ofi",
        symbol="BTCUSDT",
        name="ofi_1m",
        value=999.0,
        unit="base_asset",
        event_time=event_time + timedelta(seconds=1),
        received_at=received + timedelta(seconds=1),
        source="bybit.public.trades",
        quality=0.98,
    )

    source = BybitPITFeatureSource(database)
    history, evidence = source.load(["ofi_1m"])
    assert history["source"].unique().tolist() == ["bybit.public.orderbook"]
    assert evidence["rejected_source_contract_count"] == 1
    latest, _ = source.latest(
        "BTCUSDT", ["ofi_1m"], decision_at=received + timedelta(seconds=2)
    )
    assert latest["ofi_1m"] == 0.0


def test_bybit_training_snapshot_freezes_observation_and_invalidation_journals(
    tmp_path,
):
    database = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(database)
    ingestor = BybitPublicPITIngestor(store, session_id="frozen-sequence")
    first_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ingestor.ingest(
        _snapshot("BTCUSDT", first_time, 8),
        received_at=first_time + timedelta(milliseconds=250),
    )
    source = BybitPITFeatureSource(database)
    frozen_sequence, frozen_invalidation_rowid = source.snapshot_watermarks()
    with store.connect() as connection:
        first_observation_id = connection.execute(
            """SELECT observation_id FROM bybit_feature_observations
                 WHERE name='orderbook_depth_usdt_l5' ORDER BY sequence LIMIT 1"""
        ).fetchone()[0]
    second_time = first_time + timedelta(minutes=1)
    ingestor.ingest(
        _snapshot("BTCUSDT", second_time, 12),
        received_at=second_time + timedelta(milliseconds=250),
    )
    with store.connect() as connection:
        connection.execute(
            """INSERT INTO bybit_feature_invalidations(
                   observation_id,invalidated_at,reason,correction_version
               ) VALUES (?,?,?,?)""",
            (
                first_observation_id,
                second_time.isoformat(),
                "test correction after experiment snapshot",
                "test-v2",
            ),
        )
        connection.commit()

    frozen, evidence = source.load(
        ["orderbook_depth_usdt_l5"],
        maximum_sequence=frozen_sequence,
        maximum_invalidation_rowid=frozen_invalidation_rowid,
    )
    current, current_evidence = source.load(["orderbook_depth_usdt_l5"])
    assert len(frozen) == 1
    assert len(current) == 1
    assert frozen.iloc[0]["value"] != current.iloc[0]["value"]
    assert evidence["snapshot_maximum_sequence"] == frozen_sequence
    assert (
        evidence["snapshot_maximum_invalidation_rowid"]
        == frozen_invalidation_rowid
    )
    assert (
        current_evidence["snapshot_maximum_invalidation_rowid"]
        > frozen_invalidation_rowid
    )
    assert len(evidence["snapshot_sha256"]) == 64


def test_bybit_training_snapshot_is_bounded_to_decision_window(tmp_path):
    database = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(database)
    ingestor = BybitPublicPITIngestor(store, session_id="bounded-window")
    old_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    current_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ingestor.ingest(
        _snapshot("BTCUSDT", old_time, 8),
        received_at=old_time + timedelta(milliseconds=250),
    )
    ingestor.ingest(
        _snapshot("BTCUSDT", current_time, 12),
        received_at=current_time + timedelta(milliseconds=250),
    )
    decision_at = current_time + timedelta(seconds=20)

    history, evidence = BybitPITFeatureSource(database).load(
        ["orderbook_depth_usdt_l5"],
        minimum_decision_at=decision_at,
        maximum_decision_at=decision_at,
        symbols=("BTCUSDT",),
    )

    assert len(history) == 1
    assert history.iloc[0]["event_time"] == pd.Timestamp(current_time)
    assert evidence["minimum_decision_at"] == decision_at.isoformat().replace(
        "+00:00", "Z"
    )
    assert evidence["maximum_decision_at"] == evidence["minimum_decision_at"]
    assert evidence["requested_symbols"] == ["BTCUSDT"]
    assert evidence["effective_available_at_minimum"] < evidence["minimum_decision_at"]


def test_bybit_loader_accepts_mixed_iso_precision_without_coercing_valid_time(tmp_path):
    database = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(database)
    exact = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index, received_at in enumerate(
        (exact, exact + timedelta(seconds=1, microseconds=250_000))
    ):
        store.append_feature(
            event_id=f"mixed-time-{index}",
            symbol="BTCUSDT",
            name="orderbook_spread_bps",
            value=2.0 + index,
            unit="bps",
            event_time=received_at - timedelta(milliseconds=100),
            received_at=received_at,
            source="bybit.public.orderbook",
            quality=0.98,
        )

    history, evidence = BybitPITFeatureSource(database).load(
        ["orderbook_spread_bps"]
    )
    assert len(history) == 2
    assert evidence["observation_count"] == 2
    assert history[["event_time", "available_at", "ingested_at"]].notna().all().all()
