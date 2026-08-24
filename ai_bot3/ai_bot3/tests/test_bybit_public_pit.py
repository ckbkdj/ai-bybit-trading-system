from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_public_pit import (
    BybitPublicPITCollector,
    BybitPublicPITIngestor,
    BybitPublicPITStore,
    CaptureConflict,
    StalePublicEvent,
)
from core.training.bybit_pit_panel import BybitPITFeatureSource
from scripts.rebuild_bybit_liquidation_semantics import main as rebuild_liquidations


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def test_new_capture_session_reconciles_unclean_previous_process(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    restarted = first + timedelta(minutes=5)
    store.start_session(
        "stale-session", endpoint="wss://public", symbols=["BTCUSDT"], started_at=first
    )
    store.start_session(
        "replacement-session",
        endpoint="wss://public",
        symbols=["BTCUSDT"],
        started_at=restarted,
    )

    with sqlite3.connect(path) as connection:
        stale = connection.execute(
            "SELECT status,ended_at,error FROM bybit_capture_sessions WHERE session_id=?",
            ("stale-session",),
        ).fetchone()
        replacement = connection.execute(
            "SELECT status FROM bybit_capture_sessions WHERE session_id=?",
            ("replacement-session",),
        ).fetchone()
    assert stale == (
        "disconnected",
        "2026-01-01T00:05:00Z",
        "collector_restarted_after_unclean_shutdown",
    )
    assert replacement == ("running",)


def test_new_capture_session_cannot_disconnect_a_live_collector(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.start_session(
        "live-session", endpoint="wss://public", symbols=["BTCUSDT"], started_at=first
    )

    with pytest.raises(CaptureConflict, match="active database lease"):
        store.start_session(
            "competing-session",
            endpoint="wss://public",
            symbols=["BTCUSDT"],
            started_at=first + timedelta(seconds=60),
        )

    with sqlite3.connect(path) as connection:
        sessions = connection.execute(
            "SELECT session_id,status FROM bybit_capture_sessions ORDER BY session_id"
        ).fetchall()
    assert sessions == [("live-session", "running")]


def test_liquidation_v1_invalidation_migration_has_durable_marker(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    with store.connect() as connection:
        marker = connection.execute(
            """SELECT migration_id FROM bybit_store_migrations
                 WHERE migration_id='invalidate-bybit-liquidation-side-v1'"""
        ).fetchone()
    assert marker[0] == "invalidate-bybit-liquidation-side-v1"
    store.close()

    # Reopening uses the marker instead of repeating the historical scan.
    reopened = BybitPublicPITStore(path)
    with reopened.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_store_migrations"
        ).fetchone()[0] == 1
    reopened.close()


def test_orderbook_snapshot_delta_and_disconnect_are_pit_and_fail_closed(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    ingestor = BybitPublicPITIngestor(store, session_id="session-1")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    received = event_time + timedelta(milliseconds=250)
    snapshot = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": _ms(event_time),
        "cts": _ms(event_time),
        "data": {
            "s": "BTCUSDT",
            "u": 100,
            "seq": 200,
            "b": [["99.9", "5"], ["99.8", "4"]],
            "a": [["100.1", "3"], ["100.2", "6"]],
        },
    }
    accepted = ingestor.ingest(snapshot, received_at=received)
    assert accepted["status"] == "accepted"
    point = store.latest_features(
        "BTCUSDT",
        [
            "orderbook_spread_bps",
            "bybit_orderbook_delta_l5",
            "ofi_1m",
            "orderbook_depth_usdt_l5",
            "microprice_deviation_bps",
            "fill_probability",
            "expected_slippage_bps",
        ],
        simulated_time=received,
    )
    assert set(point) == {
        "orderbook_spread_bps",
        "bybit_orderbook_delta_l5",
        "ofi_1m",
        "orderbook_depth_usdt_l5",
        "microprice_deviation_bps",
        "fill_probability",
        "expected_slippage_bps",
    }
    assert point["orderbook_depth_usdt_l5"]["value"] > 0
    assert point["ofi_1m"]["value"] == 0.0
    assert 0.8 < point["fill_probability"]["value"] <= 1.0
    assert point["orderbook_spread_bps"]["available_at"].endswith(".250000Z")
    assert store.latest_features(
        "ETHUSDT", ["orderbook_spread_bps"], simulated_time=received
    ) == {}

    delta_time = event_time + timedelta(seconds=1)
    delta = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": _ms(delta_time),
        "cts": _ms(delta_time),
        "data": {
            "s": "BTCUSDT",
            "u": 101,
            "seq": 201,
            "b": [["99.9", "7"]],
            "a": [["100.1", "2"]],
        },
    }
    assert ingestor.ingest(delta, received_at=delta_time + timedelta(milliseconds=250))["status"] == "accepted"
    assert ingestor.ingest(delta, received_at=delta_time + timedelta(milliseconds=300))["status"] == "duplicate_memory"
    ofi = store.latest_features(
        "BTCUSDT", ["ofi_1m"], simulated_time=delta_time + timedelta(milliseconds=250)
    )
    assert ofi["ofi_1m"]["value"] == 3.0

    ingestor.invalidate_books("test_disconnect", delta_time + timedelta(seconds=1))
    after_disconnect = dict(delta)
    after_disconnect["ts"] = _ms(delta_time + timedelta(seconds=2))
    after_disconnect["cts"] = after_disconnect["ts"]
    after_disconnect["data"] = dict(delta["data"], u=102, seq=202)
    result = ingestor.ingest(
        after_disconnect,
        received_at=delta_time + timedelta(seconds=2, milliseconds=250),
    )
    assert result["status"] == "waiting_for_snapshot"
    assert ingestor.books["BTCUSDT"].valid is False

    with sqlite3.connect(path) as connection:
        feature_time = connection.execute(
            """SELECT event_time,available_at FROM bybit_feature_observations
                 WHERE name='orderbook_spread_bps' ORDER BY sequence DESC LIMIT 1"""
        ).fetchone()
        parsed = [datetime.fromisoformat(value.replace("Z", "+00:00")) for value in feature_time]
        assert parsed[0] <= parsed[1]


def test_public_trades_liquidations_and_ticker_create_direct_observations(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    ingestor = BybitPublicPITIngestor(store, session_id="session-2")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    trades = {
        "topic": "publicTrade.BTCUSDT",
        "ts": _ms(event_time),
        "data": [
            {"T": _ms(event_time), "s": "BTCUSDT", "S": "Buy", "v": "2", "p": "100", "i": "a", "seq": 1},
            {"T": _ms(event_time), "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100", "i": "b", "seq": 1},
        ],
    }
    result = ingestor.ingest(trades, received_at=event_time + timedelta(milliseconds=100))
    assert result["accepted"] == 2
    assert result["features"]["public_trade_imbalance_1m"][0] == 1 / 3

    liquidations = {
        "topic": "allLiquidation.BTCUSDT",
        "ts": _ms(event_time + timedelta(seconds=1)),
        "data": [
            {"T": _ms(event_time + timedelta(seconds=1)), "s": "BTCUSDT", "S": "Buy", "v": "2", "p": "100"},
            {"T": _ms(event_time + timedelta(seconds=1)), "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100"},
        ],
    }
    result = ingestor.ingest(
        liquidations, received_at=event_time + timedelta(seconds=1, milliseconds=100)
    )
    assert result["imbalance"] == -1 / 3

    ticker_0 = {
        "topic": "tickers.BTCUSDT",
        "ts": _ms(event_time),
        "data": {
            "symbol": "BTCUSDT",
            "markPrice": "101",
            "indexPrice": "100",
            "fundingRate": "0.0001",
            "openInterest": "1000",
        },
    }
    ingestor.ingest(ticker_0, received_at=event_time + timedelta(milliseconds=100))
    hour = event_time + timedelta(hours=1)
    ticker_1 = {
        "topic": "tickers.BTCUSDT",
        "ts": _ms(hour),
        "data": {
            "symbol": "BTCUSDT",
            "markPrice": "102",
            "indexPrice": "100",
            "fundingRate": "0.0002",
            "openInterest": "1100",
        },
    }
    result = ingestor.ingest(ticker_1, received_at=hour + timedelta(milliseconds=100))
    assert result["features"]["open_interest_change_1h"][0] == 0.1

    point = store.latest_features(
        "BTCUSDT",
        [
            "public_trade_imbalance_1m",
            "aggressive_cvd_1m",
            "liquidation_imbalance_5m",
            "perpetual_basis_bps",
            "funding_rate",
            "open_interest_change_1h",
        ],
        simulated_time=hour + timedelta(milliseconds=100),
    )
    assert point["open_interest_change_1h"]["value"] == 0.1
    assert point["funding_rate"]["value"] == 0.0002
    assert point["liquidation_imbalance_5m"]["value"] == -1 / 3
    assert point["liquidation_imbalance_5m"]["source"] == "bybit.public.liquidations.v2"


def test_inverted_liquidation_v1_is_preserved_but_invalidated(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store = BybitPublicPITStore(path)
    assert store.append_feature(
        event_id="old-side-contract",
        symbol="BTCUSDT",
        name="liquidation_imbalance_5m",
        value=0.5,
        unit="ratio",
        event_time=event_time,
        received_at=event_time + timedelta(milliseconds=100),
        source="bybit.public.liquidations",
        quality=1.0,
    )
    store.close()

    migrated = BybitPublicPITStore(path)
    migrated.close()
    history, evidence = BybitPITFeatureSource(path).load(
        ["liquidation_imbalance_5m"]
    )
    assert history.empty
    assert evidence["invalidated_observation_count"] == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_invalidations"
        ).fetchone()[0] == 1


def test_liquidation_v2_rebuild_uses_retained_raw_events(tmp_path, monkeypatch):
    path = tmp_path / "bybit.sqlite3"
    report = tmp_path / "rebuild.json"
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    received_at = event_time + timedelta(milliseconds=100)
    payload = {
        "T": _ms(event_time),
        "s": "BTCUSDT",
        "S": "Buy",
        "v": "2",
        "p": "100",
    }
    store = BybitPublicPITStore(path)
    assert store.append_raw(
        event_id="liq:BTCUSDT:retained",
        session_id="legacy-session",
        topic="allLiquidation.BTCUSDT",
        symbol="BTCUSDT",
        event_type="liquidation",
        exchange_time=event_time,
        received_at=received_at,
        payload=payload,
    )
    assert store.append_feature(
        event_id="liq:BTCUSDT:retained",
        symbol="BTCUSDT",
        name="liquidation_imbalance_5m",
        value=1.0,
        unit="ratio",
        event_time=event_time,
        received_at=received_at,
        source="bybit.public.liquidations",
        quality=1.0,
    )
    store.close()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "rebuild_bybit_liquidation_semantics.py",
            "--database",
            str(path),
            "--report",
            str(report),
        ],
    )
    assert rebuild_liquidations() == 0
    output = json.loads(report.read_text(encoding="utf-8"))
    assert output["old_observations_deleted"] is False
    assert output["invalidated_v1_observations"] == 1
    assert output["v2_observations_inserted"] == 1

    history, evidence = BybitPITFeatureSource(path).load(
        ["liquidation_imbalance_5m"]
    )
    assert history["value"].tolist() == [-1.0]
    assert history["source"].tolist() == ["bybit.public.liquidations.v2"]
    assert evidence["invalidated_observation_count"] == 1


def test_collector_cadence_samples_raw_book_evidence_and_bounds_feature_amplification(
    tmp_path,
):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    ingestor = BybitPublicPITIngestor(
        store,
        session_id="cadence",
        feature_emit_interval_sec=5.0,
        raw_persist_interval_sec=5.0,
    )
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": _ms(event_time),
        "cts": _ms(event_time),
        "data": {
            "s": "BTCUSDT",
            "u": 1,
            "seq": 1,
            "b": [["99.9", "5"]],
            "a": [["100.1", "5"]],
        },
    }
    first = ingestor.ingest(
        snapshot, received_at=event_time + timedelta(milliseconds=100)
    )
    delta = {
        **snapshot,
        "type": "delta",
        "ts": _ms(event_time + timedelta(seconds=1)),
        "cts": _ms(event_time + timedelta(seconds=1)),
        "data": {
            **snapshot["data"],
            "u": 2,
            "seq": 2,
            "b": [["99.9", "6"]],
        },
    }
    second = ingestor.ingest(
        delta,
        received_at=event_time + timedelta(seconds=1, milliseconds=100),
    )

    assert first["features_emitted"] is True
    assert first["raw_persisted"] is True
    assert second["features_emitted"] is False
    assert second["raw_persisted"] is False
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_raw_public_events"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 8


def test_stale_public_stream_event_fails_closed_before_raw_or_feature_acceptance(
    tmp_path,
):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path)
    ingestor = BybitPublicPITIngestor(
        store,
        session_id="stale-stream",
        maximum_event_lag_sec=10.0,
    )
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    message = {
        "topic": "tickers.BTCUSDT",
        "ts": _ms(event_time),
        "data": {
            "symbol": "BTCUSDT",
            "markPrice": "101",
            "indexPrice": "100",
        },
    }

    with pytest.raises(StalePublicEvent, match="event lag"):
        ingestor.ingest(message, received_at=event_time + timedelta(seconds=11))
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_raw_public_events"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 0


def test_collector_batch_store_commits_explicitly_and_preserves_duplicate_identity(
    tmp_path,
):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(
        path,
        batch_writes=True,
        batch_max_operations=1_000,
        batch_max_interval_sec=60.0,
    )
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    arguments = {
        "event_id": "batched-book",
        "symbol": "BTCUSDT",
        "name": "orderbook_spread_bps",
        "value": 2.5,
        "unit": "bps",
        "event_time": observed_at,
        "received_at": observed_at + timedelta(milliseconds=100),
        "source": "bybit.public.orderbook",
        "quality": 0.98,
    }
    assert store.append_feature(**arguments) is True
    assert store.append_feature(**arguments) is False
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 0

    store.flush()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 1
    store.close()


def test_batched_snapshot_quality_recovery_does_not_deadlock_its_writer(tmp_path):
    path = tmp_path / "bybit.sqlite3"
    store = BybitPublicPITStore(path, batch_writes=True)
    ingestor = BybitPublicPITIngestor(store, session_id="batch-snapshot")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": _ms(event_time),
        "cts": _ms(event_time),
        "data": {
            "s": "BTCUSDT",
            "u": 1,
            "seq": 1,
            "b": [["99.9", "5"]],
            "a": [["100.1", "5"]],
        },
    }

    assert ingestor.ingest(
        snapshot, received_at=event_time + timedelta(milliseconds=100)
    )["status"] == "accepted"
    store.flush()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_raw_public_events"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0] == 8
    store.close()


def test_collector_retry_failure_is_visible_in_operational_logs(
    tmp_path, monkeypatch, caplog
):
    store = BybitPublicPITStore(tmp_path / "bybit.sqlite3", batch_writes=True)
    collector = BybitPublicPITCollector(store, ["BTCUSDT"])

    async def fail_before_session():
        raise sqlite3.OperationalError("database is locked")

    async def stop_after_first_retry(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(collector, "run_once", fail_before_session)
    monkeypatch.setattr(asyncio, "sleep", stop_after_first_retry)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(collector.run_forever())
    assert "Bybit public collector retry after OperationalError" in caplog.text
    assert "database is locked" in caplog.text
    store.close()
