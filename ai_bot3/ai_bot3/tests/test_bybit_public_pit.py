from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_public_pit import BybitPublicPITIngestor, BybitPublicPITStore


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


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
    point = store.features.snapshot(
        [
            "orderbook_spread_bps",
            "bybit_orderbook_delta_l5",
            "orderbook_depth_usdt_l5",
            "microprice_deviation_bps",
            "fill_probability",
            "expected_slippage_bps",
        ],
        received,
    )
    assert point.status == "ok"
    assert point.values["orderbook_depth_usdt_l5"].value > 0
    assert 0.8 < point.values["fill_probability"].value <= 1.0
    assert point.values["orderbook_spread_bps"].available_at == received

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
    assert ingestor.ingest(delta, received_at=delta_time + timedelta(milliseconds=300))["status"] == "duplicate"

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
            """SELECT event_time,available_at FROM raw_observations
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
    assert result["imbalance"] == 1 / 3

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

    point = store.features.snapshot(
        [
            "public_trade_imbalance_1m",
            "ofi_1m",
            "aggressive_cvd_1m",
            "liquidation_imbalance_5m",
            "perpetual_basis_bps",
            "funding_rate",
            "open_interest_change_1h",
        ],
        hour + timedelta(milliseconds=100),
        minimum_coverage=1.0,
    )
    assert point.values["open_interest_change_1h"].value == 0.1
    assert point.values["funding_rate"].value == 0.0002
