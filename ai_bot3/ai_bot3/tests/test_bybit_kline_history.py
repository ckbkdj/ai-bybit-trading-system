from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_kline_history import (
    BybitHTTPReceipt,
    BybitKlineHistoryStore,
    instrument_url,
    kline_url,
    verify_bybit_listing_evidence,
)
from core.evaluation.profitability_rebuild import (
    KlinePanelSource,
    _market_bars,
    audit_source_coverage,
)


def _database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """CREATE TABLE raw_kline(
                   symbol TEXT NOT NULL,timeframe TEXT NOT NULL,source TEXT NOT NULL,
                   open_time INTEGER NOT NULL,close_time INTEGER NOT NULL,
                   open REAL NOT NULL,high REAL NOT NULL,low REAL NOT NULL,
                   close REAL NOT NULL,volume REAL NOT NULL,fetched_at TEXT NOT NULL,
                   PRIMARY KEY(symbol,timeframe,source,open_time)
               )"""
        )
        connection.commit()


def _receipt(url: str, payload: object, offset: int = 0) -> BybitHTTPReceipt:
    requested = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset
    )
    return BybitHTTPReceipt(
        request_url=url,
        requested_at=requested,
        received_at=requested + timedelta(milliseconds=150),
        http_status=200,
        body=json.dumps(payload, sort_keys=True).encode(),
    )


def _instrument_payload(launch_ms: int, *, marker: str) -> dict[str, object]:
    return {
        "retCode": 0,
        "result": {
            "category": "linear",
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "LinearPerpetual",
                    "quoteCoin": "USDT",
                    "status": "Trading",
                    "launchTime": str(launch_ms),
                    "priceScale": marker,
                }
            ],
        },
    }


def _kline_payload(rows: list[list[str]], *, symbol: str = "BTCUSDT") -> dict[str, object]:
    return {
        "retCode": 0,
        "result": {"category": "linear", "symbol": symbol, "list": rows},
    }


def _row(open_ms: int, price: float) -> list[str]:
    return [
        str(open_ms),
        str(price),
        str(price + 2),
        str(price - 1),
        str(price + 1),
        "10",
        "1000",
    ]


def test_official_receipts_import_a_complete_immutable_bybit_grid(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    store = BybitKlineHistoryStore(database)
    interval_ms = 180_000
    start_ms = 1_767_225_600_000
    end_ms = start_ms + 4 * interval_ms

    first_instrument = _receipt(
        instrument_url("BTCUSDT"),
        _instrument_payload(start_ms, marker="2"),
    )
    first_receipt_id = store.record_instrument("BTCUSDT", first_instrument)
    updated_instrument = _receipt(
        instrument_url("BTCUSDT"),
        _instrument_payload(start_ms, marker="3"),
        offset=1,
    )
    updated_receipt_id = store.record_instrument("BTCUSDT", updated_instrument)
    assert updated_receipt_id != first_receipt_id

    first_body = _kline_payload(
        [_row(start_ms + interval_ms, 101), _row(start_ms, 100)]
    )
    second_body = _kline_payload(
        [_row(start_ms + 3 * interval_ms, 103), _row(start_ms + 2 * interval_ms, 102)]
    )
    responses = [
        _receipt(
            kline_url(
                "BTCUSDT",
                "3m",
                start_ms=start_ms,
                end_ms=start_ms + 2 * interval_ms,
                limit=2,
            ),
            first_body,
            offset=2,
        ),
        _receipt(
            kline_url(
                "BTCUSDT",
                "3m",
                start_ms=start_ms + 2 * interval_ms,
                end_ms=end_ms,
                limit=2,
            ),
            second_body,
            offset=3,
        ),
    ]
    imported = store.import_window(
        symbol="BTCUSDT",
        timeframe="3m",
        window_start_ms=start_ms,
        window_end_ms=end_ms,
        instrument_receipt_id=first_receipt_id,
        responses=responses,
    )

    assert imported.row_count == 4
    assert imported.response_count == 2
    assert store.completed("BTCUSDT", "3m", start_ms, end_ms) is True
    evidence = verify_bybit_listing_evidence(database, "BTCUSDT", "3m")
    assert evidence is not None
    assert evidence["status"] == "VERIFIED_SINCE_LAUNCH"
    assert evidence["raw_receipt_reverified"] is True
    assert evidence["immutable_trigger_count"] == 8
    source = KlinePanelSource(database, source="bybit")
    frame = source.load("BTCUSDT", "3m", 10)
    coverage = audit_source_coverage(
        frame,
        "3m",
        listing_evidence=source.listing_evidence("BTCUSDT", "3m"),
    )
    assert frame["source"].unique().tolist() == ["bybit"]
    bars = _market_bars(frame)
    assert all(bar.price_source == "bybit_last_trade_kline" for bar in bars)
    assert all(bar.price_observed for bar in bars)
    assert coverage["status"] == "PASSED"
    assert coverage["coverage_policy"] == "fixed_floor_or_verified_since_launch"
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        raw = connection.execute(
            "SELECT * FROM raw_kline ORDER BY open_time"
        ).fetchall()
        receipts = connection.execute(
            "SELECT * FROM bybit_kline_api_responses ORDER BY window_start_ms"
        ).fetchall()
        instruments = connection.execute(
            "SELECT COUNT(*) FROM bybit_kline_instrument_receipts"
        ).fetchone()[0]
        assert instruments == 2
        assert len(raw) == 4
        assert {row["source"] for row in raw} == {"bybit"}
        assert all(row["close_time"] - row["open_time"] == interval_ms for row in raw)
        assert len(receipts) == 2
        assert receipts[0]["content_blob"] == json.dumps(
            first_body, sort_keys=True
        ).encode()
        assert receipts[0]["content_sha256"] == hashlib.sha256(
            receipts[0]["content_blob"]
        ).hexdigest()
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE raw_kline SET close=999 WHERE source='bybit'"
            )


def test_response_rows_must_belong_to_their_exact_request(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    store = BybitKlineHistoryStore(database)
    interval_ms = 180_000
    start_ms = 1_767_225_600_000
    end_ms = start_ms + 2 * interval_ms
    receipt_id = store.record_instrument(
        "BTCUSDT",
        _receipt(
            instrument_url("BTCUSDT"),
            _instrument_payload(start_ms, marker="2"),
        ),
    )
    response = _receipt(
        kline_url(
            "BTCUSDT",
            "3m",
            start_ms=start_ms,
            end_ms=start_ms + interval_ms,
            limit=1,
        ),
        _kline_payload([_row(start_ms + interval_ms, 101)]),
        offset=1,
    )

    with pytest.raises(ValueError, match="outside its request"):
        store.import_window(
            symbol="BTCUSDT",
            timeframe="3m",
            window_start_ms=start_ms,
            window_end_ms=end_ms,
            instrument_receipt_id=receipt_id,
            responses=[response],
        )

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM raw_kline").fetchone()[0] == 0


def test_request_receipts_must_contiguously_partition_the_import(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    store = BybitKlineHistoryStore(database)
    interval_ms = 180_000
    start_ms = 1_767_225_600_000
    end_ms = start_ms + 3 * interval_ms
    receipt_id = store.record_instrument(
        "BTCUSDT",
        _receipt(
            instrument_url("BTCUSDT"),
            _instrument_payload(start_ms, marker="2"),
        ),
    )
    responses = [
        _receipt(
            kline_url(
                "BTCUSDT",
                "3m",
                start_ms=start_ms,
                end_ms=start_ms + interval_ms,
                limit=1,
            ),
            _kline_payload([_row(start_ms, 100)]),
            offset=1,
        ),
        _receipt(
            kline_url(
                "BTCUSDT",
                "3m",
                start_ms=start_ms + 2 * interval_ms,
                end_ms=end_ms,
                limit=1,
            ),
            _kline_payload([_row(start_ms + 2 * interval_ms, 102)]),
            offset=2,
        ),
    ]

    with pytest.raises(ValueError, match="do not partition"):
        store.import_window(
            symbol="BTCUSDT",
            timeframe="3m",
            window_start_ms=start_ms,
            window_end_ms=end_ms,
            instrument_receipt_id=receipt_id,
            responses=responses,
        )


def test_response_identity_is_fail_closed(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    store = BybitKlineHistoryStore(database)
    interval_ms = 180_000
    start_ms = 1_767_225_600_000
    receipt_id = store.record_instrument(
        "BTCUSDT",
        _receipt(
            instrument_url("BTCUSDT"),
            _instrument_payload(start_ms, marker="2"),
        ),
    )
    response = _receipt(
        kline_url(
            "BTCUSDT",
            "3m",
            start_ms=start_ms,
            end_ms=start_ms + interval_ms,
            limit=1,
        ),
        _kline_payload([_row(start_ms, 100)], symbol="ETHUSDT"),
        offset=1,
    )

    with pytest.raises(ValueError, match="identity"):
        store.import_window(
            symbol="BTCUSDT",
            timeframe="3m",
            window_start_ms=start_ms,
            window_end_ms=start_ms + interval_ms,
            instrument_receipt_id=receipt_id,
            responses=[response],
        )
