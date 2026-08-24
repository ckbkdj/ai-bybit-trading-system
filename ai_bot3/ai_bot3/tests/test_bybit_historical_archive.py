from __future__ import annotations

import csv
import gzip
import json
import sqlite3
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.providers.bybit_historical_archive import (
    archive_already_completed,
    download_official_archive,
    orderbook_archive_url,
    record_archive_failure,
    replay_orderbook_archive,
    replay_trade_archive,
    trade_archive_url,
)
from core.providers.bybit_public_pit import BybitPublicPITStore
from core.training.bybit_pit_panel import BybitPITFeatureSource


DAY = date(2026, 1, 1)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
FETCHED = datetime(2026, 1, 2, 1, tzinfo=timezone.utc)
SYMBOL = "BTCUSDT"


def _book_message(offset_sec: int, sequence: int, *, snapshot: bool) -> dict:
    timestamp = int((START + timedelta(seconds=offset_sec)).timestamp() * 1000)
    return {
        "topic": f"orderbook.200.{SYMBOL}",
        "type": "snapshot" if snapshot else "delta",
        "ts": timestamp + 10,
        "cts": timestamp,
        "data": {
            "s": SYMBOL,
            "b": (
                [["99.9", "8"], ["99.8", "4"], ["99.7", "3"], ["99.6", "2"], ["99.5", "1"]]
                if snapshot
                else [["99.9", str(8 + sequence)]]
            ),
            "a": (
                [["100.1", "2"], ["100.2", "3"], ["100.3", "4"], ["100.4", "5"], ["100.5", "6"]]
                if snapshot
                else [["100.1", str(2 + sequence)]]
            ),
            "u": sequence,
            "seq": sequence,
        },
    }


def _orderbook_zip(path: Path) -> None:
    member = f"{DAY.isoformat()}_{SYMBOL}_ob200.data"
    messages = (
        _book_message(1, 1, snapshot=True),
        _book_message(16, 2, snapshot=False),
        _book_message(31, 3, snapshot=False),
        _book_message(86_402, 4, snapshot=True),
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, "\n".join(json.dumps(item) for item in messages))


def _trade_gzip(path: Path) -> None:
    fields = (
        "timestamp",
        "symbol",
        "side",
        "size",
        "price",
        "tickDirection",
        "trdMatchID",
    )
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for index, offset in enumerate((1, 6, 16, 21, 31, 36)):
            writer.writerow(
                {
                    "timestamp": (START + timedelta(seconds=offset)).timestamp(),
                    "symbol": SYMBOL,
                    "side": "Buy" if index % 3 else "Sell",
                    "size": 1 + index,
                    "price": 100 + index / 10,
                    "tickDirection": "PlusTick",
                    "trdMatchID": f"trade-{index}",
                }
            )


def test_official_archive_replay_preserves_event_available_ingested_chronology(tmp_path):
    database = tmp_path / "pit.sqlite3"
    archive = tmp_path / "book.zip"
    _orderbook_zip(archive)
    store = BybitPublicPITStore(database, batch_writes=True, batch_max_operations=8)
    evidence = replay_orderbook_archive(
        store,
        archive,
        symbol=SYMBOL,
        trading_date=DAY,
        source_url=orderbook_archive_url(SYMBOL, DAY),
        fetched_at=FETCHED,
        feature_emit_interval_sec=15,
        assumed_feed_latency_ms=1_000,
    )
    store.close()

    assert evidence.status == "completed"
    assert evidence.rows_read == 4
    assert evidence.feature_observation_count == 24
    assert archive_already_completed(
        BybitPublicPITStore(database),
        data_kind="orderbook",
        symbol=SYMBOL,
        trading_date=DAY,
    )
    history, source_evidence = BybitPITFeatureSource(database).load(
        ["orderbook_spread_bps", "ofi_1m", "orderbook_depth_usdt_l5"]
    )
    assert len(history) == 9
    assert (history["event_time"] < history["available_at"]).all()
    assert (history["available_at"] < history["ingested_at"]).all()
    assert history["ingested_at"].nunique() == 1
    assert source_evidence["rejected_source_contract_count"] == 0
    assert source_evidence["provenance_observation_counts"] == {
        "historical_archive_replay": 9
    }
    assert source_evidence["historical_archive_file_count"] == 1
    assert source_evidence["historical_archive_files"][0]["content_sha256"] == evidence.content_sha256


def test_official_trade_replay_builds_real_rolling_cvd_with_archive_provenance(tmp_path):
    database = tmp_path / "pit.sqlite3"
    archive = tmp_path / "trades.csv.gz"
    _trade_gzip(archive)
    store = BybitPublicPITStore(database, batch_writes=True, batch_max_operations=4)
    evidence = replay_trade_archive(
        store,
        archive,
        symbol=SYMBOL,
        trading_date=DAY,
        source_url=trade_archive_url(SYMBOL, DAY),
        fetched_at=FETCHED,
        feature_emit_interval_sec=15,
        assumed_feed_latency_ms=1_000,
    )
    store.close()

    assert evidence.rows_read == 6
    assert evidence.feature_observation_count == 6
    history, _ = BybitPITFeatureSource(database).load(
        ["public_trade_imbalance_1m", "aggressive_cvd_1m"]
    )
    assert len(history) == 6
    assert history["source"].unique().tolist() == ["bybit.public.trades"]
    assert history["value"].abs().sum() > 0


def test_archive_downloader_rejects_non_allowlisted_hosts_before_network(tmp_path):
    try:
        download_official_archive(
            "https://example.com/untrusted.zip", tmp_path / "untrusted.zip"
        )
    except ValueError as exc:
        assert "allow-list" in str(exc)
    else:
        raise AssertionError("untrusted archive host was accepted")


def test_backfill_store_can_use_a_bounded_extended_sqlite_busy_timeout(tmp_path):
    store = BybitPublicPITStore(tmp_path / "pit.sqlite3", busy_timeout_sec=45.0)
    with store.connect() as connection:
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 45_000


def test_invalid_archive_is_rejected_before_any_feature_rows_are_committed(tmp_path):
    database = tmp_path / "pit.sqlite3"
    archive = tmp_path / "invalid-book.zip"
    member = f"{DAY.isoformat()}_{SYMBOL}_ob200.data"
    messages = (
        _book_message(1, 1, snapshot=True),
        _book_message(16, 2, snapshot=False),
        _book_message(86_411, 3, snapshot=False),
    )
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        output.writestr(member, "\n".join(json.dumps(item) for item in messages))
    store = BybitPublicPITStore(database)
    try:
        replay_orderbook_archive(
            store,
            archive,
            symbol=SYMBOL,
            trading_date=DAY,
            source_url=orderbook_archive_url(SYMBOL, DAY),
            fetched_at=FETCHED,
            feature_emit_interval_sec=15,
        )
    except ValueError as exc:
        assert "outside its UTC trading date" in str(exc)
    else:
        raise AssertionError("invalid archive boundary was accepted")
    with store.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM bybit_feature_observations"
        ).fetchone()[0]
    assert count == 0


def test_completed_archive_evidence_cannot_be_downgraded_by_late_failure(tmp_path):
    database = tmp_path / "pit.sqlite3"
    archive = tmp_path / "book.zip"
    _orderbook_zip(archive)
    store = BybitPublicPITStore(database)
    completed = replay_orderbook_archive(
        store,
        archive,
        symbol=SYMBOL,
        trading_date=DAY,
        source_url=orderbook_archive_url(SYMBOL, DAY),
        fetched_at=FETCHED,
        feature_emit_interval_sec=15,
    )

    record_archive_failure(
        store,
        data_kind="orderbook",
        symbol=SYMBOL,
        trading_date=DAY,
        source_url=completed.source_url,
        fetched_at=FETCHED + timedelta(seconds=1),
        error="a concurrent retry failed after the completed commit",
    )

    with store.connect() as connection:
        row = connection.execute(
            """SELECT archive_id,status,error,content_sha256,rows_read,
                      feature_observation_count
                 FROM bybit_historical_archive_files
                WHERE data_kind='orderbook' AND symbol=? AND trading_date=?""",
            (SYMBOL, DAY.isoformat()),
        ).fetchone()
    assert dict(row) == {
        "archive_id": completed.archive_id,
        "status": "completed",
        "error": None,
        "content_sha256": completed.content_sha256,
        "rows_read": completed.rows_read,
        "feature_observation_count": completed.feature_observation_count,
    }
    repeated = replay_orderbook_archive(
        store,
        archive,
        symbol=SYMBOL,
        trading_date=DAY,
        source_url=completed.source_url,
        fetched_at=FETCHED,
        feature_emit_interval_sec=15,
    )
    assert repeated.feature_observation_count == 0
    try:
        with store.connect() as connection:
            connection.execute(
                "UPDATE bybit_historical_archive_files SET status='failed'"
            )
    except sqlite3.IntegrityError as exc:
        assert "immutable" in str(exc)
    else:
        raise AssertionError("completed archive evidence accepted an update")
