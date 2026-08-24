from __future__ import annotations

import csv
import hashlib
import io
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.evaluation.profitability_rebuild import KlinePanelSource
from core.providers.binance_kline_archive import BinanceKlineArchiveStore


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


def _archive() -> tuple[bytes, bytes]:
    first = int(datetime(2023, 5, 1, tzinfo=timezone.utc).timestamp() * 1000)
    rows = []
    for index in range(2):
        opened = first + index * 86_400_000
        rows.append(
            [
                opened,
                "100.0",
                "102.0",
                "99.0",
                "101.0",
                "10.0",
                opened + 86_400_000 - 1,
                "0",
                "0",
                "0",
                "0",
                "0",
            ]
        )
    csv_body = io.StringIO()
    csv.writer(csv_body, lineterminator="\n").writerows(rows)
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("1000PEPEUSDT-1d-2023-05.csv", csv_body.getvalue())
    archive_body = archive_buffer.getvalue()
    checksum_body = (
        hashlib.sha256(archive_body).hexdigest()
        + "  1000PEPEUSDT-1d-2023-05.zip\n"
    ).encode()
    return archive_body, checksum_body


def test_checksum_verified_month_and_adjacent_404_create_listing_evidence(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    archive_body, checksum_body = _archive()
    archive_path = tmp_path / "1000PEPEUSDT-1d-2023-05.zip"
    checksum_path = tmp_path / "1000PEPEUSDT-1d-2023-05.zip.CHECKSUM"
    archive_path.write_bytes(archive_body)
    checksum_path.write_bytes(checksum_body)
    store = BinanceKlineArchiveStore(database)

    imported = store.import_month(
        symbol="1000PEPEUSDT",
        timeframe="1d",
        year_month="2023-05",
        archive_body=archive_body,
        checksum_body=checksum_body,
        archive_path=archive_path,
        checksum_path=checksum_path,
    )
    store.record_not_found(
        symbol="1000PEPEUSDT",
        timeframe="1d",
        year_month="2023-04",
        archive_http_status=404,
        checksum_http_status=404,
    )
    store.finalize_listing_evidence(
        symbol="1000PEPEUSDT",
        timeframe="1d",
        first_archive_year_month="2023-05",
        prior_month_year_month="2023-04",
    )

    assert imported.row_count == 2
    assert store.completed("1000PEPEUSDT", "1d", "2023-05") is True
    evidence = KlinePanelSource(database).listing_evidence(
        "1000PEPEUSDT", "1d"
    )
    assert evidence is not None
    assert evidence["status"] == "VERIFIED_SINCE_LISTING"
    assert evidence["prior_month_http_status"] == 404
    assert evidence["first_archive_checksum_verified"] == 1
    assert evidence["raw_receipt_reverified"] is True
    assert evidence["raw_receipt_reverification_failures"] == []

    archive_path.write_bytes(archive_body + b"tampered-after-import")
    tampered = KlinePanelSource(database).listing_evidence(
        "1000PEPEUSDT", "1d"
    )
    assert tampered is not None
    assert tampered["raw_receipt_reverified"] is False
    assert tampered["raw_receipt_reverification_failures"] == [
        "retained_archive_sha256_mismatch"
    ]


def test_archive_checksum_mismatch_is_rejected_before_database_write(tmp_path):
    database = tmp_path / "klines.sqlite3"
    _database(database)
    archive_body, checksum_body = _archive()
    archive_path = tmp_path / "archive.zip"
    checksum_path = tmp_path / "archive.zip.CHECKSUM"
    archive_path.write_bytes(archive_body)
    checksum_path.write_bytes(checksum_body)
    store = BinanceKlineArchiveStore(database)

    try:
        store.import_month(
            symbol="1000PEPEUSDT",
            timeframe="1d",
            year_month="2023-05",
            archive_body=archive_body + b"tampered",
            checksum_body=checksum_body,
            archive_path=archive_path,
            checksum_path=checksum_path,
        )
    except ValueError as exc:
        assert "SHA256" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("tampered archive was accepted")

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM raw_kline").fetchone()[0] == 0
