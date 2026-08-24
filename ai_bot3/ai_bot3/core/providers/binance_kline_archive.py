from __future__ import annotations

import csv
import hashlib
import io
import sqlite3
import zipfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pandas as pd

from contracts.horizons import HORIZON_TIMEFRAME


TIMEFRAME_INTERVAL_MS = {
    timeframe: horizon * 1000 for horizon, timeframe in HORIZON_TIMEFRAME.items()
}
ARCHIVE_ROOT = "https://data.binance.vision/data/futures/um/monthly/klines"


def archive_url(symbol: str, timeframe: str, year_month: str) -> str:
    normalized = symbol.strip().upper()
    filename = f"{normalized}-{timeframe}-{year_month}.zip"
    return f"{ARCHIVE_ROOT}/{normalized}/{timeframe}/{filename}"


def checksum_url(symbol: str, timeframe: str, year_month: str) -> str:
    return archive_url(symbol, timeframe, year_month) + ".CHECKSUM"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _expected_checksum(body: bytes, expected_filename: str) -> str:
    text = body.decode("utf-8").strip()
    fields = text.split()
    if len(fields) < 2 or fields[-1].lstrip("*") != expected_filename:
        raise ValueError("Binance checksum receipt does not name the requested archive")
    digest = fields[0].lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("Binance checksum receipt is invalid")
    return digest


@dataclass(frozen=True)
class KlineArchiveImport:
    batch_id: str
    symbol: str
    timeframe: str
    year_month: str
    row_count: int
    earliest_open_time_ms: int
    latest_open_time_ms: int
    archive_sha256: str


class BinanceKlineArchiveStore:
    """Atomically add checksum-verified official Binance monthly kline files."""

    def __init__(self, database: Path) -> None:
        self.database = Path(database).resolve()
        if not self.database.is_file():
            raise FileNotFoundError(self.database)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=120)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_schema(self) -> None:
        with closing(self._connect()) as connection:
            raw_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='raw_kline'"""
            ).fetchone()
            if not raw_table:
                raise RuntimeError("target database has no raw_kline table")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS kline_archive_batches(
                    batch_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    source TEXT NOT NULL,
                    year_month TEXT NOT NULL,
                    archive_url TEXT NOT NULL,
                    checksum_url TEXT NOT NULL,
                    archive_path TEXT NOT NULL,
                    checksum_path TEXT NOT NULL,
                    archive_sha256 TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    checksum_verified INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    earliest_open_time_ms INTEGER NOT NULL,
                    latest_open_time_ms INTEGER NOT NULL,
                    imported_at TEXT NOT NULL,
                    UNIQUE(symbol,timeframe,source,year_month)
                );
                CREATE TABLE IF NOT EXISTS kline_archive_not_found(
                    evidence_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    source TEXT NOT NULL,
                    year_month TEXT NOT NULL,
                    archive_url TEXT NOT NULL,
                    checksum_url TEXT NOT NULL,
                    archive_http_status INTEGER NOT NULL,
                    checksum_http_status INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    UNIQUE(symbol,timeframe,source,year_month)
                );
                CREATE TABLE IF NOT EXISTS kline_listing_evidence(
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    listing_start_utc TEXT NOT NULL,
                    earliest_open_time_ms INTEGER NOT NULL,
                    first_archive_year_month TEXT NOT NULL,
                    first_archive_url TEXT NOT NULL,
                    first_archive_sha256 TEXT NOT NULL,
                    first_archive_checksum_verified INTEGER NOT NULL,
                    prior_month_year_month TEXT NOT NULL,
                    prior_month_archive_url TEXT NOT NULL,
                    prior_month_http_status INTEGER NOT NULL,
                    verified_at TEXT NOT NULL,
                    PRIMARY KEY(symbol,timeframe,source)
                );
                CREATE TRIGGER IF NOT EXISTS reject_kline_archive_batch_update
                BEFORE UPDATE ON kline_archive_batches
                BEGIN SELECT RAISE(ABORT,'completed kline archive batches are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_kline_archive_batch_delete
                BEFORE DELETE ON kline_archive_batches
                BEGIN SELECT RAISE(ABORT,'kline archive batches are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS reject_kline_archive_not_found_update
                BEFORE UPDATE ON kline_archive_not_found
                BEGIN SELECT RAISE(ABORT,'kline 404 evidence is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_kline_archive_not_found_delete
                BEFORE DELETE ON kline_archive_not_found
                BEGIN SELECT RAISE(ABORT,'kline 404 evidence is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS reject_kline_listing_evidence_update
                BEFORE UPDATE ON kline_listing_evidence
                BEGIN SELECT RAISE(ABORT,'kline listing evidence is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_kline_listing_evidence_delete
                BEFORE DELETE ON kline_listing_evidence
                BEGIN SELECT RAISE(ABORT,'kline listing evidence is append-only'); END;
                """
            )
            connection.commit()

    def completed(self, symbol: str, timeframe: str, year_month: str) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT 1 FROM kline_archive_batches
                    WHERE symbol=? AND timeframe=? AND source='binance'
                      AND year_month=?""",
                (symbol.strip().upper(), timeframe, year_month),
            ).fetchone()
        return row is not None

    def not_found(self, symbol: str, timeframe: str, year_month: str) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT 1 FROM kline_archive_not_found
                    WHERE symbol=? AND timeframe=? AND source='binance'
                      AND year_month=?""",
                (symbol.strip().upper(), timeframe, year_month),
            ).fetchone()
        return row is not None

    @staticmethod
    def _rows(
        archive_body: bytes,
        *,
        symbol: str,
        timeframe: str,
        year_month: str,
    ) -> list[tuple[object, ...]]:
        interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
        expected_filename = f"{symbol}-{timeframe}-{year_month}.csv"
        with zipfile.ZipFile(io.BytesIO(archive_body)) as archive:
            files = [name for name in archive.namelist() if not name.endswith("/")]
            if files != [expected_filename]:
                raise ValueError("Binance kline archive has an unexpected file set")
            stream = io.TextIOWrapper(archive.open(expected_filename), encoding="utf-8")
            parsed = list(csv.reader(stream))
        if parsed and parsed[0] and not str(parsed[0][0]).isdigit():
            parsed = parsed[1:]
        if not parsed:
            raise ValueError("Binance kline archive is empty")
        output: list[tuple[object, ...]] = []
        previous_open: int | None = None
        fetched_at = _iso(datetime.now(timezone.utc))
        for row in parsed:
            if len(row) < 7:
                raise ValueError("Binance kline archive row is truncated")
            open_time = int(row[0])
            close_time = int(row[6])
            values = tuple(float(row[index]) for index in (1, 2, 3, 4, 5))
            open_price, high, low, close, volume = values
            if min(open_price, high, low, close) <= 0 or volume < 0:
                raise ValueError("Binance kline archive contains invalid market values")
            if high < max(open_price, close) or low > min(open_price, close):
                raise ValueError("Binance kline OHLC invariants failed")
            if abs((close_time - open_time + 1) - interval_ms) > 1_000:
                raise ValueError("Binance kline duration contract failed")
            if previous_open is not None and open_time - previous_open != interval_ms:
                raise ValueError("Binance monthly kline archive is discontinuous")
            observed_month = pd.Timestamp(open_time, unit="ms", tz="UTC").strftime(
                "%Y-%m"
            )
            if observed_month != year_month:
                raise ValueError("Binance archive row falls outside its named month")
            output.append(
                (
                    symbol,
                    timeframe,
                    "binance",
                    open_time,
                    close_time,
                    open_price,
                    high,
                    low,
                    close,
                    volume,
                    fetched_at,
                )
            )
            previous_open = open_time
        return output

    def import_month(
        self,
        *,
        symbol: str,
        timeframe: str,
        year_month: str,
        archive_body: bytes,
        checksum_body: bytes,
        archive_path: Path,
        checksum_path: Path,
    ) -> KlineArchiveImport:
        normalized = symbol.strip().upper()
        if timeframe not in TIMEFRAME_INTERVAL_MS:
            raise ValueError(f"unsupported Binance archive timeframe: {timeframe}")
        filename = f"{normalized}-{timeframe}-{year_month}.zip"
        expected = _expected_checksum(checksum_body, filename)
        archive_sha = _sha256(archive_body)
        if archive_sha != expected:
            raise ValueError("Binance archive SHA256 does not match CHECKSUM")
        archive_path = Path(archive_path).resolve()
        checksum_path = Path(checksum_path).resolve()
        if not archive_path.is_file() or _sha256(archive_path.read_bytes()) != archive_sha:
            raise ValueError("retained Binance archive file does not match imported bytes")
        if not checksum_path.is_file() or checksum_path.read_bytes() != checksum_body:
            raise ValueError("retained Binance checksum file does not match imported bytes")
        rows = self._rows(
            archive_body,
            symbol=normalized,
            timeframe=timeframe,
            year_month=year_month,
        )
        batch_id = "bka_" + hashlib.sha256(
            f"{normalized}|{timeframe}|{year_month}|{archive_sha}".encode()
        ).hexdigest()[:40]
        with closing(self._connect()) as connection:
            existing = connection.execute(
                "SELECT * FROM kline_archive_batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if existing:
                return KlineArchiveImport(
                    batch_id=batch_id,
                    symbol=normalized,
                    timeframe=timeframe,
                    year_month=year_month,
                    row_count=int(existing["row_count"]),
                    earliest_open_time_ms=int(existing["earliest_open_time_ms"]),
                    latest_open_time_ms=int(existing["latest_open_time_ms"]),
                    archive_sha256=str(existing["archive_sha256"]),
                )
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                """INSERT OR REPLACE INTO raw_kline(
                       symbol,timeframe,source,open_time,close_time,open,high,low,
                       close,volume,fetched_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            connection.execute(
                """INSERT INTO kline_archive_batches(
                       batch_id,symbol,timeframe,source,year_month,archive_url,
                       checksum_url,archive_path,checksum_path,archive_sha256,
                       checksum_sha256,checksum_verified,row_count,
                       earliest_open_time_ms,latest_open_time_ms,imported_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    batch_id,
                    normalized,
                    timeframe,
                    "binance",
                    year_month,
                    archive_url(normalized, timeframe, year_month),
                    checksum_url(normalized, timeframe, year_month),
                    str(archive_path),
                    str(checksum_path),
                    archive_sha,
                    _sha256(checksum_body),
                    1,
                    len(rows),
                    int(rows[0][3]),
                    int(rows[-1][3]),
                    _iso(datetime.now(timezone.utc)),
                ),
            )
            connection.commit()
        return KlineArchiveImport(
            batch_id=batch_id,
            symbol=normalized,
            timeframe=timeframe,
            year_month=year_month,
            row_count=len(rows),
            earliest_open_time_ms=int(rows[0][3]),
            latest_open_time_ms=int(rows[-1][3]),
            archive_sha256=archive_sha,
        )

    def record_not_found(
        self,
        *,
        symbol: str,
        timeframe: str,
        year_month: str,
        archive_http_status: int,
        checksum_http_status: int,
    ) -> None:
        if archive_http_status != 404 or checksum_http_status != 404:
            raise ValueError(
                "listing evidence requires independent archive and checksum HTTP 404 responses"
            )
        normalized = symbol.strip().upper()
        evidence_id = "bkn_" + hashlib.sha256(
            f"{normalized}|{timeframe}|{year_month}|404".encode()
        ).hexdigest()[:40]
        with closing(self._connect()) as connection:
            connection.execute(
                """INSERT OR IGNORE INTO kline_archive_not_found(
                       evidence_id,symbol,timeframe,source,year_month,archive_url,
                       checksum_url,archive_http_status,checksum_http_status,
                       observed_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    evidence_id,
                    normalized,
                    timeframe,
                    "binance",
                    year_month,
                    archive_url(normalized, timeframe, year_month),
                    checksum_url(normalized, timeframe, year_month),
                    int(archive_http_status),
                    int(checksum_http_status),
                    _iso(datetime.now(timezone.utc)),
                ),
            )
            connection.commit()

    def finalize_listing_evidence(
        self,
        *,
        symbol: str,
        timeframe: str,
        first_archive_year_month: str,
        prior_month_year_month: str,
    ) -> None:
        normalized = symbol.strip().upper()
        with closing(self._connect()) as connection:
            first = connection.execute(
                """SELECT * FROM kline_archive_batches
                    WHERE symbol=? AND timeframe=? AND source='binance'
                      AND year_month=?""",
                (normalized, timeframe, first_archive_year_month),
            ).fetchone()
            prior = connection.execute(
                """SELECT * FROM kline_archive_not_found
                    WHERE symbol=? AND timeframe=? AND source='binance'
                      AND year_month=?""",
                (normalized, timeframe, prior_month_year_month),
            ).fetchone()
            if first is None or prior is None:
                raise ValueError("listing boundary requires completed and prior-404 receipts")
            prior_status = min(
                int(prior["archive_http_status"]),
                int(prior["checksum_http_status"]),
            )
            if prior_status != 404:
                raise ValueError("prior archive month is not a verified 404")
            earliest_ms = int(first["earliest_open_time_ms"])
            listing_start = pd.Timestamp(earliest_ms, unit="ms", tz="UTC")
            connection.execute(
                """INSERT OR IGNORE INTO kline_listing_evidence(
                       symbol,timeframe,source,status,listing_start_utc,
                       earliest_open_time_ms,first_archive_year_month,
                       first_archive_url,first_archive_sha256,
                       first_archive_checksum_verified,prior_month_year_month,
                       prior_month_archive_url,prior_month_http_status,verified_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    normalized,
                    timeframe,
                    "binance",
                    "VERIFIED_SINCE_LISTING",
                    listing_start.isoformat().replace("+00:00", "Z"),
                    earliest_ms,
                    first_archive_year_month,
                    first["archive_url"],
                    first["archive_sha256"],
                    int(first["checksum_verified"]),
                    prior_month_year_month,
                    prior["archive_url"],
                    404,
                    _iso(datetime.now(timezone.utc)),
                ),
            )
            connection.commit()


__all__: Sequence[str] = (
    "ARCHIVE_ROOT",
    "BinanceKlineArchiveStore",
    "KlineArchiveImport",
    "archive_url",
    "checksum_url",
)
