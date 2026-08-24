from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import parse_qs, urlencode, urlparse

from contracts.horizons import HORIZON_TIMEFRAME


API_BASE = "https://api.bybit.com"
TIMEFRAME_INTERVAL_MS = {
    timeframe: int(horizon) * 1_000
    for horizon, timeframe in HORIZON_TIMEFRAME.items()
}
BYBIT_INTERVAL = {
    "3m": "3",
    "15m": "15",
    "2h": "120",
    "4h": "240",
    "1d": "D",
}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Bybit receipt timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def instrument_url(symbol: str) -> str:
    query = urlencode({"category": "linear", "symbol": symbol.strip().upper()})
    return f"{API_BASE}/v5/market/instruments-info?{query}"


def kline_url(
    symbol: str,
    timeframe: str,
    *,
    start_ms: int,
    end_ms: int,
    limit: int = 1_000,
) -> str:
    if timeframe not in BYBIT_INTERVAL:
        raise ValueError(f"unsupported Bybit kline timeframe: {timeframe}")
    if start_ms < 0 or end_ms <= start_ms or not 1 <= limit <= 1_000:
        raise ValueError("invalid Bybit kline request window")
    query = urlencode(
        {
            "category": "linear",
            "symbol": symbol.strip().upper(),
            "interval": BYBIT_INTERVAL[timeframe],
            "start": int(start_ms),
            "end": int(end_ms) - 1,
            "limit": int(limit),
        }
    )
    return f"{API_BASE}/v5/market/kline?{query}"


@dataclass(frozen=True)
class BybitHTTPReceipt:
    request_url: str
    requested_at: datetime
    received_at: datetime
    http_status: int
    body: bytes

    def __post_init__(self) -> None:
        requested = _utc(self.requested_at)
        received = _utc(self.received_at)
        if received < requested:
            raise ValueError("Bybit response precedes its request")
        if self.http_status != 200 or not self.body:
            raise ValueError("completed Bybit receipts require a non-empty HTTP 200 body")
        if not self.request_url.startswith(f"{API_BASE}/v5/market/"):
            raise ValueError("Bybit receipt URL is not an official market endpoint")


@dataclass(frozen=True)
class BybitKlineImport:
    batch_id: str
    symbol: str
    timeframe: str
    window_start_ms: int
    window_end_ms: int
    row_count: int
    response_count: int
    earliest_open_time_ms: int
    latest_open_time_ms: int
    request_manifest_sha256: str


def _payload(receipt: BybitHTTPReceipt) -> Mapping[str, object]:
    try:
        payload = json.loads(receipt.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Bybit receipt is not valid UTF-8 JSON") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("Bybit response root must be an object")
    if int(payload.get("retCode", -1)) != 0:
        raise ValueError(f"Bybit response retCode is not zero: {payload.get('retCode')}")
    return payload


def _instrument(receipt: BybitHTTPReceipt, symbol: str) -> dict[str, object]:
    payload = _payload(receipt)
    result = payload.get("result")
    if not isinstance(result, Mapping) or str(result.get("category")) != "linear":
        raise ValueError("Bybit instrument response category contract failed")
    records = result.get("list")
    if not isinstance(records, list):
        raise ValueError("Bybit instrument response list is missing")
    normalized = symbol.strip().upper()
    matches = [
        item
        for item in records
        if isinstance(item, Mapping) and str(item.get("symbol")) == normalized
    ]
    if len(matches) != 1:
        raise ValueError("Bybit instrument response must contain the requested symbol once")
    item = dict(matches[0])
    launch_time_ms = int(item.get("launchTime", 0))
    if (
        launch_time_ms <= 0
        or str(item.get("contractType")) != "LinearPerpetual"
        or str(item.get("quoteCoin")) != "USDT"
        or str(item.get("status")) != "Trading"
    ):
        raise ValueError("Bybit instrument is not a live linear USDT perpetual")
    return {
        "symbol": normalized,
        "launch_time_ms": launch_time_ms,
        "contract_type": str(item["contractType"]),
        "status": str(item["status"]),
    }


class BybitKlineHistoryStore:
    """Import official Bybit last-trade klines with immutable raw receipts."""

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
                CREATE TABLE IF NOT EXISTS bybit_kline_instrument_receipts(
                    receipt_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    request_url TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    content_blob BLOB NOT NULL,
                    launch_time_ms INTEGER NOT NULL,
                    contract_type TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bybit_kline_batches(
                    batch_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    source TEXT NOT NULL,
                    window_start_ms INTEGER NOT NULL,
                    window_end_ms INTEGER NOT NULL,
                    instrument_receipt_id TEXT NOT NULL,
                    response_count INTEGER NOT NULL,
                    request_manifest_sha256 TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    earliest_open_time_ms INTEGER NOT NULL,
                    latest_open_time_ms INTEGER NOT NULL,
                    imported_at TEXT NOT NULL,
                    UNIQUE(symbol,timeframe,source,window_start_ms,window_end_ms),
                    FOREIGN KEY(instrument_receipt_id)
                        REFERENCES bybit_kline_instrument_receipts(receipt_id)
                );
                CREATE TABLE IF NOT EXISTS bybit_kline_api_responses(
                    response_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    request_url TEXT NOT NULL,
                    window_start_ms INTEGER NOT NULL,
                    window_end_ms INTEGER NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    content_blob BLOB NOT NULL,
                    row_count INTEGER NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES bybit_kline_batches(batch_id),
                    UNIQUE(batch_id,request_url)
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_instrument_symbol
                    ON bybit_kline_instrument_receipts(symbol,received_at);
                CREATE INDEX IF NOT EXISTS idx_bybit_kline_batch_series
                    ON bybit_kline_batches(symbol,timeframe,window_start_ms);
                CREATE INDEX IF NOT EXISTS idx_bybit_kline_response_batch
                    ON bybit_kline_api_responses(batch_id,response_id);
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_instrument_update
                BEFORE UPDATE ON bybit_kline_instrument_receipts
                BEGIN SELECT RAISE(ABORT,'Bybit instrument receipts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_instrument_delete
                BEFORE DELETE ON bybit_kline_instrument_receipts
                BEGIN SELECT RAISE(ABORT,'Bybit instrument receipts are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_batch_update
                BEFORE UPDATE ON bybit_kline_batches
                BEGIN SELECT RAISE(ABORT,'Bybit kline batches are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_batch_delete
                BEFORE DELETE ON bybit_kline_batches
                BEGIN SELECT RAISE(ABORT,'Bybit kline batches are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_response_update
                BEFORE UPDATE ON bybit_kline_api_responses
                BEGIN SELECT RAISE(ABORT,'Bybit kline responses are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_kline_response_delete
                BEFORE DELETE ON bybit_kline_api_responses
                BEGIN SELECT RAISE(ABORT,'Bybit kline responses are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_raw_kline_update
                BEFORE UPDATE ON raw_kline WHEN OLD.source='bybit'
                BEGIN SELECT RAISE(ABORT,'Bybit raw klines are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS reject_bybit_raw_kline_delete
                BEFORE DELETE ON raw_kline WHEN OLD.source='bybit'
                BEGIN SELECT RAISE(ABORT,'Bybit raw klines are append-only'); END;
                """
            )
            connection.commit()

    def record_instrument(
        self,
        symbol: str,
        receipt: BybitHTTPReceipt,
    ) -> str:
        normalized = symbol.strip().upper()
        expected_url = instrument_url(normalized)
        if receipt.request_url != expected_url:
            raise ValueError("Bybit instrument receipt URL does not match the request")
        parsed = _instrument(receipt, normalized)
        content_sha256 = _sha256(receipt.body)
        receipt_id = "bki_" + hashlib.sha256(
            f"{normalized}|{content_sha256}".encode()
        ).hexdigest()[:40]
        with closing(self._connect()) as connection:
            existing = connection.execute(
                """SELECT * FROM bybit_kline_instrument_receipts
                    WHERE symbol=? AND content_sha256=?""",
                (normalized, content_sha256),
            ).fetchone()
            if existing:
                return str(existing["receipt_id"])
            connection.execute(
                """INSERT INTO bybit_kline_instrument_receipts(
                       receipt_id,symbol,request_url,requested_at,received_at,
                       http_status,content_length,content_sha256,content_blob,
                       launch_time_ms,contract_type,status
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    receipt_id,
                    normalized,
                    receipt.request_url,
                    _iso(receipt.requested_at),
                    _iso(receipt.received_at),
                    receipt.http_status,
                    len(receipt.body),
                    content_sha256,
                    receipt.body,
                    int(parsed["launch_time_ms"]),
                    parsed["contract_type"],
                    parsed["status"],
                ),
            )
            connection.commit()
        return receipt_id

    def completed(
        self,
        symbol: str,
        timeframe: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT 1 FROM bybit_kline_batches
                    WHERE symbol=? AND timeframe=? AND source='bybit'
                      AND window_start_ms=? AND window_end_ms=?""",
                (
                    symbol.strip().upper(),
                    timeframe,
                    int(window_start_ms),
                    int(window_end_ms),
                ),
            ).fetchone()
        return row is not None

    @staticmethod
    def _request_window(
        request_url: str,
        *,
        symbol: str,
        timeframe: str,
    ) -> tuple[int, int, int]:
        parsed = urlparse(request_url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "api.bybit.com"
            or parsed.path != "/v5/market/kline"
            or parsed.fragment
        ):
            raise ValueError("Bybit kline receipt URL is not the official endpoint")
        query = parse_qs(parsed.query, keep_blank_values=True)
        required = {"category", "symbol", "interval", "start", "end", "limit"}
        if set(query) != required or any(len(values) != 1 for values in query.values()):
            raise ValueError("Bybit kline receipt query contract failed")
        if (
            query["category"][0] != "linear"
            or query["symbol"][0] != symbol
            or query["interval"][0] != BYBIT_INTERVAL[timeframe]
        ):
            raise ValueError("Bybit kline receipt query identity failed")
        try:
            start_ms = int(query["start"][0])
            end_ms = int(query["end"][0]) + 1
            limit = int(query["limit"][0])
        except ValueError as exc:
            raise ValueError("Bybit kline receipt query is not numeric") from exc
        interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
        if (
            start_ms < 0
            or end_ms <= start_ms
            or start_ms % interval_ms
            or end_ms % interval_ms
            or not 1 <= limit <= 1_000
            or (end_ms - start_ms) // interval_ms > limit
        ):
            raise ValueError("Bybit kline receipt request window is invalid")
        if request_url != kline_url(
            symbol,
            timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=limit,
        ):
            raise ValueError("Bybit kline receipt URL is not canonical")
        return start_ms, end_ms, limit

    @staticmethod
    def _response_rows(
        receipt: BybitHTTPReceipt,
        *,
        symbol: str,
        timeframe: str,
        request_start_ms: int,
        request_end_ms: int,
    ) -> dict[int, tuple[float, float, float, float, float]]:
        payload = _payload(receipt)
        result = payload.get("result")
        if (
            not isinstance(result, Mapping)
            or str(result.get("category")) != "linear"
            or str(result.get("symbol")) != symbol
        ):
            raise ValueError("Bybit kline response identity contract failed")
        raw_rows = result.get("list")
        if not isinstance(raw_rows, list):
            raise ValueError("Bybit kline response list is missing")
        interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
        output: dict[int, tuple[float, float, float, float, float]] = {}
        for raw in raw_rows:
            if not isinstance(raw, list) or len(raw) < 7:
                raise ValueError("Bybit kline response row is truncated")
            open_time = int(raw[0])
            if not request_start_ms <= open_time < request_end_ms:
                raise ValueError("Bybit kline response contains a row outside its request")
            if open_time % interval_ms:
                raise ValueError("Bybit kline open time is off-grid")
            values = tuple(float(raw[index]) for index in (1, 2, 3, 4, 5))
            open_price, high, low, close, volume = values
            if (
                any(not math.isfinite(value) for value in values)
                or min(open_price, high, low, close) <= 0
                or volume < 0
                or high < max(open_price, close)
                or low > min(open_price, close)
            ):
                raise ValueError("Bybit kline OHLCV invariants failed")
            if open_time in output and output[open_time] != values:
                raise ValueError("Bybit kline response contains a conflicting duplicate")
            output[open_time] = values
        return output

    def import_window(
        self,
        *,
        symbol: str,
        timeframe: str,
        window_start_ms: int,
        window_end_ms: int,
        instrument_receipt_id: str,
        responses: Sequence[BybitHTTPReceipt],
    ) -> BybitKlineImport:
        normalized = symbol.strip().upper()
        if timeframe not in TIMEFRAME_INTERVAL_MS:
            raise ValueError(f"unsupported Bybit kline timeframe: {timeframe}")
        interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
        if (
            window_start_ms < 0
            or window_end_ms <= window_start_ms
            or window_start_ms % interval_ms
            or window_end_ms % interval_ms
            or not responses
        ):
            raise ValueError("Bybit kline import window is invalid")
        with closing(self._connect()) as connection:
            instrument = connection.execute(
                """SELECT * FROM bybit_kline_instrument_receipts
                    WHERE receipt_id=? AND symbol=?""",
                (instrument_receipt_id, normalized),
            ).fetchone()
        if instrument is None:
            raise ValueError("Bybit kline import lacks an instrument launch receipt")
        launch_time_ms = int(instrument["launch_time_ms"])
        launch_floor = launch_time_ms - launch_time_ms % interval_ms
        expected_request_start = max(window_start_ms, launch_floor)
        rows: dict[int, tuple[float, float, float, float, float]] = {}
        response_records: list[dict[str, object]] = []
        request_windows: list[tuple[int, int]] = []
        request_urls: set[str] = set()
        for receipt in responses:
            if receipt.request_url in request_urls:
                raise ValueError("Bybit kline import contains a duplicate request URL")
            request_urls.add(receipt.request_url)
            request_start_ms, request_end_ms, _ = self._request_window(
                receipt.request_url,
                symbol=normalized,
                timeframe=timeframe,
            )
            if (
                request_start_ms < expected_request_start
                or request_end_ms > window_end_ms
            ):
                raise ValueError("Bybit kline receipt request exceeds the import window")
            request_windows.append((request_start_ms, request_end_ms))
            parsed = self._response_rows(
                receipt,
                symbol=normalized,
                timeframe=timeframe,
                request_start_ms=request_start_ms,
                request_end_ms=request_end_ms,
            )
            for open_time, values in parsed.items():
                if open_time in rows and rows[open_time] != values:
                    raise ValueError("Bybit kline receipts conflict")
                rows[open_time] = values
            response_records.append(
                {
                    "request_url": receipt.request_url,
                    "window_start_ms": request_start_ms,
                    "window_end_ms": request_end_ms,
                    "requested_at": _iso(receipt.requested_at),
                    "received_at": _iso(receipt.received_at),
                    "http_status": receipt.http_status,
                    "content_length": len(receipt.body),
                    "content_sha256": _sha256(receipt.body),
                    "content_blob": receipt.body,
                    "row_count": len(parsed),
                }
            )
        ordered_windows = sorted(request_windows)
        if (
            ordered_windows[0][0] != expected_request_start
            or ordered_windows[-1][1] != window_end_ms
            or any(
                next_start != previous_end
                for (_, previous_end), (next_start, _) in zip(
                    ordered_windows, ordered_windows[1:]
                )
            )
        ):
            raise ValueError("Bybit kline receipt requests do not partition the window")
        if not rows:
            raise ValueError("Bybit kline import is empty")
        times = sorted(rows)
        expected_first = max(window_start_ms, launch_floor)
        if times[0] not in {expected_first, expected_first + interval_ms}:
            raise ValueError("Bybit first kline is not aligned with launch/window evidence")
        expected_last = window_end_ms - interval_ms
        if times[-1] != expected_last:
            raise ValueError("Bybit kline import does not reach the requested completed end")
        if any(later - earlier != interval_ms for earlier, later in zip(times, times[1:])):
            raise ValueError("Bybit kline import is discontinuous")
        expected_count = (times[-1] - times[0]) // interval_ms + 1
        if len(times) != expected_count:
            raise ValueError("Bybit kline import grid is incomplete")
        request_manifest = [
            {
                "request_url": item["request_url"],
                "window_start_ms": item["window_start_ms"],
                "window_end_ms": item["window_end_ms"],
                "content_sha256": item["content_sha256"],
                "row_count": item["row_count"],
            }
            for item in sorted(response_records, key=lambda item: str(item["request_url"]))
        ]
        manifest_sha256 = _sha256(_canonical(request_manifest).encode())
        batch_id = "bkb_" + hashlib.sha256(
            (
                f"{normalized}|{timeframe}|{window_start_ms}|{window_end_ms}|"
                f"{instrument_receipt_id}|{manifest_sha256}"
            ).encode()
        ).hexdigest()[:40]
        fetched_at = max(str(item["received_at"]) for item in response_records)
        raw_rows = [
            (
                normalized,
                timeframe,
                "bybit",
                open_time,
                open_time + interval_ms,
                *rows[open_time],
                fetched_at,
            )
            for open_time in times
        ]
        with closing(self._connect()) as connection:
            existing = connection.execute(
                "SELECT * FROM bybit_kline_batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if existing:
                return BybitKlineImport(
                    batch_id=batch_id,
                    symbol=normalized,
                    timeframe=timeframe,
                    window_start_ms=window_start_ms,
                    window_end_ms=window_end_ms,
                    row_count=int(existing["row_count"]),
                    response_count=int(existing["response_count"]),
                    earliest_open_time_ms=int(existing["earliest_open_time_ms"]),
                    latest_open_time_ms=int(existing["latest_open_time_ms"]),
                    request_manifest_sha256=str(existing["request_manifest_sha256"]),
                )
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                """INSERT INTO raw_kline(
                       symbol,timeframe,source,open_time,close_time,open,high,low,
                       close,volume,fetched_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                raw_rows,
            )
            connection.execute(
                """INSERT INTO bybit_kline_batches(
                       batch_id,symbol,timeframe,source,window_start_ms,
                       window_end_ms,instrument_receipt_id,response_count,
                       request_manifest_sha256,row_count,earliest_open_time_ms,
                       latest_open_time_ms,imported_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    batch_id,
                    normalized,
                    timeframe,
                    "bybit",
                    window_start_ms,
                    window_end_ms,
                    instrument_receipt_id,
                    len(response_records),
                    manifest_sha256,
                    len(raw_rows),
                    times[0],
                    times[-1],
                    _iso(datetime.now(timezone.utc)),
                ),
            )
            for item in response_records:
                response_id = "bkr_" + hashlib.sha256(
                    (
                        f"{batch_id}|{item['request_url']}|"
                        f"{item['content_sha256']}"
                    ).encode()
                ).hexdigest()[:40]
                connection.execute(
                    """INSERT INTO bybit_kline_api_responses(
                           response_id,batch_id,request_url,window_start_ms,
                           window_end_ms,requested_at,received_at,http_status,
                           content_length,content_sha256,content_blob,row_count
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        response_id,
                        batch_id,
                        item["request_url"],
                        item["window_start_ms"],
                        item["window_end_ms"],
                        item["requested_at"],
                        item["received_at"],
                        item["http_status"],
                        item["content_length"],
                        item["content_sha256"],
                        item["content_blob"],
                        item["row_count"],
                    ),
                )
            connection.commit()
        return BybitKlineImport(
            batch_id=batch_id,
            symbol=normalized,
            timeframe=timeframe,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            row_count=len(raw_rows),
            response_count=len(response_records),
            earliest_open_time_ms=times[0],
            latest_open_time_ms=times[-1],
            request_manifest_sha256=manifest_sha256,
        )


def verify_bybit_listing_evidence(
    database: Path,
    symbol: str,
    timeframe: str,
) -> dict[str, object] | None:
    """Reparse retained Bybit bodies and prove one complete same-venue grid."""

    normalized = symbol.strip().upper()
    if timeframe not in TIMEFRAME_INTERVAL_MS:
        raise ValueError(f"unsupported Bybit kline timeframe: {timeframe}")
    path = Path(database).resolve()
    uri = f"file:{path.as_posix()}?mode=ro"
    required_tables = {
        "bybit_kline_instrument_receipts",
        "bybit_kline_batches",
        "bybit_kline_api_responses",
        "raw_kline",
    }
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if not required_tables.issubset(tables):
            return None
        batch = connection.execute(
            """SELECT batch.*, instrument.request_url AS instrument_request_url,
                      instrument.requested_at AS instrument_requested_at,
                      instrument.received_at AS instrument_received_at,
                      instrument.http_status AS instrument_http_status,
                      instrument.content_length AS instrument_content_length,
                      instrument.content_sha256 AS instrument_content_sha256,
                      instrument.content_blob AS instrument_content_blob,
                      instrument.launch_time_ms AS launch_time_ms,
                      instrument.contract_type AS contract_type,
                      instrument.status AS instrument_status
                 FROM bybit_kline_batches AS batch
                 JOIN bybit_kline_instrument_receipts AS instrument
                   ON instrument.receipt_id=batch.instrument_receipt_id
                WHERE batch.symbol=? AND batch.timeframe=? AND batch.source='bybit'
                ORDER BY (batch.window_end_ms-batch.window_start_ms) DESC,
                         batch.imported_at DESC
                LIMIT 1""",
            (normalized, timeframe),
        ).fetchone()
        if batch is None:
            return None
        responses = connection.execute(
            """SELECT * FROM bybit_kline_api_responses
                WHERE batch_id=? ORDER BY window_start_ms,window_end_ms""",
            (batch["batch_id"],),
        ).fetchall()
        raw_rows = connection.execute(
            """SELECT open_time,close_time,open,high,low,close,volume,fetched_at
                 FROM raw_kline
                WHERE symbol=? AND timeframe=? AND source='bybit'
                ORDER BY open_time""",
            (normalized, timeframe),
        ).fetchall()
        immutable_triggers = {
            str(row[0])
            for row in connection.execute(
                """SELECT name FROM sqlite_master
                    WHERE type='trigger' AND name LIKE 'reject_bybit_%'"""
            ).fetchall()
        }

    failures: list[str] = []

    def fail(reason: str) -> None:
        if reason not in failures:
            failures.append(reason)

    expected_triggers = {
        "reject_bybit_kline_instrument_update",
        "reject_bybit_kline_instrument_delete",
        "reject_bybit_kline_batch_update",
        "reject_bybit_kline_batch_delete",
        "reject_bybit_kline_response_update",
        "reject_bybit_kline_response_delete",
        "reject_bybit_raw_kline_update",
        "reject_bybit_raw_kline_delete",
    }
    if not expected_triggers.issubset(immutable_triggers):
        fail("immutable_trigger_set_incomplete")

    instrument_body = bytes(batch["instrument_content_blob"])
    if len(instrument_body) != int(batch["instrument_content_length"]):
        fail("instrument_content_length_mismatch")
    if _sha256(instrument_body) != str(batch["instrument_content_sha256"]):
        fail("instrument_content_sha256_mismatch")
    try:
        instrument_receipt = BybitHTTPReceipt(
            request_url=str(batch["instrument_request_url"]),
            requested_at=datetime.fromisoformat(
                str(batch["instrument_requested_at"]).replace("Z", "+00:00")
            ),
            received_at=datetime.fromisoformat(
                str(batch["instrument_received_at"]).replace("Z", "+00:00")
            ),
            http_status=int(batch["instrument_http_status"]),
            body=instrument_body,
        )
        if instrument_receipt.request_url != instrument_url(normalized):
            raise ValueError("instrument URL mismatch")
        parsed_instrument = _instrument(instrument_receipt, normalized)
        if (
            int(parsed_instrument["launch_time_ms"]) != int(batch["launch_time_ms"])
            or parsed_instrument["contract_type"] != batch["contract_type"]
            or parsed_instrument["status"] != batch["instrument_status"]
        ):
            raise ValueError("instrument identity mismatch")
    except (TypeError, ValueError) as exc:
        fail(f"instrument_receipt_invalid:{type(exc).__name__}")

    interval_ms = TIMEFRAME_INTERVAL_MS[timeframe]
    window_start_ms = int(batch["window_start_ms"])
    window_end_ms = int(batch["window_end_ms"])
    launch_time_ms = int(batch["launch_time_ms"])
    launch_floor = launch_time_ms - launch_time_ms % interval_ms
    expected_request_start = max(window_start_ms, launch_floor)
    parsed_rows: dict[int, tuple[float, float, float, float, float]] = {}
    request_windows: list[tuple[int, int]] = []
    request_manifest: list[dict[str, object]] = []
    for response in responses:
        body = bytes(response["content_blob"])
        if len(body) != int(response["content_length"]):
            fail("response_content_length_mismatch")
        content_sha256 = _sha256(body)
        if content_sha256 != str(response["content_sha256"]):
            fail("response_content_sha256_mismatch")
        try:
            receipt = BybitHTTPReceipt(
                request_url=str(response["request_url"]),
                requested_at=datetime.fromisoformat(
                    str(response["requested_at"]).replace("Z", "+00:00")
                ),
                received_at=datetime.fromisoformat(
                    str(response["received_at"]).replace("Z", "+00:00")
                ),
                http_status=int(response["http_status"]),
                body=body,
            )
            request_start_ms, request_end_ms, _ = (
                BybitKlineHistoryStore._request_window(
                    receipt.request_url,
                    symbol=normalized,
                    timeframe=timeframe,
                )
            )
            if (
                request_start_ms != int(response["window_start_ms"])
                or request_end_ms != int(response["window_end_ms"])
            ):
                raise ValueError("stored request boundary mismatch")
            response_rows = BybitKlineHistoryStore._response_rows(
                receipt,
                symbol=normalized,
                timeframe=timeframe,
                request_start_ms=request_start_ms,
                request_end_ms=request_end_ms,
            )
            if len(response_rows) != int(response["row_count"]):
                raise ValueError("stored response row count mismatch")
            for open_time, values in response_rows.items():
                if open_time in parsed_rows and parsed_rows[open_time] != values:
                    raise ValueError("response rows conflict")
                parsed_rows[open_time] = values
            request_windows.append((request_start_ms, request_end_ms))
            request_manifest.append(
                {
                    "request_url": receipt.request_url,
                    "window_start_ms": request_start_ms,
                    "window_end_ms": request_end_ms,
                    "content_sha256": content_sha256,
                    "row_count": len(response_rows),
                }
            )
        except (TypeError, ValueError) as exc:
            fail(f"response_receipt_invalid:{type(exc).__name__}")

    ordered_windows = sorted(request_windows)
    if (
        not ordered_windows
        or ordered_windows[0][0] != expected_request_start
        or ordered_windows[-1][1] != window_end_ms
        or any(
            next_start != previous_end
            for (_, previous_end), (next_start, _) in zip(
                ordered_windows, ordered_windows[1:]
            )
        )
    ):
        fail("response_request_partition_incomplete")
    expected_manifest_sha256 = _sha256(
        _canonical(sorted(request_manifest, key=lambda item: str(item["request_url"]))).encode()
    )
    if expected_manifest_sha256 != str(batch["request_manifest_sha256"]):
        fail("request_manifest_sha256_mismatch")
    if len(responses) != int(batch["response_count"]):
        fail("response_count_mismatch")

    observed_rows = {
        int(row["open_time"]): tuple(
            float(row[column]) for column in ("open", "high", "low", "close", "volume")
        )
        for row in raw_rows
    }
    if observed_rows != parsed_rows:
        fail("raw_rows_do_not_match_retained_responses")
    times = sorted(observed_rows)
    if not times:
        fail("raw_kline_grid_empty")
        earliest_open_time_ms = None
        latest_open_time_ms = None
    else:
        earliest_open_time_ms = times[0]
        latest_open_time_ms = times[-1]
        if times[0] not in {expected_request_start, expected_request_start + interval_ms}:
            fail("first_kline_not_supported_by_launch_receipt")
        if times[-1] != window_end_ms - interval_ms:
            fail("last_kline_does_not_reach_completed_end")
        if any(later - earlier != interval_ms for earlier, later in zip(times, times[1:])):
            fail("raw_kline_grid_discontinuous")
        if any(int(row["close_time"]) - int(row["open_time"]) != interval_ms for row in raw_rows):
            fail("raw_kline_close_boundary_invalid")
    if len(raw_rows) != int(batch["row_count"]):
        fail("batch_row_count_mismatch")
    if earliest_open_time_ms != int(batch["earliest_open_time_ms"]):
        fail("batch_earliest_open_mismatch")
    if latest_open_time_ms != int(batch["latest_open_time_ms"]):
        fail("batch_latest_open_mismatch")

    listing_start_utc = (
        datetime.fromtimestamp(earliest_open_time_ms / 1_000, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
        if earliest_open_time_ms is not None
        else None
    )
    launch_time_utc = (
        datetime.fromtimestamp(launch_time_ms / 1_000, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    verified_since_launch = window_start_ms <= launch_floor
    return {
        "status": (
            "FAILED"
            if failures
            else (
                "VERIFIED_SINCE_LAUNCH"
                if verified_since_launch
                else "VERIFIED_WINDOW"
            )
        ),
        "source": "bybit",
        "symbol": normalized,
        "timeframe": timeframe,
        "batch_id": str(batch["batch_id"]),
        "instrument_receipt_id": str(batch["instrument_receipt_id"]),
        "official_instrument_url": str(batch["instrument_request_url"]),
        "launch_time_ms": launch_time_ms,
        "launch_time_utc": launch_time_utc,
        "verified_since_launch": verified_since_launch,
        "listing_start_utc": listing_start_utc,
        "earliest_open_time_ms": earliest_open_time_ms,
        "latest_open_time_ms": latest_open_time_ms,
        "response_count": len(responses),
        "row_count": len(raw_rows),
        "raw_receipt_reverified": not failures,
        "raw_receipt_reverification_failures": failures,
        "immutable_trigger_count": len(expected_triggers & immutable_triggers),
        "request_manifest_sha256": str(batch["request_manifest_sha256"]),
    }


def instrument_launch_time_ms(
    receipt: BybitHTTPReceipt,
    symbol: str,
) -> int:
    """Return the official launch time after validating the instrument identity."""

    return int(_instrument(receipt, symbol)["launch_time_ms"])


__all__: Sequence[str] = (
    "API_BASE",
    "BYBIT_INTERVAL",
    "BybitHTTPReceipt",
    "BybitKlineHistoryStore",
    "BybitKlineImport",
    "TIMEFRAME_INTERVAL_MS",
    "instrument_url",
    "instrument_launch_time_ms",
    "kline_url",
    "verify_bybit_listing_evidence",
)
