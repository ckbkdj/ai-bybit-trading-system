from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections import defaultdict, deque
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Mapping, Sequence

from core.features.point_in_time_store import PointInTimeFeatureStore
from core.features.registry import default_registry


BYBIT_PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("capture timestamps must include a timezone")
    return value.astimezone(timezone.utc)


def _from_ms(value: Any) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000.0, timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _canonical(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class CaptureConflict(RuntimeError):
    pass


class StalePublicEvent(RuntimeError):
    pass


class BybitPublicPITStore:
    """Append-only raw public market events plus standardized PIT observations."""

    def __init__(
        self,
        path: Path,
        *,
        batch_writes: bool = False,
        batch_max_operations: int = 1_000,
        batch_max_interval_sec: float = 0.25,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if batch_max_operations <= 0 or batch_max_interval_sec <= 0:
            raise ValueError("invalid public PIT batch configuration")
        self.batch_writes = bool(batch_writes)
        self.batch_max_operations = int(batch_max_operations)
        self.batch_max_interval_sec = float(batch_max_interval_sec)
        self._batch_connection: sqlite3.Connection | None = None
        self._pending_operations = 0
        self._last_batch_commit = time.monotonic()
        self.registry = default_registry()
        self.quality_store = PointInTimeFeatureStore(self.path, self.registry)
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS bybit_capture_sessions(
                    session_id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    symbols_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS bybit_raw_public_events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    exchange_time TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    update_id INTEGER,
                    cross_sequence INTEGER,
                    book_state_valid INTEGER,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_raw_pit
                    ON bybit_raw_public_events(symbol, topic, received_at, sequence);
                CREATE TABLE IF NOT EXISTS bybit_feature_observations(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL UNIQUE,
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    quality REAL NOT NULL,
                    payload_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_feature_pit
                    ON bybit_feature_observations(symbol,name,available_at,sequence);
                CREATE TABLE IF NOT EXISTS bybit_feature_invalidations(
                    observation_id TEXT PRIMARY KEY,
                    invalidated_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    correction_version TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bybit_historical_archive_files(
                    archive_id TEXT PRIMARY KEY,
                    data_kind TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    member_name TEXT,
                    member_size INTEGER,
                    first_event_time TEXT,
                    last_event_time TEXT,
                    rows_read INTEGER NOT NULL DEFAULT 0,
                    feature_observation_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    error TEXT,
                    UNIQUE(data_kind,market,symbol,trading_date)
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_archive_status
                    ON bybit_historical_archive_files(status,data_kind,trading_date,symbol);
                CREATE TABLE IF NOT EXISTS bybit_historical_api_batches(
                    batch_id TEXT PRIMARY KEY,
                    data_kind TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    trading_date TEXT NOT NULL,
                    endpoint_group TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    first_event_time TEXT,
                    last_event_time TEXT,
                    response_count INTEGER NOT NULL,
                    rows_read INTEGER NOT NULL,
                    feature_observation_count INTEGER NOT NULL,
                    request_manifest_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    UNIQUE(data_kind,market,symbol,trading_date)
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_api_batch_status
                    ON bybit_historical_api_batches(status,data_kind,trading_date,symbol);
                CREATE TABLE IF NOT EXISTS bybit_historical_api_responses(
                    response_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    request_url TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    http_status INTEGER NOT NULL,
                    content_length INTEGER NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    rows_read INTEGER NOT NULL,
                    ret_code INTEGER NOT NULL,
                    UNIQUE(batch_id,request_url)
                );
                CREATE INDEX IF NOT EXISTS idx_bybit_api_response_batch
                    ON bybit_historical_api_responses(batch_id,response_id);
                """
            )
            feature_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(bybit_feature_observations)"
                ).fetchall()
            }
            if "provenance_kind" not in feature_columns:
                connection.execute(
                    """ALTER TABLE bybit_feature_observations
                       ADD COLUMN provenance_kind TEXT NOT NULL DEFAULT 'live_capture'"""
                )
            if "archive_id" not in feature_columns:
                connection.execute(
                    """ALTER TABLE bybit_feature_observations
                       ADD COLUMN archive_id TEXT"""
                )
            if "api_batch_id" not in feature_columns:
                connection.execute(
                    """ALTER TABLE bybit_feature_observations
                       ADD COLUMN api_batch_id TEXT"""
                )
            # The original liquidation-side interpretation was the inverse of
            # Bybit's documented position-side contract. Preserve those rows for
            # audit, but make them ineligible everywhere before v2 is rebuilt.
            connection.execute(
                """INSERT OR IGNORE INTO bybit_feature_invalidations(
                       observation_id,invalidated_at,reason,correction_version
                   )
                   SELECT observation_id,?,
                          'Bybit allLiquidation S=Buy means a long position was liquidated; v1 inverted the side',
                          'bybit-liquidation-side-v2'
                     FROM bybit_feature_observations
                    WHERE name='liquidation_imbalance_5m'
                      AND source='bybit.public.liquidations'""",
                (_iso(datetime.now(timezone.utc)),),
            )
        if self.batch_writes:
            self._batch_connection = self.connect()

    def _write_connection(self) -> tuple[sqlite3.Connection, bool]:
        if self._batch_connection is not None:
            return self._batch_connection, False
        return self.connect(), True

    def _complete_write(
        self,
        connection: sqlite3.Connection,
        *,
        close_after: bool,
        operations: int,
    ) -> None:
        if close_after:
            connection.commit()
            connection.close()
            return
        self._pending_operations += operations
        now = time.monotonic()
        if (
            self._pending_operations >= self.batch_max_operations
            or now - self._last_batch_commit >= self.batch_max_interval_sec
        ):
            self.flush()

    def flush(self) -> None:
        if self._batch_connection is None or self._pending_operations == 0:
            return
        self._batch_connection.commit()
        self._pending_operations = 0
        self._last_batch_commit = time.monotonic()

    def close(self) -> None:
        self.flush()
        if self._batch_connection is not None:
            self._batch_connection.close()
            self._batch_connection = None

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def start_session(
        self,
        session_id: str,
        *,
        endpoint: str,
        symbols: Sequence[str],
        started_at: datetime,
    ) -> None:
        self.flush()
        with self.connect() as connection:
            # A prior process can disappear without executing its cancellation
            # handler (host reboot, forced kill, interpreter crash). Reconcile
            # those stale rows before declaring the new capture session live,
            # otherwise operational evidence can falsely show two collectors.
            connection.execute(
                """UPDATE bybit_capture_sessions
                      SET ended_at=?,status='disconnected',
                          error=COALESCE(error, 'collector_restarted_after_unclean_shutdown')
                    WHERE status='running'""",
                (_iso(started_at),),
            )
            connection.execute(
                """INSERT INTO bybit_capture_sessions(
                       session_id,endpoint,symbols_json,started_at,status
                   ) VALUES (?,?,?,?,?)""",
                (session_id, endpoint, _canonical(sorted(symbols)), _iso(started_at), "running"),
            )
            connection.commit()

    def end_session(
        self,
        session_id: str,
        *,
        ended_at: datetime,
        status: str,
        error: str | None = None,
    ) -> None:
        if status not in {"completed", "disconnected", "failed"}:
            raise ValueError("invalid capture session status")
        self.flush()
        with self.connect() as connection:
            connection.execute(
                """UPDATE bybit_capture_sessions
                      SET ended_at=?,status=?,error=? WHERE session_id=?""",
                (_iso(ended_at), status, error, session_id),
            )
            connection.commit()

    def append_raw(
        self,
        *,
        event_id: str,
        session_id: str,
        topic: str,
        symbol: str,
        event_type: str,
        exchange_time: datetime,
        received_at: datetime,
        payload: object,
        update_id: int | None = None,
        cross_sequence: int | None = None,
        book_state_valid: bool | None = None,
    ) -> bool:
        encoded = _canonical(payload)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        connection, close_after = self._write_connection()
        try:
            row = connection.execute(
                "SELECT payload_sha256 FROM bybit_raw_public_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row:
                if row["payload_sha256"] != digest:
                    raise CaptureConflict("event_id already exists with different content")
                if close_after:
                    connection.close()
                return False
            connection.execute(
                """INSERT INTO bybit_raw_public_events(
                       event_id,session_id,topic,symbol,event_type,exchange_time,
                       received_at,update_id,cross_sequence,book_state_valid,
                       payload_json,payload_sha256
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id,
                    session_id,
                    topic,
                    symbol,
                    event_type,
                    _iso(exchange_time),
                    _iso(received_at),
                    update_id,
                    cross_sequence,
                    None if book_state_valid is None else int(book_state_valid),
                    encoded,
                    digest,
                ),
            )
            self._complete_write(
                connection, close_after=close_after, operations=1
            )
        except Exception:
            if close_after:
                connection.rollback()
                connection.close()
            raise
        return True

    def append_feature(
        self,
        *,
        event_id: str,
        symbol: str,
        name: str,
        value: float,
        unit: str,
        event_time: datetime,
        received_at: datetime,
        source: str,
        quality: float,
        ingested_at: datetime | None = None,
    ) -> bool:
        event_time = _utc(event_time)
        received_at = _utc(received_at)
        ingested_at = _utc(ingested_at or received_at)
        if event_time > received_at or received_at > ingested_at:
            self.flush()
            self.quality_store.source_event(
                source,
                "degraded",
                "feature_chronology_violation",
                ingested_at,
            )
            return False
        definition = self.registry.require(name)
        if definition.unit != unit:
            raise ValueError(f"unit mismatch for {name}: {unit} != {definition.unit}")
        normalized_symbol = symbol.strip().upper()
        token = hashlib.sha256(
            f"{event_id}|{normalized_symbol}|{name}".encode()
        ).hexdigest()[:48]
        observation_id = f"bp_{token}"
        payload = {
            "observation_id": observation_id,
            "symbol": normalized_symbol,
            "name": name,
            "value": float(value),
            "unit": unit,
            "event_time": _iso(event_time),
            "available_at": _iso(received_at),
            "ingested_at": _iso(ingested_at),
            "source": source,
            "quality": quality,
        }
        digest = hashlib.sha256(_canonical(payload).encode()).hexdigest()
        connection, close_after = self._write_connection()
        try:
            row = connection.execute(
                "SELECT payload_sha256 FROM bybit_feature_observations WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
            if row:
                if row["payload_sha256"] != digest:
                    raise CaptureConflict(
                        "feature observation_id already exists with different content"
                    )
                if close_after:
                    connection.close()
                return False
            connection.execute(
                """INSERT INTO bybit_feature_observations(
                       observation_id,symbol,name,value,unit,event_time,available_at,
                       ingested_at,source,quality,payload_sha256
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    observation_id,
                    normalized_symbol,
                    name,
                    float(value),
                    unit,
                    _iso(event_time),
                    _iso(received_at),
                    _iso(ingested_at),
                    source,
                    quality,
                    digest,
                ),
            )
            self._complete_write(
                connection, close_after=close_after, operations=1
            )
        except Exception:
            if close_after:
                connection.rollback()
                connection.close()
            raise
        return True

    def append_feature_batch(
        self,
        observations: Sequence[Mapping[str, object]],
        *,
        archive_record: Mapping[str, object] | None = None,
        api_batch_record: Mapping[str, object] | None = None,
        api_response_records: Sequence[Mapping[str, object]] = (),
    ) -> int:
        """Atomically append one validated historical source unit and its features."""

        if archive_record is not None and api_batch_record is not None:
            raise ValueError("a feature batch cannot have two historical provenance records")
        if api_response_records and api_batch_record is None:
            raise ValueError("API response provenance requires an API batch record")
        if not observations and archive_record is None and api_batch_record is None:
            return 0
        if archive_record is not None:
            provenance_kind = "historical_archive_replay"
            archive_id = str(archive_record["archive_id"])
            api_batch_id = None
        elif api_batch_record is not None:
            provenance_kind = "historical_api_replay"
            archive_id = None
            api_batch_id = str(api_batch_record["batch_id"])
        else:
            provenance_kind = "live_capture"
            archive_id = None
            api_batch_id = None
        self.flush()
        inserted = 0
        with self.connect() as connection:
            for item in observations:
                event_time = _utc(item["event_time"])  # type: ignore[arg-type]
                available_at = _utc(item["available_at"])  # type: ignore[arg-type]
                ingested_at = _utc(item["ingested_at"])  # type: ignore[arg-type]
                if not event_time <= available_at <= ingested_at:
                    raise ValueError("feature batch chronology invariant failed")
                name = str(item["name"])
                unit = str(item["unit"])
                definition = self.registry.require(name)
                if definition.unit != unit:
                    raise ValueError(f"unit mismatch for {name}: {unit} != {definition.unit}")
                symbol = str(item["symbol"]).strip().upper()
                source = str(item["source"])
                quality = float(item["quality"])
                value = float(item["value"])
                if not math.isfinite(value) or not 0 <= quality <= 1:
                    raise ValueError("feature batch value or quality is invalid")
                token = hashlib.sha256(
                    f"{item['event_id']}|{symbol}|{name}".encode()
                ).hexdigest()[:48]
                observation_id = f"bp_{token}"
                payload = {
                    "observation_id": observation_id,
                    "symbol": symbol,
                    "name": name,
                    "value": value,
                    "unit": unit,
                    "event_time": _iso(event_time),
                    "available_at": _iso(available_at),
                    "ingested_at": _iso(ingested_at),
                    "source": source,
                    "quality": quality,
                    "provenance_kind": provenance_kind,
                    "archive_id": archive_id,
                }
                if api_batch_id is not None:
                    payload["api_batch_id"] = api_batch_id
                digest = hashlib.sha256(_canonical(payload).encode()).hexdigest()
                row = connection.execute(
                    "SELECT payload_sha256 FROM bybit_feature_observations WHERE observation_id=?",
                    (observation_id,),
                ).fetchone()
                if row:
                    if row["payload_sha256"] != digest:
                        raise CaptureConflict(
                            "archive observation already exists with different content"
                        )
                    continue
                connection.execute(
                    """INSERT INTO bybit_feature_observations(
                           observation_id,symbol,name,value,unit,event_time,available_at,
                           ingested_at,source,quality,payload_sha256,provenance_kind,
                           archive_id,api_batch_id
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        observation_id,
                        symbol,
                        name,
                        value,
                        unit,
                        _iso(event_time),
                        _iso(available_at),
                        _iso(ingested_at),
                        source,
                        quality,
                        digest,
                        payload["provenance_kind"],
                        payload["archive_id"],
                        payload.get("api_batch_id"),
                    ),
                )
                inserted += 1
            if archive_record is not None:
                connection.execute(
                    """INSERT INTO bybit_historical_archive_files(
                           archive_id,data_kind,market,symbol,trading_date,source_url,
                           fetched_at,content_length,content_sha256,member_name,member_size,
                           first_event_time,last_event_time,rows_read,
                           feature_observation_count,status,error
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(data_kind,market,symbol,trading_date) DO UPDATE SET
                           archive_id=excluded.archive_id,
                           source_url=excluded.source_url,
                           fetched_at=excluded.fetched_at,
                           content_length=excluded.content_length,
                           content_sha256=excluded.content_sha256,
                           member_name=excluded.member_name,
                           member_size=excluded.member_size,
                           first_event_time=excluded.first_event_time,
                           last_event_time=excluded.last_event_time,
                           rows_read=excluded.rows_read,
                           feature_observation_count=CASE
                               WHEN excluded.feature_observation_count > 0
                               THEN excluded.feature_observation_count
                               ELSE bybit_historical_archive_files.feature_observation_count
                           END,
                           status=excluded.status,
                           error=excluded.error""",
                    (
                        archive_record["archive_id"],
                        archive_record["data_kind"],
                        archive_record["market"],
                        archive_record["symbol"],
                        archive_record["trading_date"],
                        archive_record["source_url"],
                        archive_record["fetched_at"],
                        int(archive_record["content_length"]),
                        archive_record["content_sha256"],
                        archive_record.get("member_name"),
                        archive_record.get("member_size"),
                        archive_record["first_event_time"],
                        archive_record["last_event_time"],
                        int(archive_record["rows_read"]),
                        inserted,
                        "completed",
                        None,
                    ),
                )
            if api_batch_record is not None:
                expected_response_count = int(api_batch_record["response_count"])
                if len(api_response_records) != expected_response_count:
                    raise ValueError("API response provenance count does not match batch")
                for response in api_response_records:
                    if str(response["batch_id"]) != str(api_batch_record["batch_id"]):
                        raise ValueError("API response references another batch")
                    connection.execute(
                        """INSERT INTO bybit_historical_api_responses(
                               response_id,batch_id,request_url,requested_at,received_at,
                               http_status,content_length,content_sha256,rows_read,ret_code
                           ) VALUES (?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(response_id) DO UPDATE SET
                               batch_id=excluded.batch_id,
                               request_url=excluded.request_url,
                               requested_at=excluded.requested_at,
                               received_at=excluded.received_at,
                               http_status=excluded.http_status,
                               content_length=excluded.content_length,
                               content_sha256=excluded.content_sha256,
                               rows_read=excluded.rows_read,
                               ret_code=excluded.ret_code""",
                        (
                            response["response_id"],
                            response["batch_id"],
                            response["request_url"],
                            response["requested_at"],
                            response["received_at"],
                            int(response["http_status"]),
                            int(response["content_length"]),
                            response["content_sha256"],
                            int(response["rows_read"]),
                            int(response["ret_code"]),
                        ),
                    )
                connection.execute(
                    """INSERT INTO bybit_historical_api_batches(
                           batch_id,data_kind,market,symbol,trading_date,endpoint_group,
                           requested_at,completed_at,first_event_time,last_event_time,
                           response_count,rows_read,feature_observation_count,
                           request_manifest_sha256,status,error
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(data_kind,market,symbol,trading_date) DO UPDATE SET
                           batch_id=excluded.batch_id,
                           endpoint_group=excluded.endpoint_group,
                           requested_at=excluded.requested_at,
                           completed_at=excluded.completed_at,
                           first_event_time=excluded.first_event_time,
                           last_event_time=excluded.last_event_time,
                           response_count=excluded.response_count,
                           rows_read=excluded.rows_read,
                           feature_observation_count=excluded.feature_observation_count,
                           request_manifest_sha256=excluded.request_manifest_sha256,
                           status=excluded.status,
                           error=excluded.error""",
                    (
                        api_batch_record["batch_id"],
                        api_batch_record["data_kind"],
                        api_batch_record["market"],
                        api_batch_record["symbol"],
                        api_batch_record["trading_date"],
                        api_batch_record["endpoint_group"],
                        api_batch_record["requested_at"],
                        api_batch_record["completed_at"],
                        api_batch_record.get("first_event_time"),
                        api_batch_record.get("last_event_time"),
                        expected_response_count,
                        int(api_batch_record["rows_read"]),
                        inserted,
                        api_batch_record["request_manifest_sha256"],
                        "completed",
                        None,
                    ),
                )
            connection.commit()
        return inserted

    def latest_features(
        self,
        symbol: str,
        names: Sequence[str],
        *,
        simulated_time: datetime,
    ) -> dict[str, dict[str, object]]:
        """Return the latest feature per name that was available at the cutoff."""

        cutoff = _iso(simulated_time)
        output: dict[str, dict[str, object]] = {}
        with closing(self.connect()) as connection:
            for name in names:
                row = connection.execute(
                    """SELECT * FROM bybit_feature_observations
                         WHERE symbol=? AND name=? AND available_at<=?
                           AND observation_id NOT IN (
                               SELECT observation_id FROM bybit_feature_invalidations
                           )
                         ORDER BY available_at DESC,sequence DESC LIMIT 1""",
                    (symbol.strip().upper(), name, cutoff),
                ).fetchone()
                if row:
                    output[name] = dict(row)
        return output


@dataclass
class _Book:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    update_id: int | None = None
    cross_sequence: int | None = None
    valid: bool = False
    imbalance_l5: float | None = None
    best_bid: float | None = None
    best_bid_size: float | None = None
    best_ask: float | None = None
    best_ask_size: float | None = None


class BybitPublicPITIngestor:
    """Validate Bybit public messages and derive only directly supported factors."""

    def __init__(
        self,
        store: BybitPublicPITStore,
        *,
        session_id: str,
        execution_probe_notional_usdt: float = 1_000.0,
        feature_emit_interval_sec: float = 0.0,
        raw_persist_interval_sec: float = 0.0,
        maximum_event_lag_sec: float = 10.0,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.execution_probe_notional_usdt = execution_probe_notional_usdt
        if (
            feature_emit_interval_sec < 0
            or raw_persist_interval_sec < 0
            or maximum_event_lag_sec <= 0
        ):
            raise ValueError("invalid public capture cadence or lag limit")
        self.feature_emit_interval_sec = float(feature_emit_interval_sec)
        self.raw_persist_interval_sec = float(raw_persist_interval_sec)
        self.maximum_event_lag_sec = float(maximum_event_lag_sec)
        self.books: dict[str, _Book] = defaultdict(_Book)
        self.trades: dict[str, Deque[tuple[datetime, float, float, str]]] = defaultdict(deque)
        self.liquidations: dict[str, Deque[tuple[datetime, float, str]]] = defaultdict(deque)
        self.open_interest: dict[str, Deque[tuple[datetime, float]]] = defaultdict(deque)
        self.order_flow_imbalance: dict[str, Deque[tuple[datetime, float]]] = defaultdict(deque)
        self.last_feature_emit_at: dict[tuple[str, str], datetime] = {}
        self.last_raw_persist_at: dict[tuple[str, str], datetime] = {}
        self.recent_trade_ids: dict[str, set[str]] = defaultdict(set)
        self.recent_trade_id_times: dict[str, Deque[tuple[datetime, str]]] = defaultdict(deque)

    def _validate_event_lag(
        self,
        event_time: datetime,
        received_at: datetime,
        source: str,
    ) -> None:
        lag = (_utc(received_at) - _utc(event_time)).total_seconds()
        if lag > self.maximum_event_lag_sec or lag < -2.0:
            self.store.flush()
            self.store.quality_store.source_event(
                source,
                "outage",
                f"public_stream_event_lag_sec={lag:.3f}",
                _utc(received_at),
            )
            raise StalePublicEvent(
                f"{source} event lag {lag:.3f}s exceeds capture contract"
            )

    def _should_emit(
        self,
        symbol: str,
        group: str,
        received_at: datetime,
        *,
        force: bool = False,
    ) -> bool:
        key = (symbol, group)
        previous = self.last_feature_emit_at.get(key)
        if (
            not force
            and previous is not None
            and (received_at - previous).total_seconds()
            < self.feature_emit_interval_sec
        ):
            return False
        self.last_feature_emit_at[key] = received_at
        return True

    def _should_persist_raw(
        self,
        symbol: str,
        group: str,
        received_at: datetime,
        *,
        force: bool = False,
    ) -> bool:
        key = (symbol, group)
        previous = self.last_raw_persist_at.get(key)
        if (
            not force
            and previous is not None
            and (received_at - previous).total_seconds()
            < self.raw_persist_interval_sec
        ):
            return False
        self.last_raw_persist_at[key] = received_at
        return True

    def invalidate_books(self, reason: str, received_at: datetime) -> None:
        for book in self.books.values():
            book.valid = False
        self.store.flush()
        self.store.quality_store.source_event(
            "bybit.public.orderbook", "outage", reason, _utc(received_at)
        )

    @staticmethod
    def _apply_levels(target: dict[float, float], levels: Sequence[Sequence[Any]]) -> None:
        for level in levels:
            if len(level) < 2:
                continue
            price = float(level[0])
            size = float(level[1])
            if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size < 0:
                raise ValueError("invalid orderbook level")
            if size == 0:
                target.pop(price, None)
            else:
                target[price] = size

    def _feature(
        self,
        event_id: str,
        symbol: str,
        name: str,
        value: float,
        unit: str,
        event_time: datetime,
        received_at: datetime,
        source: str,
        quality: float = 0.98,
    ) -> None:
        self.store.append_feature(
            event_id=event_id,
            symbol=symbol,
            name=name,
            value=value,
            unit=unit,
            event_time=event_time,
            received_at=received_at,
            source=source,
            quality=quality,
        )

    def ingest(
        self,
        message: Mapping[str, Any],
        *,
        received_at: datetime,
    ) -> dict[str, object]:
        received_at = _utc(received_at)
        topic = str(message.get("topic") or "")
        if topic.startswith("orderbook."):
            return self._orderbook(message, received_at)
        if topic.startswith("publicTrade."):
            return self._public_trades(message, received_at)
        if topic.startswith("allLiquidation."):
            return self._liquidations(message, received_at)
        if topic.startswith("tickers."):
            return self._ticker(message, received_at)
        return {"status": "ignored", "topic": topic}

    def _orderbook(
        self, message: Mapping[str, Any], received_at: datetime
    ) -> dict[str, object]:
        topic = str(message["topic"])
        data = dict(message.get("data") or {})
        symbol = str(data.get("s") or topic.rsplit(".", 1)[-1]).upper()
        update_id = int(data.get("u") or 0)
        cross_sequence = int(data.get("seq") or 0)
        event_time = _from_ms(message.get("cts") or message.get("ts"))
        self._validate_event_lag(
            event_time, received_at, "bybit.public.orderbook"
        )
        event_type = str(message.get("type") or "delta").lower()
        payload_hash = hashlib.sha256(_canonical(data).encode()).hexdigest()[:24]
        event_id = f"book:{symbol}:{update_id}:{payload_hash}"
        book = self.books[symbol]
        is_snapshot = event_type == "snapshot" or update_id == 1
        if (
            not is_snapshot
            and book.update_id == update_id
            and book.cross_sequence == cross_sequence
        ):
            return {"status": "duplicate_memory", "event_id": event_id}
        sequence_regressed = (
            book.cross_sequence is not None and cross_sequence < book.cross_sequence
        )
        raw_persisted = self._should_persist_raw(
            symbol, "orderbook", received_at, force=is_snapshot
        )
        if raw_persisted:
            accepted = self.store.append_raw(
                event_id=event_id,
                session_id=self.session_id,
                topic=topic,
                symbol=symbol,
                event_type=event_type,
                exchange_time=event_time,
                received_at=received_at,
                update_id=update_id,
                cross_sequence=cross_sequence,
                book_state_valid=book.valid or is_snapshot,
                payload=data,
            )
            if not accepted:
                return {"status": "duplicate", "event_id": event_id}
        if sequence_regressed and not is_snapshot:
            book.valid = False
            self.store.flush()
            self.store.quality_store.source_event(
                "bybit.public.orderbook",
                "degraded",
                "cross_sequence_regressed_waiting_for_snapshot",
                received_at,
            )
            return {"status": "invalid_sequence", "event_id": event_id}
        if is_snapshot:
            book.bids.clear()
            book.asks.clear()
            book.valid = True
            book.best_bid = None
            book.best_bid_size = None
            book.best_ask = None
            book.best_ask_size = None
            book.imbalance_l5 = None
            self.order_flow_imbalance[symbol].clear()
            self.store.flush()
            self.store.quality_store.source_event(
                "bybit.public.orderbook", "ok", "snapshot_recovered", received_at
            )
        elif not book.valid:
            return {"status": "waiting_for_snapshot", "event_id": event_id}
        self._apply_levels(book.bids, data.get("b") or [])
        self._apply_levels(book.asks, data.get("a") or [])
        book.update_id = update_id
        book.cross_sequence = cross_sequence
        if not book.bids or not book.asks:
            book.valid = False
            return {"status": "empty_book", "event_id": event_id}
        bids = sorted(book.bids.items(), reverse=True)[:5]
        asks = sorted(book.asks.items())[:5]
        best_bid, best_bid_size = bids[0]
        best_ask, best_ask_size = asks[0]
        if best_ask <= best_bid:
            book.valid = False
            return {"status": "crossed_book", "event_id": event_id}
        midpoint = (best_bid + best_ask) / 2.0
        bid_depth_usdt = sum(price * size for price, size in bids)
        ask_depth_usdt = sum(price * size for price, size in asks)
        total_depth = bid_depth_usdt + ask_depth_usdt
        imbalance = (
            (bid_depth_usdt - ask_depth_usdt) / total_depth if total_depth else 0.0
        )
        delta = imbalance - (book.imbalance_l5 if book.imbalance_l5 is not None else imbalance)
        book.imbalance_l5 = imbalance
        ofi_window = self.order_flow_imbalance[symbol]
        if all(
            value is not None
            for value in (
                book.best_bid,
                book.best_bid_size,
                book.best_ask,
                book.best_ask_size,
            )
        ):
            # Cont-style best-level OFI: bid additions/price improvements are
            # positive, ask additions/price deterioration are negative. This
            # is book event flow, deliberately distinct from trade CVD.
            contribution = (
                (best_bid_size if best_bid >= book.best_bid else 0.0)
                - (book.best_bid_size if best_bid <= book.best_bid else 0.0)
                - (best_ask_size if best_ask <= book.best_ask else 0.0)
                + (book.best_ask_size if best_ask >= book.best_ask else 0.0)
            )
            ofi_window.append((received_at, float(contribution)))
        book.best_bid = best_bid
        book.best_bid_size = best_bid_size
        book.best_ask = best_ask
        book.best_ask_size = best_ask_size
        ofi_cutoff = received_at - timedelta(minutes=1)
        while ofi_window and ofi_window[0][0] < ofi_cutoff:
            ofi_window.popleft()
        microprice = (
            best_ask * best_bid_size + best_bid * best_ask_size
        ) / max(best_bid_size + best_ask_size, 1e-12)

        def sweep(levels: Sequence[tuple[float, float]]) -> tuple[float, float]:
            remaining = self.execution_probe_notional_usdt
            filled = 0.0
            quantity = 0.0
            for price, size in levels:
                take = min(remaining, price * size)
                filled += take
                quantity += take / price
                remaining -= take
                if remaining <= 1e-9:
                    break
            return filled / self.execution_probe_notional_usdt, filled / quantity if quantity else midpoint

        ask_fill, ask_vwap = sweep(asks)
        bid_fill, bid_vwap = sweep(bids)
        fill_probability = min(ask_fill, bid_fill)
        slippage_bps = max(
            0.0,
            ((ask_vwap - midpoint) / midpoint + (midpoint - bid_vwap) / midpoint)
            * 5_000.0,
        )
        factors = {
            "orderbook_spread_bps": ((best_ask - best_bid) / midpoint * 10_000, "bps"),
            "bybit_orderbook_delta_l5": (delta, "ratio"),
            "ofi_1m": (sum(value for _, value in ofi_window), "base_asset"),
            "orderbook_imbalance_l5": (imbalance, "ratio"),
            "orderbook_depth_usdt_l5": (total_depth, "usd"),
            "microprice_deviation_bps": ((microprice - midpoint) / midpoint * 10_000, "bps"),
            "fill_probability": (fill_probability, "probability"),
            "expected_slippage_bps": (slippage_bps, "bps"),
        }
        emitted = self._should_emit(
            symbol,
            "orderbook",
            received_at,
            force=is_snapshot,
        )
        if emitted:
            for name, (value, unit) in factors.items():
                self._feature(
                    event_id,
                    symbol,
                    name,
                    value,
                    unit,
                    event_time,
                    received_at,
                    "bybit.public.orderbook",
                )
        return {
            "status": "accepted",
            "event_id": event_id,
            "features": factors if emitted else {},
            "features_emitted": emitted,
            "raw_persisted": raw_persisted,
        }

    def _public_trades(
        self, message: Mapping[str, Any], received_at: datetime
    ) -> dict[str, object]:
        topic = str(message["topic"])
        accepted = 0
        latest_event: datetime | None = None
        latest_id = ""
        symbol = topic.rsplit(".", 1)[-1].upper()
        persist_message = self._should_persist_raw(
            symbol, "trades", received_at
        )
        for item in list(message.get("data") or []):
            row = dict(item)
            symbol = str(row.get("s") or symbol).upper()
            trade_id = str(row.get("i") or hashlib.sha256(_canonical(row).encode()).hexdigest())
            trade_id_cutoff = received_at - timedelta(minutes=2)
            id_times = self.recent_trade_id_times[symbol]
            while id_times and id_times[0][0] < trade_id_cutoff:
                _, expired_id = id_times.popleft()
                self.recent_trade_ids[symbol].discard(expired_id)
            if trade_id in self.recent_trade_ids[symbol]:
                continue
            self.recent_trade_ids[symbol].add(trade_id)
            id_times.append((received_at, trade_id))
            event_id = f"trade:{symbol}:{trade_id}"
            event_time = _from_ms(row.get("T") or message.get("ts"))
            self._validate_event_lag(
                event_time, received_at, "bybit.public.trades"
            )
            if persist_message:
                if not self.store.append_raw(
                    event_id=event_id,
                    session_id=self.session_id,
                    topic=topic,
                    symbol=symbol,
                    event_type="trade",
                    exchange_time=event_time,
                    received_at=received_at,
                    payload=row,
                    cross_sequence=int(row.get("seq") or 0),
                ):
                    continue
            accepted += 1
            latest_event = max(latest_event or event_time, event_time)
            latest_id = event_id
            self.trades[symbol].append(
                (received_at, float(row["p"]), float(row["v"]), str(row["S"]).lower())
            )
        cutoff = received_at - timedelta(minutes=1)
        while self.trades[symbol] and self.trades[symbol][0][0] < cutoff:
            self.trades[symbol].popleft()
        if not accepted or latest_event is None:
            return {"status": "duplicate", "accepted": 0}
        buy = sum(size for _, _, size, side in self.trades[symbol] if side == "buy")
        sell = sum(size for _, _, size, side in self.trades[symbol] if side == "sell")
        total = buy + sell
        factors = {
            "public_trade_imbalance_1m": ((buy - sell) / total if total else 0.0, "ratio"),
            "aggressive_cvd_1m": (buy - sell, "base_asset"),
        }
        emitted = self._should_emit(symbol, "trades", received_at)
        if emitted:
            for name, (value, unit) in factors.items():
                self._feature(
                    latest_id,
                    symbol,
                    name,
                    value,
                    unit,
                    latest_event,
                    received_at,
                    "bybit.public.trades",
                )
        return {
            "status": "accepted",
            "accepted": accepted,
            "features": factors if emitted else {},
            "features_emitted": emitted,
            "raw_persisted": persist_message,
        }

    def _liquidations(
        self, message: Mapping[str, Any], received_at: datetime
    ) -> dict[str, object]:
        topic = str(message["topic"])
        accepted = 0
        latest_event: datetime | None = None
        latest_id = ""
        symbol = topic.rsplit(".", 1)[-1].upper()
        for item in list(message.get("data") or []):
            row = dict(item)
            symbol = str(row.get("s") or symbol).upper()
            event_time = _from_ms(row.get("T") or message.get("ts"))
            self._validate_event_lag(
                event_time, received_at, "bybit.public.liquidations"
            )
            digest = hashlib.sha256(_canonical(row).encode()).hexdigest()[:40]
            event_id = f"liq:{symbol}:{digest}"
            if not self.store.append_raw(
                event_id=event_id,
                session_id=self.session_id,
                topic=topic,
                symbol=symbol,
                event_type="liquidation",
                exchange_time=event_time,
                received_at=received_at,
                payload=row,
            ):
                continue
            accepted += 1
            latest_event = max(latest_event or event_time, event_time)
            latest_id = event_id
            notional = float(row["p"]) * float(row["v"])
            # Bybit's allLiquidation contract is position-side, not taker-side:
            # S=Buy means a long was liquidated; S=Sell means a short.
            position = "long" if str(row["S"]).lower() == "buy" else "short"
            self.liquidations[symbol].append((received_at, notional, position))
        cutoff = received_at - timedelta(minutes=5)
        while self.liquidations[symbol] and self.liquidations[symbol][0][0] < cutoff:
            self.liquidations[symbol].popleft()
        if not accepted or latest_event is None:
            return {"status": "duplicate", "accepted": 0}
        long_value = sum(value for _, value, side in self.liquidations[symbol] if side == "long")
        short_value = sum(value for _, value, side in self.liquidations[symbol] if side == "short")
        total = long_value + short_value
        imbalance = (short_value - long_value) / total if total else 0.0
        self._feature(
            f"{latest_id}:bybit-liquidation-side-v2",
            symbol,
            "liquidation_imbalance_5m",
            imbalance,
            "ratio",
            latest_event,
            received_at,
            "bybit.public.liquidations.v2",
        )
        return {"status": "accepted", "accepted": accepted, "imbalance": imbalance}

    def _ticker(
        self, message: Mapping[str, Any], received_at: datetime
    ) -> dict[str, object]:
        topic = str(message["topic"])
        data = dict(message.get("data") or {})
        symbol = str(data.get("symbol") or topic.rsplit(".", 1)[-1]).upper()
        event_time = _from_ms(message.get("ts"))
        self._validate_event_lag(event_time, received_at, "bybit.public.ticker")
        digest = hashlib.sha256(_canonical(data).encode()).hexdigest()[:40]
        event_id = f"ticker:{symbol}:{int(message.get('ts') or 0)}:{digest}"
        raw_persisted = self._should_persist_raw(symbol, "ticker", received_at)
        if raw_persisted:
            if not self.store.append_raw(
                event_id=event_id,
                session_id=self.session_id,
                topic=topic,
                symbol=symbol,
                event_type="ticker",
                exchange_time=event_time,
                received_at=received_at,
                payload=data,
            ):
                return {"status": "duplicate"}
        factors: dict[str, tuple[float, str]] = {}
        if data.get("markPrice") and data.get("indexPrice"):
            mark = float(data["markPrice"])
            index = float(data["indexPrice"])
            factors["perpetual_basis_bps"] = ((mark - index) / index * 10_000, "bps")
        if data.get("fundingRate") not in (None, ""):
            factors["funding_rate"] = (float(data["fundingRate"]), "ratio")
        if data.get("openInterest") not in (None, ""):
            current = float(data["openInterest"])
            history = self.open_interest[symbol]
            history.append((received_at, current))
            cutoff = received_at - timedelta(hours=1)
            while len(history) > 1 and history[1][0] <= cutoff:
                history.popleft()
            if history and history[0][0] <= cutoff and history[0][1] > 0:
                factors["open_interest_change_1h"] = (
                    (current - history[0][1]) / history[0][1],
                    "ratio",
                )
        emitted = self._should_emit(symbol, "ticker", received_at)
        if emitted:
            for name, (value, unit) in factors.items():
                self._feature(
                    event_id,
                    symbol,
                    name,
                    value,
                    unit,
                    event_time,
                    received_at,
                    "bybit.public.ticker",
                )
        return {
            "status": "accepted",
            "features": factors if emitted else {},
            "features_emitted": emitted,
            "raw_persisted": raw_persisted,
        }


class BybitPublicPITCollector:
    """Reconnectable public-only WebSocket capture; never authenticates or trades."""

    def __init__(
        self,
        store: BybitPublicPITStore,
        symbols: Sequence[str],
        *,
        endpoint: str = BYBIT_PUBLIC_LINEAR_WS,
        orderbook_depth: int = 50,
        feature_emit_interval_sec: float = 5.0,
        raw_persist_interval_sec: float = 5.0,
        maximum_event_lag_sec: float = 10.0,
    ) -> None:
        self.store = store
        self.symbols = tuple(sorted({str(symbol).upper() for symbol in symbols}))
        self.endpoint = endpoint
        self.orderbook_depth = orderbook_depth
        self.feature_emit_interval_sec = feature_emit_interval_sec
        self.raw_persist_interval_sec = raw_persist_interval_sec
        self.maximum_event_lag_sec = maximum_event_lag_sec
        if not self.symbols:
            raise ValueError("at least one Bybit symbol is required")
        if orderbook_depth not in {1, 50, 200, 1000}:
            raise ValueError("unsupported Bybit linear orderbook depth")

    @property
    def topics(self) -> list[str]:
        output = []
        for symbol in self.symbols:
            output.extend(
                [
                    f"orderbook.{self.orderbook_depth}.{symbol}",
                    f"publicTrade.{symbol}",
                    f"allLiquidation.{symbol}",
                    f"tickers.{symbol}",
                ]
            )
        return output

    async def run_once(self) -> None:
        import aiohttp

        session_id = f"bp_{uuid.uuid4().hex}"
        started_at = datetime.now(timezone.utc)
        self.store.start_session(
            session_id,
            endpoint=self.endpoint,
            symbols=self.symbols,
            started_at=started_at,
        )
        ingestor = BybitPublicPITIngestor(
            self.store,
            session_id=session_id,
            feature_emit_interval_sec=self.feature_emit_interval_sec,
            raw_persist_interval_sec=self.raw_persist_interval_sec,
            maximum_event_lag_sec=self.maximum_event_lag_sec,
        )
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.ws_connect(self.endpoint, heartbeat=20) as websocket:
                    await websocket.send_json({"op": "subscribe", "args": self.topics})
                    async for item in websocket:
                        received_at = datetime.now(timezone.utc)
                        if item.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(item.data)
                            if payload.get("topic"):
                                ingestor.ingest(payload, received_at=received_at)
                        elif item.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            raise ConnectionError(f"Bybit public WebSocket ended: {item.type}")
            self.store.end_session(
                session_id,
                ended_at=datetime.now(timezone.utc),
                status="completed",
            )
        except asyncio.CancelledError:
            self.store.end_session(
                session_id,
                ended_at=datetime.now(timezone.utc),
                status="completed",
            )
            raise
        except Exception as exc:
            ingestor.invalidate_books(
                f"websocket_disconnect:{type(exc).__name__}", datetime.now(timezone.utc)
            )
            self.store.end_session(
                session_id,
                ended_at=datetime.now(timezone.utc),
                status="disconnected",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

    async def run_forever(self, *, maximum_backoff_sec: float = 60.0) -> None:
        backoff = 1.0
        while True:
            try:
                await self.run_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(maximum_backoff_sec, backoff * 2.0)


__all__ = (
    "BYBIT_PUBLIC_LINEAR_WS",
    "BybitPublicPITCollector",
    "BybitPublicPITIngestor",
    "BybitPublicPITStore",
    "CaptureConflict",
    "StalePublicEvent",
)
