"""Durable predictor publication outbox and idempotent publisher worker."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator

from contracts.forecast_v1 import ForecastEnvelope
from contracts.operation_ticket_v1 import OperationTicket
from contracts.portfolio_intent_v1 import PortfolioIntent
from core.control_plane import ControlPlaneRepository


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def canonical(payload: dict) -> tuple[str, str]:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


class PublicationConflict(RuntimeError):
    pass


class PublicationCapacityError(RuntimeError):
    pass


@dataclass(frozen=True)
class OutboxLimits:
    max_pending: int = 100_000
    max_bytes: int = 2 * 1024 * 1024 * 1024
    max_oldest_age_seconds: int = 7 * 24 * 3600
    min_disk_free_bytes: int = 1024 * 1024 * 1024


class ForecastPublicationOutbox:
    def __init__(self, db_path: Path, *, limits: OutboxLimits | None = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.limits = limits or OutboxLimits()
        self.migrate()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def migrate(self) -> None:
        with self.transaction(immediate=True) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS forecast_publication_outbox(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    publication_id TEXT NOT NULL UNIQUE,
                    forecast_json TEXT NOT NULL,
                    ticket_json TEXT,
                    portfolio_intent_json TEXT,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT,
                    acknowledged_at TEXT,
                    archived_at TEXT,
                    archive_reason TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_forecast_publication_due
                    ON forecast_publication_outbox(
                        acknowledged_at, archived_at, next_attempt_at, sequence
                    );
                """
            )

    @staticmethod
    def publication_id(forecast: ForecastEnvelope) -> str:
        return f"forecast:{forecast.forecast_id}:{forecast.revision}"

    def _guard_capacity(self, connection: sqlite3.Connection, *, now: datetime) -> None:
        row = connection.execute(
            """SELECT COUNT(*) AS depth, MIN(created_at) AS oldest
               FROM forecast_publication_outbox
               WHERE acknowledged_at IS NULL AND archived_at IS NULL"""
        ).fetchone()
        depth = int(row["depth"] or 0)
        if depth >= self.limits.max_pending:
            raise PublicationCapacityError("publication outbox pending-count limit reached")
        if row["oldest"]:
            age = (now - parse_time(row["oldest"])).total_seconds()
            if age > self.limits.max_oldest_age_seconds:
                raise PublicationCapacityError("publication outbox oldest-item age limit reached")
        database_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        if database_bytes >= self.limits.max_bytes:
            raise PublicationCapacityError("publication outbox database-size limit reached")
        disk_free = shutil.disk_usage(self.db_path.parent).free
        if disk_free < self.limits.min_disk_free_bytes:
            raise PublicationCapacityError("publication outbox disk-free floor reached")

    def enqueue(
        self,
        forecast: ForecastEnvelope,
        ticket: OperationTicket | None = None,
        portfolio_intent: PortfolioIntent | None = None,
        *,
        now: datetime | None = None,
    ) -> bool:
        point = (now or utc_now()).astimezone(timezone.utc)
        payload = {
            "forecast": forecast.model_dump(mode="json"),
            "ticket": ticket.model_dump(mode="json") if ticket is not None else None,
            "portfolio_intent": (
                portfolio_intent.model_dump(mode="json")
                if portfolio_intent is not None
                else None
            ),
        }
        _, payload_hash = canonical(payload)
        forecast_json = json.dumps(payload["forecast"], ensure_ascii=False, sort_keys=True)
        ticket_json = (
            json.dumps(payload["ticket"], ensure_ascii=False, sort_keys=True)
            if payload["ticket"] is not None
            else None
        )
        intent_json = (
            json.dumps(payload["portfolio_intent"], ensure_ascii=False, sort_keys=True)
            if payload["portfolio_intent"] is not None
            else None
        )
        publication_id = self.publication_id(forecast)
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                """SELECT payload_sha256 FROM forecast_publication_outbox
                   WHERE publication_id=?""",
                (publication_id,),
            ).fetchone()
            if existing:
                if existing["payload_sha256"] != payload_hash:
                    raise PublicationConflict(
                        "forecast publication id already exists with different content"
                    )
                return False
            self._guard_capacity(connection, now=point)
            connection.execute(
                """INSERT INTO forecast_publication_outbox(
                    publication_id, forecast_json, ticket_json, portfolio_intent_json,
                    payload_sha256, created_at, next_attempt_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    publication_id,
                    forecast_json,
                    ticket_json,
                    intent_json,
                    payload_hash,
                    iso(point),
                    iso(point),
                ),
            )
            return True

    def pending(
        self, *, limit: int = 100, now: datetime | None = None
    ) -> list[dict]:
        point = (now or utc_now()).astimezone(timezone.utc)
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM forecast_publication_outbox
                   WHERE acknowledged_at IS NULL AND archived_at IS NULL
                     AND next_attempt_at<=?
                   ORDER BY sequence LIMIT ?""",
                (iso(point), max(1, min(int(limit), 500))),
            ).fetchall()
        return [dict(row) for row in rows]

    def acknowledge(self, publication_id: str, *, now: datetime | None = None) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE forecast_publication_outbox
                   SET acknowledged_at=?, last_error=NULL WHERE publication_id=?""",
                (iso(now or utc_now()), publication_id),
            )

    def archive(
        self, publication_id: str, reason: str, *, now: datetime | None = None
    ) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE forecast_publication_outbox
                   SET archived_at=?, archive_reason=?, last_error=NULL
                   WHERE publication_id=?""",
                (iso(now or utc_now()), reason, publication_id),
            )

    def retry(
        self,
        publication_id: str,
        error: Exception | str,
        *,
        now: datetime | None = None,
    ) -> None:
        point = (now or utc_now()).astimezone(timezone.utc)
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """SELECT retry_count FROM forecast_publication_outbox
                   WHERE publication_id=?""",
                (publication_id,),
            ).fetchone()
            if not row:
                raise KeyError(publication_id)
            retry_count = int(row["retry_count"] or 0) + 1
            delay_seconds = min(3600, 2 ** min(retry_count, 12))
            connection.execute(
                """UPDATE forecast_publication_outbox
                   SET retry_count=?, next_attempt_at=?, last_error=?
                   WHERE publication_id=?""",
                (
                    retry_count,
                    iso(point + timedelta(seconds=delay_seconds)),
                    f"{type(error).__name__}: {error}"[:1000]
                    if isinstance(error, Exception)
                    else str(error)[:1000],
                    publication_id,
                ),
            )

    def metrics(self, *, now: datetime | None = None) -> dict:
        point = (now or utc_now()).astimezone(timezone.utc)
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT
                    SUM(CASE WHEN acknowledged_at IS NULL AND archived_at IS NULL THEN 1 ELSE 0 END)
                        AS pending,
                    SUM(CASE WHEN acknowledged_at IS NOT NULL THEN 1 ELSE 0 END) AS acknowledged,
                    SUM(CASE WHEN archived_at IS NOT NULL THEN 1 ELSE 0 END) AS archived,
                    MIN(CASE WHEN acknowledged_at IS NULL AND archived_at IS NULL
                        THEN created_at END) AS oldest
                   FROM forecast_publication_outbox"""
            ).fetchone()
        oldest_age = (
            max(0.0, (point - parse_time(row["oldest"])).total_seconds())
            if row["oldest"]
            else 0.0
        )
        database_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        disk_free = shutil.disk_usage(self.db_path.parent).free
        pending = int(row["pending"] or 0)
        healthy = (
            pending < self.limits.max_pending
            and database_bytes < self.limits.max_bytes
            and oldest_age <= self.limits.max_oldest_age_seconds
            and disk_free >= self.limits.min_disk_free_bytes
        )
        return {
            "healthy": healthy,
            "pending": pending,
            "acknowledged": int(row["acknowledged"] or 0),
            "archived": int(row["archived"] or 0),
            "oldest_pending_age_seconds": oldest_age,
            "database_bytes": database_bytes,
            "disk_free_bytes": disk_free,
        }


class PublicationWorker:
    def __init__(
        self,
        outbox: ForecastPublicationOutbox,
        control_plane: ControlPlaneRepository | Callable[[], ControlPlaneRepository],
    ):
        self.outbox = outbox
        self._control_plane = control_plane

    def _repository(self) -> ControlPlaneRepository:
        return self._control_plane() if callable(self._control_plane) else self._control_plane

    def run_once(self, *, limit: int = 100, now: datetime | None = None) -> dict[str, int]:
        point = (now or utc_now()).astimezone(timezone.utc)
        result = {"acknowledged": 0, "archived_expired": 0, "retried": 0}
        for item in self.outbox.pending(limit=limit, now=point):
            publication_id = str(item["publication_id"])
            try:
                forecast = ForecastEnvelope.model_validate_json(item["forecast_json"])
                ticket = (
                    OperationTicket.model_validate_json(item["ticket_json"])
                    if item["ticket_json"]
                    else None
                )
                intent = (
                    PortfolioIntent.model_validate_json(item["portfolio_intent_json"])
                    if item["portfolio_intent_json"]
                    else None
                )
                repository = self._repository()
                if ticket is not None and ticket.expires_at <= point:
                    repository.publish(forecast, None)
                    self.outbox.archive(publication_id, "ticket_expired_before_publication", now=point)
                    result["archived_expired"] += 1
                    continue
                repository.publish(forecast, ticket, intent)
                self.outbox.acknowledge(publication_id, now=point)
                result["acknowledged"] += 1
            except Exception as exc:
                self.outbox.retry(publication_id, exc, now=point)
                result["retried"] += 1
        return result
