from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from contracts.execution_receipt_v1 import ExecutionReceipt
from contracts.forecast_v1 import ForecastEnvelope
from contracts.operation_ticket_v1 import OperationTicket
from contracts.portfolio_intent_v1 import PortfolioIntent


SCHEMA_VERSION = 2
SCHEMA_CHECKSUM = hashlib.sha256(
    b"control-plane:v2:portfolio-intents:claim-epoch:immutable-outbox"
).hexdigest()
CODE_COMMIT = os.environ.get("APP_CODE_COMMIT", "workspace-uncommitted")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(payload: dict[str, Any]) -> tuple[str, str]:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


class ImmutableConflict(RuntimeError):
    pass


class ControlPlaneRepository:
    """SQLite WAL implementation behind the prediction control-plane boundary."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._migration_lock = threading.Lock()
        self.migrate()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
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
        with self._migration_lock, self.transaction(immediate=True) as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )"""
            )
            current = connection.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
            ).fetchone()["version"]
            if int(current) > SCHEMA_VERSION:
                raise RuntimeError(
                    f"control-plane DB schema {current} is newer than supported {SCHEMA_VERSION}"
                )
            migration_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(schema_migrations)")
            }
            for name, declaration in (
                ("migration_id", "TEXT"),
                ("code_commit", "TEXT"),
                ("schema_checksum", "TEXT"),
            ):
                if name not in migration_columns:
                    connection.execute(
                        f"ALTER TABLE schema_migrations ADD COLUMN {name} {declaration}"
                    )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS forecasts (
                    forecast_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    PRIMARY KEY (forecast_id, revision)
                );
                CREATE TABLE IF NOT EXISTS operation_tickets (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT NOT NULL UNIQUE,
                    forecast_id TEXT NOT NULL,
                    forecast_revision INTEGER NOT NULL,
                    supersedes_ticket_id TEXT,
                    symbol TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    FOREIGN KEY (forecast_id, forecast_revision)
                        REFERENCES forecasts(forecast_id, revision)
                );
                CREATE INDEX IF NOT EXISTS idx_tickets_cursor ON operation_tickets(sequence);
                CREATE INDEX IF NOT EXISTS idx_tickets_symbol ON operation_tickets(symbol, sequence);
                CREATE TABLE IF NOT EXISTS portfolio_intents (
                    portfolio_decision_id TEXT PRIMARY KEY,
                    strategy_release_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    decision_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    UNIQUE(symbol, decision_version)
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_intents_symbol
                    ON portfolio_intents(symbol, decision_version);
                CREATE TABLE IF NOT EXISTS ticket_delivery_outbox (
                    sequence INTEGER PRIMARY KEY,
                    ticket_id TEXT NOT NULL UNIQUE,
                    published_at TEXT NOT NULL,
                    FOREIGN KEY (sequence) REFERENCES operation_tickets(sequence),
                    FOREIGN KEY (ticket_id) REFERENCES operation_tickets(ticket_id)
                );
                CREATE TABLE IF NOT EXISTS ticket_claims (
                    ticket_id TEXT PRIMARY KEY,
                    consumer_id TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    claimed_at TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    claim_epoch INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (ticket_id) REFERENCES operation_tickets(ticket_id)
                );
                CREATE TABLE IF NOT EXISTS execution_receipts (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_id TEXT NOT NULL UNIQUE,
                    ticket_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    FOREIGN KEY (ticket_id) REFERENCES operation_tickets(ticket_id)
                );
                CREATE INDEX IF NOT EXISTS idx_receipts_ticket ON execution_receipts(ticket_id, sequence);
                CREATE TABLE IF NOT EXISTS ticket_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    ticket_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    FOREIGN KEY (ticket_id) REFERENCES operation_tickets(ticket_id)
                );
                CREATE INDEX IF NOT EXISTS idx_control_ticket_events
                    ON ticket_events(ticket_id, sequence);
                """
            )
            claim_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(ticket_claims)")
            }
            if "claim_epoch" not in claim_columns:
                connection.execute(
                    "ALTER TABLE ticket_claims ADD COLUMN claim_epoch INTEGER NOT NULL DEFAULT 0"
                )
            connection.execute(
                """INSERT OR IGNORE INTO schema_migrations(
                    version, applied_at, migration_id, code_commit, schema_checksum
                ) VALUES (?, ?, ?, ?, ?)""",
                (
                    SCHEMA_VERSION,
                    _iso(_utc_now()),
                    "control-plane-v2",
                    CODE_COMMIT,
                    SCHEMA_CHECKSUM,
                ),
            )
            recorded = connection.execute(
                "SELECT schema_checksum FROM schema_migrations WHERE version=?",
                (SCHEMA_VERSION,),
            ).fetchone()
            if not recorded or recorded["schema_checksum"] != SCHEMA_CHECKSUM:
                raise RuntimeError("control-plane DB schema checksum does not match this build")

    def publish(
        self,
        forecast: ForecastEnvelope,
        ticket: OperationTicket | None,
        portfolio_intent: PortfolioIntent | None = None,
    ) -> bool:
        forecast_data = forecast.model_dump(mode="json")
        forecast_json, forecast_hash = _canonical(forecast_data)
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM forecasts WHERE forecast_id=? AND revision=?",
                (forecast.forecast_id, forecast.revision),
            ).fetchone()
            if existing and existing["payload_sha256"] != forecast_hash:
                raise ImmutableConflict("forecast id/revision already exists with different content")
            if not existing:
                connection.execute(
                    "INSERT INTO forecasts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        forecast.forecast_id,
                        forecast.revision,
                        forecast.instrument.symbol,
                        _iso(forecast.time.created_at),
                        forecast_json,
                        forecast_hash,
                    ),
                )
            if ticket is None:
                return False
            if portfolio_intent is not None:
                if ticket.portfolio_decision_id != portfolio_intent.portfolio_decision_id:
                    raise ImmutableConflict("ticket does not reference the supplied portfolio intent")
                intent_json, intent_hash = _canonical(portfolio_intent.model_dump(mode="json"))
                existing_intent = connection.execute(
                    "SELECT payload_sha256 FROM portfolio_intents WHERE portfolio_decision_id=?",
                    (portfolio_intent.portfolio_decision_id,),
                ).fetchone()
                if existing_intent and existing_intent["payload_sha256"] != intent_hash:
                    raise ImmutableConflict(
                        "portfolio_decision_id already exists with different content"
                    )
                if not existing_intent:
                    connection.execute(
                        """INSERT INTO portfolio_intents(
                            portfolio_decision_id, strategy_release_id, symbol,
                            decision_version, created_at, valid_until, payload_json, payload_sha256
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            portfolio_intent.portfolio_decision_id,
                            portfolio_intent.strategy_release_id,
                            portfolio_intent.symbol,
                            portfolio_intent.decision_version,
                            _iso(portfolio_intent.created_at),
                            _iso(portfolio_intent.valid_until),
                            intent_json,
                            intent_hash,
                        ),
                    )

            ticket_data = ticket.model_dump(mode="json")
            ticket_json, ticket_hash = _canonical(ticket_data)
            existing_ticket = connection.execute(
                "SELECT payload_sha256 FROM operation_tickets WHERE ticket_id=?", (ticket.ticket_id,)
            ).fetchone()
            if existing_ticket:
                if existing_ticket["payload_sha256"] != ticket_hash:
                    raise ImmutableConflict("ticket_id already exists with different content")
                return False
            cursor = connection.execute(
                """INSERT INTO operation_tickets(
                    ticket_id, forecast_id, forecast_revision, supersedes_ticket_id, symbol,
                    created_at, expires_at, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ticket.ticket_id,
                    ticket.forecast_id,
                    ticket.forecast_revision,
                    ticket.supersedes_ticket_id,
                    ticket.instrument.symbol,
                    _iso(ticket.created_at),
                    _iso(ticket.expires_at),
                    ticket_json,
                    ticket_hash,
                ),
            ).lastrowid
            connection.execute(
                "INSERT INTO ticket_delivery_outbox(sequence, ticket_id, published_at) VALUES (?, ?, ?)",
                (cursor, ticket.ticket_id, _iso(_utc_now())),
            )
            return True

    def active_forecasts(
        self, symbol: str, *, strategy_release_id: str, limit: int = 100
    ) -> list[ForecastEnvelope]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT payload_json FROM forecasts WHERE symbol=?
                   ORDER BY created_at DESC, revision DESC LIMIT ?""",
                (symbol.strip().upper(), max(2, min(int(limit), 500))),
            ).fetchall()
        forecasts = [ForecastEnvelope.model_validate_json(row["payload_json"]) for row in rows]
        return [
            item for item in forecasts if item.lineage.strategy_release_id == strategy_release_id
        ]

    def next_portfolio_decision_version(self, symbol: str) -> int:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT MAX(decision_version) AS version FROM portfolio_intents WHERE symbol=?",
                (symbol.strip().upper(),),
            ).fetchone()
        return int(row["version"] or 0) + 1

    def get_portfolio_intent(self, portfolio_decision_id: str) -> Optional[PortfolioIntent]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM portfolio_intents WHERE portfolio_decision_id=?",
                (portfolio_decision_id,),
            ).fetchone()
        return PortfolioIntent.model_validate_json(row["payload_json"]) if row else None

    def latest_portfolio_intent(self, symbol: str) -> Optional[PortfolioIntent]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT payload_json FROM portfolio_intents WHERE symbol=?
                   ORDER BY decision_version DESC LIMIT 1""",
                (symbol.strip().upper(),),
            ).fetchone()
        return PortfolioIntent.model_validate_json(row["payload_json"]) if row else None

    def list_tickets(self, after_cursor: int = 0, limit: int = 100) -> tuple[list[OperationTicket], int]:
        page, cursor = self.ticket_page(after_cursor, limit)
        return [ticket for _, ticket in page], cursor

    def ticket_page(self, after_cursor: int = 0, limit: int = 100) -> tuple[list[tuple[int, OperationTicket]], int]:
        safe_limit = max(1, min(int(limit), 500))
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT o.sequence, t.payload_json
                   FROM ticket_delivery_outbox o
                   JOIN operation_tickets t ON t.ticket_id=o.ticket_id
                   WHERE o.sequence>? ORDER BY o.sequence ASC LIMIT ?""",
                (max(0, int(after_cursor)), safe_limit),
            ).fetchall()
        tickets = [
            (int(row["sequence"]), OperationTicket.model_validate_json(row["payload_json"]))
            for row in rows
        ]
        cursor = int(rows[-1]["sequence"]) if rows else max(0, int(after_cursor))
        return tickets, cursor

    def get_ticket(self, ticket_id: str) -> Optional[OperationTicket]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM operation_tickets WHERE ticket_id=?", (ticket_id,)
            ).fetchone()
        return OperationTicket.model_validate_json(row["payload_json"]) if row else None

    def get_forecast(self, forecast_id: str, revision: Optional[int] = None) -> Optional[ForecastEnvelope]:
        with closing(self.connect()) as connection:
            if revision is None:
                row = connection.execute(
                    """SELECT payload_json FROM forecasts WHERE forecast_id=?
                       ORDER BY revision DESC LIMIT 1""",
                    (forecast_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT payload_json FROM forecasts WHERE forecast_id=? AND revision=?",
                    (forecast_id, revision),
                ).fetchone()
        return ForecastEnvelope.model_validate_json(row["payload_json"]) if row else None

    def latest_forecast(self, symbol: Optional[str] = None) -> Optional[ForecastEnvelope]:
        with closing(self.connect()) as connection:
            if symbol:
                row = connection.execute(
                    """SELECT payload_json FROM forecasts WHERE symbol=?
                       ORDER BY created_at DESC, revision DESC LIMIT 1""",
                    (symbol.strip().upper(),),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT payload_json FROM forecasts ORDER BY created_at DESC, revision DESC LIMIT 1"
                ).fetchone()
        return ForecastEnvelope.model_validate_json(row["payload_json"]) if row else None

    def claim(
        self, ticket_id: str, consumer_id: str, lease_token: str, lease_sec: int = 30
    ) -> Optional[int]:
        now = _utc_now()
        expires = now + timedelta(seconds=max(5, min(int(lease_sec), 3600)))
        with self.transaction(immediate=True) as connection:
            if not connection.execute(
                "SELECT 1 FROM operation_tickets WHERE ticket_id=?", (ticket_id,)
            ).fetchone():
                return None
            row = connection.execute(
                """SELECT consumer_id, lease_token, lease_expires_at, claim_epoch
                   FROM ticket_claims WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if row:
                current_expiry = datetime.fromisoformat(row["lease_expires_at"].replace("Z", "+00:00"))
                same_lease = (
                    row["consumer_id"] == consumer_id and row["lease_token"] == lease_token
                )
                if current_expiry > now and not same_lease:
                    return None
                claim_epoch = int(row["claim_epoch"] or 0) + (0 if same_lease else 1)
                connection.execute(
                    """UPDATE ticket_claims SET consumer_id=?, lease_token=?, claimed_at=?,
                       lease_expires_at=?, claim_epoch=?
                       WHERE ticket_id=?""",
                    (
                        consumer_id,
                        lease_token,
                        _iso(now),
                        _iso(expires),
                        claim_epoch,
                        ticket_id,
                    ),
                )
            else:
                claim_epoch = 1
                connection.execute(
                    """INSERT INTO ticket_claims(
                        ticket_id, consumer_id, lease_token, claimed_at,
                        lease_expires_at, claim_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?)""",
                    (ticket_id, consumer_id, lease_token, _iso(now), _iso(expires), claim_epoch),
                )
            return claim_epoch

    def save_receipt(self, receipt: ExecutionReceipt) -> bool:
        data = receipt.model_dump(mode="json")
        payload_json, payload_hash = _canonical(data)
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM execution_receipts WHERE receipt_id=?",
                (receipt.receipt_id,),
            ).fetchone()
            if existing:
                if existing["payload_sha256"] != payload_hash:
                    raise ImmutableConflict("receipt_id already exists with different content")
                return False
            connection.execute(
                """INSERT INTO execution_receipts(
                    receipt_id, ticket_id, status, created_at, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    receipt.receipt_id,
                    receipt.ticket_id,
                    receipt.status,
                    _iso(receipt.created_at),
                    payload_json,
                    payload_hash,
                ),
            )
            return True

    def latest_position_version(self, symbol: str) -> int:
        normalized = symbol.strip().upper()
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT r.payload_json
                   FROM execution_receipts r
                   JOIN operation_tickets t ON t.ticket_id=r.ticket_id
                   WHERE t.symbol=? ORDER BY r.sequence DESC LIMIT 50""",
                (normalized,),
            ).fetchall()
        for row in rows:
            payload = json.loads(row["payload_json"])
            value = payload.get("position_version_after")
            if value is not None:
                return int(value)
        return 0

    def append_ticket_event(
        self,
        ticket_id: str,
        event_id: str,
        event_type: str,
        created_at: datetime,
        payload: dict[str, Any],
    ) -> bool:
        payload_json, payload_hash = _canonical(payload)
        with self.transaction(immediate=True) as connection:
            if not connection.execute(
                "SELECT 1 FROM operation_tickets WHERE ticket_id=?", (ticket_id,)
            ).fetchone():
                raise KeyError(ticket_id)
            row = connection.execute(
                "SELECT payload_sha256 FROM ticket_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row:
                if row["payload_sha256"] != payload_hash:
                    raise ImmutableConflict("event_id already exists with different content")
                return False
            connection.execute(
                """INSERT INTO ticket_events(
                    event_id, ticket_id, event_type, created_at, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (event_id, ticket_id, event_type, _iso(created_at), payload_json, payload_hash),
            )
            return True
