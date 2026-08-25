from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from contracts.operation_ticket_v1 import OperationTicket
from execution_state import ExecutionState, TERMINAL_STATES, require_transition
from incident_modes import IncidentMode
from shadow_contracts.repository import resolve_code_commit


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def canonical(payload: dict[str, Any]) -> tuple[str, str]:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


class TicketConflict(RuntimeError):
    pass


class StaleFencingToken(TicketConflict):
    pass


SCHEMA_VERSION = 5
SCHEMA_CHECKSUM = hashlib.sha256(
    b"execution-store:v5:claim-epoch:commands:position-owner:incident-runtime:soak-metrics"
).hexdigest()
CODE_COMMIT = resolve_code_commit(Path(__file__).resolve().parents[1])


class ExecutionStore:
    """Durable single-writer boundary for tickets, orders, fills and positions."""

    def __init__(self, db_path: Path, *, code_commit: str | None = None):
        self.db_path = Path(db_path)
        self.code_commit = (code_commit or CODE_COMMIT).strip()
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
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
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
                """CREATE TABLE IF NOT EXISTS schema_migrations(
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL,
                    migration_id TEXT NOT NULL,
                    code_commit TEXT NOT NULL,
                    schema_checksum TEXT NOT NULL
                )"""
            )
            current = connection.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
            ).fetchone()["version"]
            if int(current) > SCHEMA_VERSION:
                raise RuntimeError(
                    f"execution DB schema {current} is newer than supported {SCHEMA_VERSION}"
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
            for row in connection.execute(
                "SELECT version,migration_id,code_commit,schema_checksum FROM schema_migrations"
            ).fetchall():
                version = int(row["version"])
                checksum = (
                    SCHEMA_CHECKSUM
                    if version == SCHEMA_VERSION
                    else hashlib.sha256(
                        f"execution-store:legacy-import:v{version}".encode("utf-8")
                    ).hexdigest()
                )
                connection.execute(
                    """UPDATE schema_migrations
                       SET migration_id=COALESCE(migration_id, ?),
                           code_commit=COALESCE(code_commit, ?),
                           schema_checksum=COALESCE(schema_checksum, ?)
                       WHERE version=?""",
                    (f"execution-store-legacy-v{version}", self.code_commit, checksum, version),
                )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations(
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tickets(
                    ticket_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    supersedes_ticket_id TEXT,
                    superseded_by_ticket_id TEXT,
                    consumer_id TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    claim_epoch INTEGER NOT NULL DEFAULT 0,
                    reason_code TEXT,
                    reason_detail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tickets_state ON tickets(state, updated_at);
                CREATE TABLE IF NOT EXISTS ticket_events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    ticket_id TEXT NOT NULL,
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
                );
                CREATE TABLE IF NOT EXISTS execution_orders(
                    order_link_id TEXT PRIMARY KEY,
                    ticket_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    bybit_order_id TEXT UNIQUE,
                    order_status TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL,
                    cum_exec_qty REAL NOT NULL DEFAULT 0,
                    avg_exec_price REAL,
                    exec_fee REAL NOT NULL DEFAULT 0,
                    raw_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
                );
                CREATE INDEX IF NOT EXISTS idx_orders_ticket ON execution_orders(ticket_id);
                CREATE TABLE IF NOT EXISTS execution_fills(
                    exec_id TEXT PRIMARY KEY,
                    order_link_id TEXT NOT NULL,
                    bybit_order_id TEXT,
                    exec_qty REAL NOT NULL,
                    exec_price REAL NOT NULL,
                    exec_fee REAL NOT NULL,
                    executed_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(order_link_id) REFERENCES execution_orders(order_link_id)
                );
                CREATE TABLE IF NOT EXISTS execution_commands(
                    command_id TEXT PRIMARY KEY,
                    ticket_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    target_order_link_id TEXT,
                    status TEXT NOT NULL,
                    claim_owner TEXT NOT NULL,
                    claim_epoch INTEGER NOT NULL,
                    raw_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
                );
                CREATE INDEX IF NOT EXISTS idx_commands_ticket
                    ON execution_commands(ticket_id, role);
                CREATE TABLE IF NOT EXISTS position_snapshots(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    side TEXT,
                    quantity REAL NOT NULL,
                    avg_price REAL,
                    notional_usdt REAL NOT NULL,
                    source TEXT NOT NULL,
                    position_owner_id TEXT NOT NULL DEFAULT 'legacy-unowned',
                    captured_at TEXT NOT NULL,
                    UNIQUE(symbol, version)
                );
                CREATE TABLE IF NOT EXISTS risk_runtime(
                    risk_date TEXT PRIMARY KEY,
                    realised_pnl REAL NOT NULL DEFAULT 0,
                    unrealised_pnl REAL NOT NULL DEFAULT 0,
                    consecutive_losses INTEGER NOT NULL DEFAULT 0,
                    cooldown_until TEXT,
                    kill_switch INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS equity_runtime(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    equity_high_water_usdt REAL NOT NULL,
                    latest_equity_usdt REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS receipt_outbox(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_id TEXT NOT NULL UNIQUE,
                    ticket_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    delivered_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(ticket_id) REFERENCES tickets(ticket_id)
                );
                CREATE TABLE IF NOT EXISTS consumer_cursors(
                    consumer_id TEXT PRIMARY KEY,
                    cursor INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS system_runtime(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    incident_mode TEXT NOT NULL,
                    reconciliation_complete INTEGER NOT NULL,
                    reason TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS service_runs(
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    stopped_at TEXT,
                    clean_shutdown INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS runtime_metrics(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    labels_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_runtime_metrics_name_time
                    ON runtime_metrics(metric_name,captured_at);
                """
            )
            ticket_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(tickets)")
            }
            if "claim_epoch" not in ticket_columns:
                connection.execute(
                    "ALTER TABLE tickets ADD COLUMN claim_epoch INTEGER NOT NULL DEFAULT 0"
                )
            position_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(position_snapshots)")
            }
            if "position_owner_id" not in position_columns:
                connection.execute(
                    """ALTER TABLE position_snapshots ADD COLUMN position_owner_id TEXT
                       NOT NULL DEFAULT 'legacy-unowned'"""
                )
            connection.execute(
                """INSERT OR IGNORE INTO system_runtime(
                    singleton, incident_mode, reconciliation_complete, reason, updated_at
                ) VALUES (1, ?, 0, ?, ?)""",
                (IncidentMode.NORMAL.value, "startup reconciliation has not completed", iso(utc_now())),
            )
            connection.execute(
                """INSERT OR IGNORE INTO schema_migrations(
                    version, applied_at, migration_id, code_commit, schema_checksum
                ) VALUES (?, ?, ?, ?, ?)""",
                (
                    SCHEMA_VERSION,
                    iso(utc_now()),
                    "execution-store-v5",
                    self.code_commit,
                    SCHEMA_CHECKSUM,
                ),
            )
            recorded = connection.execute(
                "SELECT schema_checksum FROM schema_migrations WHERE version=?",
                (SCHEMA_VERSION,),
            ).fetchone()
            if not recorded or recorded["schema_checksum"] != SCHEMA_CHECKSUM:
                raise RuntimeError("execution DB schema checksum does not match this build")

    @staticmethod
    def _event_id(ticket_id: str, event_type: str, payload: dict[str, Any], now: datetime) -> str:
        payload_text, _ = canonical(payload)
        digest = hashlib.sha256(
            f"{ticket_id}|{event_type}|{now.isoformat()}|{payload_text}".encode("utf-8")
        ).hexdigest()
        return f"ev_{digest[:32]}"

    def _append_event(
        self,
        connection: sqlite3.Connection,
        ticket_id: str,
        current: Optional[ExecutionState],
        target: ExecutionState,
        event_type: str,
        payload: Optional[dict[str, Any]] = None,
        now: Optional[datetime] = None,
    ) -> None:
        timestamp = now or utc_now()
        event_payload = payload or {}
        connection.execute(
            """INSERT INTO ticket_events(
                event_id, ticket_id, from_state, to_state, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                self._event_id(ticket_id, event_type, event_payload, timestamp),
                ticket_id,
                current.value if current else None,
                target.value,
                event_type,
                canonical(event_payload)[0],
                iso(timestamp),
            ),
        )

    def receive(self, ticket: OperationTicket) -> bool:
        payload_json, payload_hash = canonical(ticket.model_dump(mode="json"))
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT payload_sha256 FROM tickets WHERE ticket_id=?", (ticket.ticket_id,)
            ).fetchone()
            if existing:
                if existing["payload_sha256"] != payload_hash:
                    raise TicketConflict("ticket_id already exists with different content")
                return False
            connection.execute(
                """INSERT INTO tickets(
                    ticket_id, payload_json, payload_sha256, state, symbol, action,
                    supersedes_ticket_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ticket.ticket_id,
                    payload_json,
                    payload_hash,
                    ExecutionState.RECEIVED.value,
                    ticket.instrument.symbol,
                    ticket.intent.action,
                    ticket.supersedes_ticket_id,
                    iso(ticket.created_at),
                    iso(now),
                ),
            )
            self._append_event(
                connection, ticket.ticket_id, None, ExecutionState.RECEIVED, "ticket_received", now=now
            )
            if ticket.supersedes_ticket_id:
                old = connection.execute(
                    "SELECT state FROM tickets WHERE ticket_id=?", (ticket.supersedes_ticket_id,)
                ).fetchone()
                if old:
                    old_state = ExecutionState(old["state"])
                    if old_state not in TERMINAL_STATES:
                        require_transition(old_state, ExecutionState.SUPERSEDED)
                        connection.execute(
                            """UPDATE tickets SET state=?, superseded_by_ticket_id=?, updated_at=?
                               WHERE ticket_id=?""",
                            (
                                ExecutionState.SUPERSEDED.value,
                                ticket.ticket_id,
                                iso(now),
                                ticket.supersedes_ticket_id,
                            ),
                        )
                        self._append_event(
                            connection,
                            ticket.supersedes_ticket_id,
                            old_state,
                            ExecutionState.SUPERSEDED,
                            "ticket_superseded",
                            {"superseded_by_ticket_id": ticket.ticket_id},
                            now,
                        )
            return True

    def get_ticket(self, ticket_id: str) -> Optional[OperationTicket]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM tickets WHERE ticket_id=?", (ticket_id,)
            ).fetchone()
        return OperationTicket.model_validate_json(row["payload_json"]) if row else None

    def state(self, ticket_id: str) -> Optional[ExecutionState]:
        with closing(self.connect()) as connection:
            row = connection.execute("SELECT state FROM tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        return ExecutionState(row["state"]) if row else None

    def transition(
        self,
        ticket_id: str,
        target: ExecutionState,
        event_type: str,
        payload: Optional[dict[str, Any]] = None,
        *,
        reason_code: Optional[str] = None,
        reason_detail: Optional[str] = None,
        fence_owner: Optional[str] = None,
        fence_epoch: Optional[int] = None,
    ) -> bool:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """SELECT state, consumer_id, claim_epoch, lease_expires_at
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                raise KeyError(ticket_id)
            if fence_owner is not None or fence_epoch is not None:
                if fence_owner is None or fence_epoch is None:
                    raise ValueError("both fence_owner and fence_epoch are required")
                lease_expiry = (
                    parse_time(row["lease_expires_at"]) if row["lease_expires_at"] else None
                )
                if (
                    row["consumer_id"] != fence_owner
                    or int(row["claim_epoch"] or 0) != int(fence_epoch)
                    or lease_expiry is None
                    or lease_expiry <= now
                ):
                    raise StaleFencingToken("ticket transition rejected stale fencing token")
            current = ExecutionState(row["state"])
            if current == target:
                return False
            require_transition(current, target)
            connection.execute(
                """UPDATE tickets SET state=?, reason_code=COALESCE(?, reason_code),
                   reason_detail=COALESCE(?, reason_detail), updated_at=? WHERE ticket_id=?""",
                (target.value, reason_code, reason_detail, iso(now), ticket_id),
            )
            self._append_event(connection, ticket_id, current, target, event_type, payload, now)
            return True

    def claim(
        self,
        ticket_id: str,
        consumer_id: str,
        lease_token: str,
        lease_sec: int = 30,
        *,
        claim_epoch: Optional[int] = None,
    ) -> Optional[int]:
        now = utc_now()
        expiry = now + timedelta(seconds=max(5, min(int(lease_sec), 3600)))
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """SELECT state, consumer_id, lease_token, lease_expires_at, claim_epoch
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                return None
            state = ExecutionState(row["state"])
            existing_expiry = parse_time(row["lease_expires_at"]) if row["lease_expires_at"] else None
            same_lease = (
                row["consumer_id"] == consumer_id and row["lease_token"] == lease_token
            )
            if existing_expiry and existing_expiry > now and row["consumer_id"] and not same_lease:
                return None
            current_epoch = int(row["claim_epoch"] or 0)
            next_epoch = current_epoch if same_lease else current_epoch + 1
            if claim_epoch is not None:
                if same_lease and int(claim_epoch) != current_epoch:
                    return None
                if not same_lease and int(claim_epoch) <= current_epoch:
                    return None
                next_epoch = int(claim_epoch)
            if state == ExecutionState.VALIDATED:
                require_transition(state, ExecutionState.CLAIMED)
                connection.execute(
                    """UPDATE tickets SET state=?, consumer_id=?, lease_token=?, lease_expires_at=?,
                       claim_epoch=?, updated_at=?
                       WHERE ticket_id=?""",
                    (
                        ExecutionState.CLAIMED.value,
                        consumer_id,
                        lease_token,
                        iso(expiry),
                        next_epoch,
                        iso(now),
                        ticket_id,
                    ),
                )
                self._append_event(
                    connection, ticket_id, state, ExecutionState.CLAIMED, "ticket_claimed",
                    {
                        "consumer_id": consumer_id,
                        "lease_expires_at": iso(expiry),
                        "claim_epoch": next_epoch,
                    },
                    now,
                )
                return next_epoch
            if state in {ExecutionState.CLAIMED, ExecutionState.RISK_APPROVED} and (
                same_lease or not existing_expiry or existing_expiry <= now
            ):
                connection.execute(
                    """UPDATE tickets SET consumer_id=?, lease_token=?, lease_expires_at=?,
                       claim_epoch=?, updated_at=?
                       WHERE ticket_id=?""",
                    (consumer_id, lease_token, iso(expiry), next_epoch, iso(now), ticket_id),
                )
                return next_epoch
            return None

    def claim_owner(self, ticket_id: str) -> Optional[str]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT consumer_id, lease_expires_at FROM tickets WHERE ticket_id=?", (ticket_id,)
            ).fetchone()
        if not row or not row["consumer_id"] or not row["lease_expires_at"]:
            return None
        return row["consumer_id"] if parse_time(row["lease_expires_at"]) > utc_now() else None

    def current_fence(self, ticket_id: str) -> Optional[tuple[str, int]]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT consumer_id, claim_epoch, lease_expires_at
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
        if not row or not row["consumer_id"] or not row["lease_expires_at"]:
            return None
        if parse_time(row["lease_expires_at"]) <= utc_now():
            return None
        return str(row["consumer_id"]), int(row["claim_epoch"] or 0)

    def claim_operation(
        self,
        ticket_id: str,
        consumer_id: str,
        lease_token: str,
        lease_sec: int = 60,
    ) -> Optional[int]:
        """Acquire a fencing epoch for child/cancel operations in any ticket state.

        A new service instance must use a new lease token.  That makes an expired
        worker's epoch permanently stale even when the configured consumer id is
        unchanged across restarts.
        """

        now = utc_now()
        expiry = now + timedelta(seconds=max(5, min(int(lease_sec), 3600)))
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """SELECT consumer_id,lease_token,lease_expires_at,claim_epoch
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                return None
            existing_expiry = (
                parse_time(row["lease_expires_at"]) if row["lease_expires_at"] else None
            )
            same_lease = (
                row["consumer_id"] == consumer_id and row["lease_token"] == lease_token
            )
            if existing_expiry and existing_expiry > now and row["consumer_id"] and not same_lease:
                return None
            current_epoch = int(row["claim_epoch"] or 0)
            next_epoch = current_epoch if same_lease else current_epoch + 1
            connection.execute(
                """UPDATE tickets SET consumer_id=?,lease_token=?,lease_expires_at=?,
                   claim_epoch=?,updated_at=? WHERE ticket_id=?""",
                (
                    consumer_id,
                    lease_token,
                    iso(expiry),
                    next_epoch,
                    iso(now),
                    ticket_id,
                ),
            )
            return next_epoch

    @staticmethod
    def _require_fence(
        row: sqlite3.Row,
        claim_owner: str,
        claim_epoch: int,
        now: datetime,
        operation: str,
    ) -> None:
        expiry = parse_time(row["lease_expires_at"]) if row["lease_expires_at"] else None
        if (
            row["consumer_id"] != claim_owner
            or int(row["claim_epoch"] or 0) != int(claim_epoch)
            or expiry is None
            or expiry <= now
        ):
            raise StaleFencingToken(f"{operation} rejected stale fencing token")

    def reserve_command(
        self,
        ticket_id: str,
        command_id: str,
        *,
        role: str,
        claim_owner: str,
        claim_epoch: int,
        target_order_link_id: Optional[str] = None,
    ) -> bool:
        """Journal a non-order operation before its first network side effect."""

        now = utc_now()
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT ticket_id,role FROM execution_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if existing:
                if existing["ticket_id"] != ticket_id or existing["role"] != role:
                    raise TicketConflict("command_id is already assigned to another operation")
                return False
            row = connection.execute(
                """SELECT state,consumer_id,claim_epoch,lease_expires_at
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                raise KeyError(ticket_id)
            self._require_fence(row, claim_owner, claim_epoch, now, "command reservation")
            connection.execute(
                """INSERT INTO execution_commands(
                    command_id,ticket_id,role,target_order_link_id,status,
                    claim_owner,claim_epoch,raw_json,created_at,updated_at
                ) VALUES (?, ?, ?, ?, 'RESERVED', ?, ?, '{}', ?, ?)""",
                (
                    command_id,
                    ticket_id,
                    role,
                    target_order_link_id,
                    claim_owner,
                    int(claim_epoch),
                    iso(now),
                    iso(now),
                ),
            )
            return True

    def confirm_command(
        self,
        command_id: str,
        status: str,
        raw: Optional[dict[str, Any]] = None,
    ) -> None:
        normalized = str(status).strip().upper()
        if normalized not in {"REST_ACCEPTED", "CONFIRMED", "FAILED", "UNKNOWN"}:
            raise ValueError("invalid execution command status")
        with self.transaction(immediate=True) as connection:
            result = connection.execute(
                """UPDATE execution_commands SET status=?,raw_json=?,updated_at=?
                   WHERE command_id=?""",
                (normalized, canonical(raw or {})[0], iso(utc_now()), command_id),
            )
            if result.rowcount != 1:
                raise KeyError(command_id)

    def command(self, command_id: str) -> Optional[dict[str, Any]]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM execution_commands WHERE command_id=?", (command_id,)
            ).fetchone()
        return dict(row) if row else None

    def reserve_order(
        self,
        ticket_id: str,
        order_link_id: str,
        *,
        role: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float],
        claim_owner: str,
        claim_epoch: int,
    ) -> bool:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT ticket_id FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if existing:
                if existing["ticket_id"] != ticket_id:
                    raise TicketConflict("order_link_id is already assigned to another ticket")
                return False
            row = connection.execute(
                """SELECT state, consumer_id, claim_epoch, lease_expires_at
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                raise KeyError(ticket_id)
            self._require_fence(row, claim_owner, claim_epoch, now, "order reservation")
            current = ExecutionState(row["state"])
            require_transition(current, ExecutionState.SUBMITTING)
            connection.execute(
                """INSERT INTO execution_orders(
                    order_link_id, ticket_id, role, order_status, side, order_type,
                    quantity, price, raw_json, created_at, updated_at
                ) VALUES (?, ?, ?, 'CREATED', ?, ?, ?, ?, '{}', ?, ?)""",
                (order_link_id, ticket_id, role, side, order_type, quantity, price, iso(now), iso(now)),
            )
            connection.execute(
                "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                (ExecutionState.SUBMITTING.value, iso(now), ticket_id),
            )
            self._append_event(
                connection, ticket_id, current, ExecutionState.SUBMITTING,
                "order_reserved", {"order_link_id": order_link_id}, now,
            )
            return True

    def reserve_exit_order(
        self,
        ticket_id: str,
        order_link_id: str,
        *,
        role: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float],
        claim_owner: str,
        claim_epoch: int,
    ) -> bool:
        """Reserve a deterministic child exit without changing the completed entry state."""
        if not role.startswith(("take_profit_", "trailing_", "stop_loss_", "time_exit_")):
            raise ValueError("exit order role is invalid")
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT ticket_id FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if existing:
                if existing["ticket_id"] != ticket_id:
                    raise TicketConflict("order_link_id is already assigned to another ticket")
                return False
            row = connection.execute(
                """SELECT state,consumer_id,claim_epoch,lease_expires_at
                   FROM tickets WHERE ticket_id=?""",
                (ticket_id,),
            ).fetchone()
            if not row:
                raise KeyError(ticket_id)
            self._require_fence(row, claim_owner, claim_epoch, now, "exit reservation")
            current = ExecutionState(row["state"])
            if current not in {ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED}:
                raise TicketConflict("exit protection can be reserved only after an entry fill")
            connection.execute(
                """INSERT INTO execution_orders(
                    order_link_id, ticket_id, role, order_status, side, order_type,
                    quantity, price, raw_json, created_at, updated_at
                ) VALUES (?, ?, ?, 'CREATED', ?, ?, ?, ?, '{}', ?, ?)""",
                (order_link_id, ticket_id, role, side, order_type, quantity, price, iso(now), iso(now)),
            )
            self._append_event(
                connection, ticket_id, current, current, "exit_order_reserved",
                {"order_link_id": order_link_id, "role": role}, now,
            )
            return True

    def record_rest_submission(
        self,
        order_link_id: str,
        bybit_order_id: Optional[str],
        raw: Optional[dict[str, Any]] = None,
    ) -> None:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            order = connection.execute(
                "SELECT ticket_id FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if not order:
                raise KeyError(order_link_id)
            ticket = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (order["ticket_id"],)
            ).fetchone()
            current = ExecutionState(ticket["state"])
            connection.execute(
                """UPDATE execution_orders SET bybit_order_id=COALESCE(?, bybit_order_id),
                   order_status='REST_ACCEPTED', raw_json=?, updated_at=? WHERE order_link_id=?""",
                (bybit_order_id, canonical(raw or {})[0], iso(now), order_link_id),
            )
            if current != ExecutionState.SUBMITTED:
                require_transition(current, ExecutionState.SUBMITTED)
                connection.execute(
                    "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                    (ExecutionState.SUBMITTED.value, iso(now), order["ticket_id"]),
                )
                self._append_event(
                    connection, order["ticket_id"], current, ExecutionState.SUBMITTED,
                    "rest_submission_accepted", {"order_link_id": order_link_id}, now,
                )

    def record_exit_submission(
        self,
        order_link_id: str,
        bybit_order_id: Optional[str],
        raw: Optional[dict[str, Any]] = None,
    ) -> None:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            order = connection.execute(
                "SELECT ticket_id, role FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if not order:
                raise KeyError(order_link_id)
            if order["role"] == "entry":
                raise TicketConflict("entry orders must use record_rest_submission")
            connection.execute(
                """UPDATE execution_orders SET bybit_order_id=COALESCE(?, bybit_order_id),
                   order_status='REST_ACCEPTED', raw_json=?, updated_at=? WHERE order_link_id=?""",
                (bybit_order_id, canonical(raw or {})[0], iso(now), order_link_id),
            )
            ticket = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (order["ticket_id"],)
            ).fetchone()
            current = ExecutionState(ticket["state"])
            self._append_event(
                connection, order["ticket_id"], current, current, "exit_rest_submission_accepted",
                {"order_link_id": order_link_id, "role": order["role"]}, now,
            )

    def acknowledge_order(
        self, order_link_id: str, order_status: str, raw: Optional[dict[str, Any]] = None
    ) -> None:
        now = utc_now()
        normalized_status = str(order_status or "UNKNOWN").strip().upper().replace("CANCELED", "CANCELLED")
        with self.transaction(immediate=True) as connection:
            order = connection.execute(
                "SELECT ticket_id, role, order_status FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if not order:
                raise KeyError(order_link_id)
            current_order_status = str(order["order_status"] or "").upper().replace("CANCELED", "CANCELLED")
            final_order_states = {"FILLED", "CANCELLED", "REJECTED", "DEACTIVATED"}
            should_update = (
                current_order_status not in final_order_states
                or normalized_status == "FILLED"
            )
            if should_update:
                remote_order_id = str((raw or {}).get("orderId") or "") or None
                connection.execute(
                    """UPDATE execution_orders SET bybit_order_id=COALESCE(?, bybit_order_id),
                       order_status=?, raw_json=?, updated_at=? WHERE order_link_id=?""",
                    (remote_order_id, normalized_status, canonical(raw or {})[0], iso(now), order_link_id),
                )
            ticket = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (order["ticket_id"],)
            ).fetchone()
            current = ExecutionState(ticket["state"])
            if order["role"] != "entry":
                self._append_event(
                    connection,
                    order["ticket_id"],
                    current,
                    current,
                    "exit_order_update",
                    {"order_link_id": order_link_id, "order_status": normalized_status},
                    now,
                )
                return
            if normalized_status in {"CANCELLED", "DEACTIVATED"} and current not in {
                ExecutionState.FILLED,
                ExecutionState.CANCELLED,
            }:
                require_transition(current, ExecutionState.CANCELLED)
                connection.execute(
                    "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                    (ExecutionState.CANCELLED.value, iso(now), order["ticket_id"]),
                )
                self._append_event(
                    connection,
                    order["ticket_id"],
                    current,
                    ExecutionState.CANCELLED,
                    "entry_order_cancelled",
                    {"order_link_id": order_link_id},
                    now,
                )
            elif normalized_status == "REJECTED" and current not in TERMINAL_STATES:
                require_transition(current, ExecutionState.FAILED)
                connection.execute(
                    """UPDATE tickets SET state=?, reason_code=?, reason_detail=?, updated_at=?
                       WHERE ticket_id=?""",
                    (
                        ExecutionState.FAILED.value,
                        "EXCHANGE_ORDER_REJECTED",
                        str((raw or {}).get("rejectReason") or "exchange rejected entry order"),
                        iso(now),
                        order["ticket_id"],
                    ),
                )
                self._append_event(
                    connection,
                    order["ticket_id"],
                    current,
                    ExecutionState.FAILED,
                    "entry_order_rejected",
                    {"order_link_id": order_link_id},
                    now,
                )
            elif current in {ExecutionState.SUBMITTING, ExecutionState.SUBMITTED} and normalized_status not in final_order_states:
                require_transition(current, ExecutionState.ACKNOWLEDGED)
                connection.execute(
                    "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                    (ExecutionState.ACKNOWLEDGED.value, iso(now), order["ticket_id"]),
                )
                self._append_event(
                    connection, order["ticket_id"], current, ExecutionState.ACKNOWLEDGED,
                    "websocket_order_acknowledged", {"order_link_id": order_link_id}, now,
                )

    def record_cancel_requested(
        self, order_link_id: str, raw: Optional[dict[str, Any]] = None
    ) -> None:
        """Persist an entry cancellation request without claiming it was confirmed."""

        now = utc_now()
        with self.transaction(immediate=True) as connection:
            order = connection.execute(
                "SELECT ticket_id, role FROM execution_orders WHERE order_link_id=?",
                (order_link_id,),
            ).fetchone()
            if not order:
                raise KeyError(order_link_id)
            connection.execute(
                """UPDATE execution_orders SET order_status='CANCEL_REQUESTED', raw_json=?,
                   updated_at=? WHERE order_link_id=? AND order_status NOT IN ('FILLED','CANCELLED')""",
                (canonical(raw or {})[0], iso(now), order_link_id),
            )
            ticket = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (order["ticket_id"],)
            ).fetchone()
            current = ExecutionState(ticket["state"])
            self._append_event(
                connection,
                order["ticket_id"],
                current,
                current,
                "entry_cancel_requested",
                {"order_link_id": order_link_id},
                now,
            )

    def record_fill(
        self,
        *,
        exec_id: str,
        order_link_id: str,
        quantity: float,
        price: float,
        fee: float,
        executed_at: datetime,
        bybit_order_id: Optional[str] = None,
        raw: Optional[dict[str, Any]] = None,
    ) -> bool:
        if quantity <= 0 or price <= 0 or fee < 0:
            raise ValueError("invalid fill values")
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            if connection.execute("SELECT 1 FROM execution_fills WHERE exec_id=?", (exec_id,)).fetchone():
                return False
            order = connection.execute(
                "SELECT ticket_id, role, quantity FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
            if not order:
                raise KeyError(order_link_id)
            connection.execute(
                """INSERT INTO execution_fills(
                    exec_id, order_link_id, bybit_order_id, exec_qty, exec_price,
                    exec_fee, executed_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    exec_id, order_link_id, bybit_order_id, quantity, price, fee,
                    iso(executed_at), canonical(raw or {})[0],
                ),
            )
            aggregate = connection.execute(
                """SELECT SUM(exec_qty) AS qty,
                          SUM(exec_qty * exec_price) / SUM(exec_qty) AS avg_price,
                          SUM(exec_fee) AS fee
                   FROM execution_fills WHERE order_link_id=?""",
                (order_link_id,),
            ).fetchone()
            cumulative = float(aggregate["qty"] or 0)
            filled = cumulative + 1e-12 >= float(order["quantity"])
            order_status = "FILLED" if filled else "PARTIALLY_FILLED"
            target = ExecutionState.FILLED if filled else ExecutionState.PARTIALLY_FILLED
            connection.execute(
                """UPDATE execution_orders SET bybit_order_id=COALESCE(?, bybit_order_id),
                   order_status=?, cum_exec_qty=?, avg_exec_price=?, exec_fee=?, updated_at=?
                   WHERE order_link_id=?""",
                (
                    bybit_order_id, order_status, cumulative, float(aggregate["avg_price"]),
                    float(aggregate["fee"] or 0), iso(now), order_link_id,
                ),
            )
            ticket = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (order["ticket_id"],)
            ).fetchone()
            current = ExecutionState(ticket["state"])
            if order["role"] != "entry":
                self._append_event(
                    connection, order["ticket_id"], current, current, "exit_fill_recorded",
                    {
                        "exec_id": exec_id,
                        "order_link_id": order_link_id,
                        "role": order["role"],
                        "cum_exec_qty": cumulative,
                    },
                    now,
                )
                return True
            if current != target:
                require_transition(current, target)
                connection.execute(
                    "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                    (target.value, iso(now), order["ticket_id"]),
                )
                self._append_event(
                    connection, order["ticket_id"], current, target, "fill_recorded",
                    {"exec_id": exec_id, "order_link_id": order_link_id, "cum_exec_qty": cumulative}, now,
                )
            return True

    def order(self, order_link_id: str) -> Optional[dict[str, Any]]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM execution_orders WHERE order_link_id=?", (order_link_id,)
            ).fetchone()
        return dict(row) if row else None

    def confirm_cancellation(
        self,
        cancel_ticket_id: str,
        target_order_link_id: str,
        raw: Optional[dict[str, Any]] = None,
    ) -> None:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            target_order = connection.execute(
                "SELECT ticket_id FROM execution_orders WHERE order_link_id=?",
                (target_order_link_id,),
            ).fetchone()
            if not target_order:
                raise KeyError(target_order_link_id)
            cancel_row = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (cancel_ticket_id,)
            ).fetchone()
            if not cancel_row:
                raise KeyError(cancel_ticket_id)
            cancel_state = ExecutionState(cancel_row["state"])
            require_transition(cancel_state, ExecutionState.CANCELLED)
            connection.execute(
                "UPDATE execution_orders SET order_status='CANCELLED', raw_json=?, updated_at=? WHERE order_link_id=?",
                (canonical(raw or {})[0], iso(now), target_order_link_id),
            )
            target_row = connection.execute(
                "SELECT state FROM tickets WHERE ticket_id=?", (target_order["ticket_id"],)
            ).fetchone()
            target_state = ExecutionState(target_row["state"])
            if target_state != ExecutionState.CANCELLED:
                require_transition(target_state, ExecutionState.CANCELLED)
                connection.execute(
                    "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                    (ExecutionState.CANCELLED.value, iso(now), target_order["ticket_id"]),
                )
                self._append_event(
                    connection,
                    target_order["ticket_id"],
                    target_state,
                    ExecutionState.CANCELLED,
                    "order_cancelled_by_ticket",
                    {"cancel_ticket_id": cancel_ticket_id, "order_link_id": target_order_link_id},
                    now,
                )
            connection.execute(
                "UPDATE tickets SET state=?, updated_at=? WHERE ticket_id=?",
                (ExecutionState.CANCELLED.value, iso(now), cancel_ticket_id),
            )
            self._append_event(
                connection,
                cancel_ticket_id,
                cancel_state,
                ExecutionState.CANCELLED,
                "cancellation_confirmed",
                {"target_order_link_id": target_order_link_id},
                now,
            )

    def incomplete_ticket_ids(self) -> list[str]:
        terminal = tuple(state.value for state in TERMINAL_STATES)
        placeholders = ",".join("?" for _ in terminal)
        with closing(self.connect()) as connection:
            rows = connection.execute(
                f"SELECT ticket_id FROM tickets WHERE state NOT IN ({placeholders}) ORDER BY updated_at",
                terminal,
            ).fetchall()
        return [row["ticket_id"] for row in rows]

    def reconciliation_ticket_ids(self) -> list[str]:
        """Include terminal entry tickets while any deterministic child order is active."""

        terminal = tuple(state.value for state in TERMINAL_STATES)
        placeholders = ",".join("?" for _ in terminal)
        with closing(self.connect()) as connection:
            rows = connection.execute(
                f"""SELECT DISTINCT t.ticket_id
                    FROM tickets t
                    LEFT JOIN execution_orders o ON o.ticket_id=t.ticket_id
                    WHERE t.state NOT IN ({placeholders})
                       OR (o.role!='entry' AND UPPER(o.order_status) NOT IN
                           ('FILLED','CANCELLED','CANCELED','REJECTED','DEACTIVATED'))
                    ORDER BY t.updated_at""",
                terminal,
            ).fetchall()
        return [row["ticket_id"] for row in rows]

    def latest_position_origin_ticket_ids(self) -> list[str]:
        """Return only the newest filled/part-filled entry ticket for each symbol."""

        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT t.ticket_id
                   FROM tickets t
                   JOIN execution_orders o ON o.ticket_id=t.ticket_id AND o.role='entry'
                   WHERE o.cum_exec_qty>0
                     AND t.updated_at=(
                         SELECT MAX(t2.updated_at)
                         FROM tickets t2
                         JOIN execution_orders o2 ON o2.ticket_id=t2.ticket_id AND o2.role='entry'
                         WHERE t2.symbol=t.symbol AND o2.cum_exec_qty>0
                     )
                   ORDER BY t.symbol"""
            ).fetchall()
        return [row["ticket_id"] for row in rows]

    def first_entry_fill_at(self, ticket_id: str) -> Optional[datetime]:
        """Return the exchange execution time that started the position clock."""

        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT MIN(f.executed_at) AS first_fill_at
                   FROM execution_fills f
                   JOIN execution_orders o ON o.order_link_id=f.order_link_id
                   WHERE o.ticket_id=? AND o.role='entry'""",
                (ticket_id,),
            ).fetchone()
        value = row["first_fill_at"] if row else None
        return parse_time(value) if value else None

    def all_ticket_ids(self) -> list[str]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT ticket_id FROM tickets ORDER BY created_at, ticket_id"
            ).fetchall()
        return [row["ticket_id"] for row in rows]

    def latest_position(self, symbol: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT * FROM position_snapshots WHERE symbol=?
                   ORDER BY version DESC LIMIT 1""",
                (symbol.strip().upper(),),
            ).fetchone()
        if row:
            return dict(row)
        return {
            "symbol": symbol.strip().upper(), "version": 0, "side": None,
            "quantity": 0.0, "avg_price": None, "notional_usdt": 0.0,
            "source": "empty", "position_owner_id": "unowned",
            "captured_at": iso(utc_now()),
        }

    def save_position(
        self,
        symbol: str,
        *,
        side: Optional[str],
        quantity: float,
        avg_price: Optional[float],
        notional_usdt: float,
        source: str,
        position_owner_id: str = "legacy-unowned",
    ) -> int:
        normalized = symbol.strip().upper()
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT MAX(version) AS version FROM position_snapshots WHERE symbol=?", (normalized,)
            ).fetchone()
            version = int(row["version"] or 0) + 1
            connection.execute(
                """INSERT INTO position_snapshots(
                    symbol, version, side, quantity, avg_price, notional_usdt, source,
                    position_owner_id, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    normalized,
                    version,
                    side,
                    quantity,
                    avg_price,
                    notional_usdt,
                    source,
                    position_owner_id,
                    iso(now),
                ),
            )
            return version

    def sync_position(
        self,
        symbol: str,
        *,
        side: Optional[str],
        quantity: float,
        avg_price: Optional[float],
        notional_usdt: float,
        source: str,
        position_owner_id: str = "legacy-unowned",
    ) -> int:
        previous = self.latest_position(symbol)
        unchanged = (
            (previous.get("side") or None) == (side or None)
            and abs(float(previous.get("quantity") or 0) - float(quantity)) <= 1e-12
            and abs(float(previous.get("avg_price") or 0) - float(avg_price or 0)) <= 1e-12
            and abs(float(previous.get("notional_usdt") or 0) - float(notional_usdt)) <= 1e-8
            and str(previous.get("position_owner_id") or "") == str(position_owner_id)
        )
        if unchanged:
            return int(previous["version"])
        return self.save_position(
            symbol,
            side=side,
            quantity=quantity,
            avg_price=avg_price,
            notional_usdt=notional_usdt,
            source=source,
            position_owner_id=position_owner_id,
        )

    def adopt_position(
        self,
        symbol: str,
        *,
        side: Optional[str],
        quantity: float,
        avg_price: Optional[float],
        notional_usdt: float,
        position_owner_id: str,
        approval_id: str,
    ) -> int:
        if len(position_owner_id.strip()) < 8 or len(approval_id.strip()) < 8:
            raise ValueError("position adoption requires explicit owner and approval identifiers")
        return self.save_position(
            symbol,
            side=side,
            quantity=quantity,
            avg_price=avg_price,
            notional_usdt=notional_usdt,
            source=f"manual-adoption:{approval_id.strip()}",
            position_owner_id=position_owner_id.strip(),
        )

    def known_order_link_ids(self) -> set[str]:
        with closing(self.connect()) as connection:
            rows = connection.execute("SELECT order_link_id FROM execution_orders").fetchall()
        return {str(row["order_link_id"]) for row in rows}

    def local_position_quantity(self, symbol: str) -> float:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT COALESCE(SUM(
                       CASE WHEN UPPER(o.side)='BUY' THEN o.cum_exec_qty ELSE -o.cum_exec_qty END
                   ), 0) AS quantity
                   FROM execution_orders o
                   JOIN tickets t ON t.ticket_id=o.ticket_id
                   WHERE t.symbol=?""",
                (symbol.strip().upper(),),
            ).fetchone()
        return float(row["quantity"] or 0)

    def set_incident_mode(self, mode: IncidentMode | str, reason: str = "") -> None:
        normalized = IncidentMode(str(getattr(mode, "value", mode)).upper())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE system_runtime SET incident_mode=?, reason=?, updated_at=?
                   WHERE singleton=1""",
                (normalized.value, reason or None, iso(utc_now())),
            )

    def set_reconciliation_complete(self, complete: bool, reason: str = "") -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE system_runtime SET reconciliation_complete=?, reason=?, updated_at=?
                   WHERE singleton=1""",
                (int(bool(complete)), reason or None, iso(utc_now())),
            )

    def system_runtime(self) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM system_runtime WHERE singleton=1"
            ).fetchone()
        return dict(row) if row else {
            "incident_mode": IncidentMode.MANUAL_HANDOVER.value,
            "reconciliation_complete": 0,
            "reason": "system runtime row is missing",
        }

    def begin_service_run(self, run_id: str) -> bool:
        """Start a run and report whether the previous run lacked a clean shutdown."""

        now = iso(utc_now())
        with self.transaction(immediate=True) as connection:
            previous = connection.execute(
                "SELECT run_id FROM service_runs WHERE stopped_at IS NULL ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            unexpected_restart = previous is not None
            if unexpected_restart:
                connection.execute(
                    """UPDATE service_runs SET stopped_at=?,clean_shutdown=0
                       WHERE run_id=? AND stopped_at IS NULL""",
                    (now, previous["run_id"]),
                )
            connection.execute(
                "INSERT INTO service_runs(run_id,started_at) VALUES (?, ?)",
                (run_id, now),
            )
            if unexpected_restart:
                connection.execute(
                    """INSERT INTO runtime_metrics(
                        captured_at,metric_name,metric_value,labels_json
                    ) VALUES (?, 'unexpected_restart', 1, ?)""",
                    (now, canonical({"previous_run_id": previous["run_id"]})[0]),
                )
        return unexpected_restart

    def finish_service_run(self, run_id: str) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE service_runs SET stopped_at=?, clean_shutdown=1
                   WHERE run_id=? AND stopped_at IS NULL""",
                (iso(utc_now()), run_id),
            )

    def record_runtime_metrics(
        self, metrics: dict[str, float], labels: Optional[dict[str, Any]] = None
    ) -> None:
        now = iso(utc_now())
        labels_json = canonical(labels or {})[0]
        rows = [
            (now, str(name), float(value), labels_json)
            for name, value in metrics.items()
        ]
        with self.transaction(immediate=True) as connection:
            connection.executemany(
                """INSERT INTO runtime_metrics(
                    captured_at,metric_name,metric_value,labels_json
                ) VALUES (?, ?, ?, ?)""",
                rows,
            )

    def operational_counts(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            pending_receipts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM receipt_outbox WHERE delivered_at IS NULL"
                ).fetchone()[0]
            )
            incomplete = len(self.incomplete_ticket_ids())
            failed = int(
                connection.execute(
                    "SELECT COUNT(*) FROM tickets WHERE state='FAILED'"
                ).fetchone()[0]
            )
            order_count = int(connection.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0])
            distinct_orders = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT order_link_id) FROM execution_orders"
                ).fetchone()[0]
            )
        return {
            "receipt_outbox_backlog": pending_receipts,
            "incomplete_ticket_count": incomplete,
            "failed_ticket_count": failed,
            "duplicate_order_count": order_count - distinct_orders,
        }

    def set_kill_switch(self, enabled: bool) -> None:
        today = utc_now().date().isoformat()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO risk_runtime(risk_date, kill_switch, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(risk_date) DO UPDATE SET kill_switch=excluded.kill_switch,
                   updated_at=excluded.updated_at""",
                (today, int(enabled), iso(utc_now())),
            )

    def kill_switch_enabled(self) -> bool:
        today = utc_now().date().isoformat()
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT kill_switch FROM risk_runtime WHERE risk_date=?", (today,)
            ).fetchone()
        return bool(row and row["kill_switch"])

    def risk_runtime(self, at: Optional[datetime] = None) -> dict[str, Any]:
        day = (at or utc_now()).astimezone(timezone.utc).date().isoformat()
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM risk_runtime WHERE risk_date=?", (day,)
            ).fetchone()
        if row:
            return dict(row)
        return {
            "risk_date": day,
            "realised_pnl": 0.0,
            "unrealised_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": None,
            "kill_switch": 0,
            "updated_at": iso(utc_now()),
        }

    def observe_equity(self, equity_usdt: float) -> float:
        """Persist and return the monotonic account-equity high-water mark."""
        observed = float(equity_usdt)
        if observed <= 0:
            raise ValueError("equity observation must be positive")
        now = iso(utc_now())
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT equity_high_water_usdt FROM equity_runtime WHERE singleton=1"
            ).fetchone()
            high_water = max(observed, float(row["equity_high_water_usdt"]) if row else observed)
            connection.execute(
                """INSERT INTO equity_runtime(
                    singleton, equity_high_water_usdt, latest_equity_usdt, updated_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    equity_high_water_usdt=excluded.equity_high_water_usdt,
                    latest_equity_usdt=excluded.latest_equity_usdt,
                    updated_at=excluded.updated_at""",
                (high_water, observed, now),
            )
        return high_water

    def update_risk_runtime(
        self,
        *,
        realised_pnl: float,
        unrealised_pnl: float,
        trade_pnl: Optional[float] = None,
        cooldown_minutes: int = 30,
    ) -> None:
        now = utc_now()
        day = now.date().isoformat()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT consecutive_losses, kill_switch FROM risk_runtime WHERE risk_date=?", (day,)
            ).fetchone()
            losses = int(row["consecutive_losses"] if row else 0)
            kill_switch = int(row["kill_switch"] if row else 0)
            cooldown_until = None
            if trade_pnl is not None:
                losses = losses + 1 if trade_pnl < 0 else 0
                if trade_pnl < 0:
                    cooldown_until = iso(now + timedelta(minutes=max(1, cooldown_minutes)))
            connection.execute(
                """INSERT INTO risk_runtime(
                    risk_date, realised_pnl, unrealised_pnl, consecutive_losses,
                    cooldown_until, kill_switch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(risk_date) DO UPDATE SET
                    realised_pnl=excluded.realised_pnl,
                    unrealised_pnl=excluded.unrealised_pnl,
                    consecutive_losses=excluded.consecutive_losses,
                    cooldown_until=COALESCE(excluded.cooldown_until, risk_runtime.cooldown_until),
                    kill_switch=excluded.kill_switch,
                    updated_at=excluded.updated_at""",
                (
                    day, realised_pnl, unrealised_pnl, losses,
                    cooldown_until, kill_switch, iso(now),
                ),
            )

    def synchronize_risk_runtime(
        self,
        *,
        realised_pnl: float,
        unrealised_pnl: float,
        consecutive_losses: int,
        last_loss_at: Optional[datetime] = None,
        cooldown_minutes: int = 30,
    ) -> None:
        """Replace today's risk snapshot from a replayable account-ledger query."""

        now = utc_now()
        day = now.date().isoformat()
        cooldown_until = None
        if consecutive_losses > 0 and last_loss_at is not None:
            cooldown_until = iso(
                last_loss_at.astimezone(timezone.utc)
                + timedelta(minutes=max(1, int(cooldown_minutes)))
            )
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT kill_switch FROM risk_runtime WHERE risk_date=?", (day,)
            ).fetchone()
            kill_switch = int(row["kill_switch"] if row else 0)
            connection.execute(
                """INSERT INTO risk_runtime(
                    risk_date, realised_pnl, unrealised_pnl, consecutive_losses,
                    cooldown_until, kill_switch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(risk_date) DO UPDATE SET
                    realised_pnl=excluded.realised_pnl,
                    unrealised_pnl=excluded.unrealised_pnl,
                    consecutive_losses=excluded.consecutive_losses,
                    cooldown_until=excluded.cooldown_until,
                    kill_switch=excluded.kill_switch,
                    updated_at=excluded.updated_at""",
                (
                    day,
                    float(realised_pnl),
                    float(unrealised_pnl),
                    max(0, int(consecutive_losses)),
                    cooldown_until,
                    kill_switch,
                    iso(now),
                ),
            )

    def ticket_events(self, ticket_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM ticket_events WHERE ticket_id=? ORDER BY sequence", (ticket_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def ticket_record(self, ticket_id: str) -> Optional[dict[str, Any]]:
        with closing(self.connect()) as connection:
            row = connection.execute("SELECT * FROM tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        return dict(row) if row else None

    def orders_for_ticket(self, ticket_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM execution_orders WHERE ticket_id=? ORDER BY created_at, order_link_id",
                (ticket_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def fills_for_ticket(self, ticket_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT f.* FROM execution_fills f
                   JOIN execution_orders o ON o.order_link_id=f.order_link_id
                   WHERE o.ticket_id=? ORDER BY f.executed_at, f.exec_id""",
                (ticket_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def enqueue_receipt(self, receipt_id: str, ticket_id: str, payload: dict[str, Any]) -> bool:
        payload_json, payload_hash = canonical(payload)
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT payload_sha256 FROM receipt_outbox WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
            if row:
                if row["payload_sha256"] != payload_hash:
                    raise TicketConflict("receipt_id already exists with different content")
                return False
            connection.execute(
                """INSERT INTO receipt_outbox(
                    receipt_id, ticket_id, payload_json, payload_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?)""",
                (receipt_id, ticket_id, payload_json, payload_hash, iso(utc_now())),
            )
            return True

    def pending_receipts(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT sequence, receipt_id, ticket_id, payload_json
                   FROM receipt_outbox WHERE delivered_at IS NULL ORDER BY sequence LIMIT ?""",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        return [
            {
                "sequence": row["sequence"],
                "receipt_id": row["receipt_id"],
                "ticket_id": row["ticket_id"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def mark_receipt_delivered(self, receipt_id: str) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE receipt_outbox SET delivered_at=? WHERE receipt_id=?",
                (iso(utc_now()), receipt_id),
            )

    def consumer_cursor(self, consumer_id: str) -> int:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT cursor FROM consumer_cursors WHERE consumer_id=?", (consumer_id,)
            ).fetchone()
        return int(row["cursor"]) if row else 0

    def advance_consumer_cursor(self, consumer_id: str, cursor: int) -> bool:
        if cursor < 0:
            raise ValueError("cursor cannot be negative")
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT cursor FROM consumer_cursors WHERE consumer_id=?", (consumer_id,)
            ).fetchone()
            if row and int(row["cursor"]) >= cursor:
                return False
            connection.execute(
                """INSERT INTO consumer_cursors(consumer_id, cursor, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(consumer_id) DO UPDATE SET cursor=excluded.cursor,
                   updated_at=excluded.updated_at""",
                (consumer_id, cursor, iso(utc_now())),
            )
            return True
