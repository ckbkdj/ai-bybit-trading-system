from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from contracts.common import sortable_id
from contracts.event_impact_v1 import EventImpactVector

from .research_job import ResearchState, require_research_transition


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime must include timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical(payload: Any) -> tuple[str, str]:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text, hashlib.sha256(text.encode()).hexdigest()


class ResearchJobStore:
    """Checkpointed slow-research control plane with no task duration limit."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.migrate()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
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
        with self._lock, self.transaction(immediate=True) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS source_events(
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    data_cutoff TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_sources(
                    source_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    source_tier TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    verified INTEGER NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES source_events(event_id)
                );
                CREATE TABLE IF NOT EXISTS research_jobs(
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_checkpoint_at TEXT NOT NULL,
                    data_cutoff TEXT NOT NULL,
                    event_ids_json TEXT NOT NULL,
                    source_count INTEGER NOT NULL DEFAULT 0,
                    primary_source_verified INTEGER NOT NULL DEFAULT 0,
                    current_revision INTEGER NOT NULL DEFAULT 0,
                    superseded_by_job_id TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS research_checkpoints(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES research_jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS research_job_revisions(
                    job_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, revision),
                    FOREIGN KEY(job_id) REFERENCES research_jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS event_asset_links(
                    job_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    impact_strength REAL NOT NULL,
                    half_life_sec INTEGER NOT NULL,
                    event_blackout INTEGER NOT NULL,
                    blackout_until TEXT,
                    PRIMARY KEY(job_id, revision, asset),
                    FOREIGN KEY(job_id) REFERENCES research_jobs(job_id)
                );
                """
            )

    def create_job(self, event_ids: list[str], data_cutoff: datetime) -> str:
        if not event_ids:
            raise ValueError("research job requires at least one event_id")
        job_id = sortable_id("job")
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO research_jobs(
                    job_id, status, started_at, last_checkpoint_at, data_cutoff, event_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    job_id, ResearchState.DETECTED.value, iso(now), iso(now),
                    iso(data_cutoff), json.dumps(event_ids),
                ),
            )
            self._checkpoint(connection, job_id, ResearchState.DETECTED, {"event_ids": event_ids}, now)
        return job_id

    @staticmethod
    def _checkpoint(
        connection: sqlite3.Connection,
        job_id: str,
        status: ResearchState,
        payload: dict[str, Any],
        now: datetime,
    ) -> None:
        text, digest = canonical(payload)
        connection.execute(
            """INSERT INTO research_checkpoints(
                job_id, status, payload_json, payload_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?)""",
            (job_id, status.value, text, digest, iso(now)),
        )

    def transition(
        self,
        job_id: str,
        target: ResearchState,
        checkpoint: dict[str, Any],
        *,
        source_count: Optional[int] = None,
        primary_source_verified: Optional[bool] = None,
        error: Optional[str] = None,
    ) -> None:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT status FROM research_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            current = ResearchState(row["status"])
            require_research_transition(current, target)
            connection.execute(
                """UPDATE research_jobs SET status=?, last_checkpoint_at=?,
                   source_count=COALESCE(?, source_count),
                   primary_source_verified=COALESCE(?, primary_source_verified),
                   error=COALESCE(?, error) WHERE job_id=?""",
                (
                    target.value, iso(now), source_count,
                    None if primary_source_verified is None else int(primary_source_verified),
                    error, job_id,
                ),
            )
            self._checkpoint(connection, job_id, target, checkpoint, now)

    def save_revision(self, job_id: str, vector: EventImpactVector) -> int:
        data = vector.model_dump(mode="json")
        payload_json, payload_hash = canonical(data)
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT status, current_revision FROM research_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            if ResearchState(row["status"]) != ResearchState.IMPACT_QUANTIFYING:
                raise ValueError("revisions may be saved only while impact is being quantified")
            if not connection.execute(
                "SELECT 1 FROM source_events WHERE event_id=?", (vector.event_id,)
            ).fetchone():
                raise ValueError("event impact vector references an unknown event")
            verified_primary = connection.execute(
                """SELECT COUNT(*) AS count FROM event_sources
                   WHERE event_id=? AND source_tier='A' AND verified=1""",
                (vector.event_id,),
            ).fetchone()["count"]
            if vector.primary_source_verified and not verified_primary:
                raise ValueError("primary_source_verified requires a verified Tier A source")
            if vector.event_blackout and not verified_primary:
                raise ValueError("event blackout requires a verified Tier A source")
            revision = int(row["current_revision"]) + 1
            if vector.revision != revision:
                raise ValueError(f"event vector revision must be {revision}")
            connection.execute(
                """INSERT INTO research_job_revisions(
                    job_id, revision, event_id, payload_json, payload_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (job_id, revision, vector.event_id, payload_json, payload_hash, iso(now)),
            )
            for asset, impact in vector.affected_assets.items():
                connection.execute(
                    """INSERT INTO event_asset_links(
                        job_id, event_id, revision, asset, impact_strength, half_life_sec,
                        event_blackout, blackout_until
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        job_id, vector.event_id, revision, asset.upper(), impact.impact_strength,
                        impact.half_life_sec, int(vector.event_blackout),
                        iso(vector.blackout_until) if vector.blackout_until else None,
                    ),
                )
            connection.execute(
                "UPDATE research_jobs SET current_revision=?, last_checkpoint_at=? WHERE job_id=?",
                (revision, iso(now), job_id),
            )
            self._checkpoint(
                connection, job_id, ResearchState.IMPACT_QUANTIFYING,
                {"revision": revision, "event_id": vector.event_id}, now,
            )
        return revision

    def record_event(
        self,
        event_id: str,
        event_type: str,
        detected_at: datetime,
        data_cutoff: datetime,
        payload: dict[str, Any],
    ) -> bool:
        payload_json, payload_hash = canonical(payload)
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT payload_sha256 FROM source_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row:
                if row["payload_sha256"] != payload_hash:
                    raise ValueError("event_id already exists with different content")
                return False
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, event_type, iso(detected_at), iso(data_cutoff), payload_json, payload_hash),
            )
            return True

    def add_event_source(
        self,
        event_id: str,
        *,
        source_tier: str,
        source_uri: str,
        published_at: datetime,
        verified: bool,
        content: dict[str, Any],
    ) -> str:
        if source_tier not in {"A", "B", "C"}:
            raise ValueError("source tier must be A, B or C")
        content_json, content_hash = canonical(content)
        source_id = f"src_{hashlib.sha256(f'{event_id}|{source_uri}|{content_hash}'.encode()).hexdigest()[:32]}"
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT OR IGNORE INTO event_sources(
                    source_id, event_id, source_tier, source_uri, published_at, verified, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    source_id, event_id, source_tier, source_uri,
                    iso(published_at), int(verified), content_hash,
                ),
            )
        return source_id

    def complete(self, job_id: str) -> None:
        self.transition(job_id, ResearchState.COMPLETED, {"completed": True})

    def supersede(self, old_job_id: str, new_job_id: str) -> None:
        now = utc_now()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT status FROM research_jobs WHERE job_id=?", (old_job_id,)
            ).fetchone()
            if not row:
                raise KeyError(old_job_id)
            current = ResearchState(row["status"])
            require_research_transition(current, ResearchState.SUPERSEDED)
            connection.execute(
                """UPDATE research_jobs SET status=?, superseded_by_job_id=?,
                   last_checkpoint_at=? WHERE job_id=?""",
                (ResearchState.SUPERSEDED.value, new_job_id, iso(now), old_job_id),
            )
            self._checkpoint(
                connection, old_job_id, ResearchState.SUPERSEDED,
                {"superseded_by_job_id": new_job_id}, now,
            )

    def get(self, job_id: str) -> Optional[dict[str, Any]]:
        with closing(self.connect()) as connection:
            row = connection.execute("SELECT * FROM research_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["event_ids"] = json.loads(result.pop("event_ids_json"))
        return result

    def checkpoints(self, job_id: str) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM research_checkpoints WHERE job_id=? ORDER BY sequence", (job_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def revisions(self, job_id: str) -> list[EventImpactVector]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT payload_json FROM research_job_revisions WHERE job_id=? ORDER BY revision", (job_id,)
            ).fetchall()
        return [EventImpactVector.model_validate_json(row["payload_json"]) for row in rows]

    def event_blackout(self, asset: str, at: datetime) -> bool:
        with closing(self.connect()) as connection:
            row = connection.execute(
                """SELECT 1 FROM event_asset_links
                   WHERE asset=? AND event_blackout=1 AND blackout_until>?
                   ORDER BY blackout_until DESC LIMIT 1""",
                (asset.strip().upper(), iso(at)),
            ).fetchone()
        return row is not None
