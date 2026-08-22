from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .registry import FactorDefinition, FactorRegistry


def utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("feature timestamps must include timezone")
    return value.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return utc(value).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


class FeatureObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    observation_id: str = Field(min_length=8, max_length=100)
    name: str = Field(min_length=1, max_length=160)
    value: Optional[float]
    unit: str
    event_time: datetime
    published_at: datetime
    available_at: datetime
    ingested_at: datetime
    source: str
    source_tier: Literal["A", "B", "C"]
    revision_id: str = "original"
    quality: float = Field(ge=0, le=1)
    missing_reason: Optional[str] = None
    source_reliability: float = Field(default=1.0, ge=0, le=1)
    label_revision_risk: float = Field(default=0.0, ge=0, le=1)
    confirmation_count: int = Field(default=1, ge=0)
    impact_half_life_sec: Optional[int] = Field(default=None, gt=0)

    @field_validator("event_time", "published_at", "available_at", "ingested_at")
    @classmethod
    def timezone_required(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def chronology_and_missing(self):
        if not (self.event_time <= self.published_at <= self.available_at <= self.ingested_at):
            raise ValueError("feature timestamps must satisfy event <= published <= available <= ingested")
        if self.value is None and not self.missing_reason:
            raise ValueError("missing feature values require missing_reason")
        if self.value is not None and self.missing_reason:
            raise ValueError("present feature values cannot carry missing_reason")
        return self


@dataclass(frozen=True)
class SnapshotValue:
    name: str
    value: Optional[float]
    unit: str
    observation_id: Optional[str]
    semantics: str
    quality: float
    staleness_sec: Optional[int]
    missing_reason: Optional[str]
    available_at: Optional[datetime]
    revision_id: Optional[str]


@dataclass(frozen=True)
class FeatureSnapshot:
    snapshot_id: str
    simulated_time: datetime
    created_at: datetime
    values: dict[str, SnapshotValue]
    data_coverage: float
    data_quality: float
    max_feature_age_sec: int
    status: Literal["ok", "degraded", "blocked"]
    warnings: tuple[str, ...]


class ObservationConflict(RuntimeError):
    pass


class PointInTimeFeatureStore:
    def __init__(self, db_path: Path, registry: FactorRegistry):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry = registry
        self._lock = threading.Lock()
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
        with self._lock, self.transaction(immediate=True) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_observations(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    value REAL,
                    unit TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_tier TEXT NOT NULL,
                    revision_id TEXT NOT NULL,
                    quality REAL NOT NULL,
                    missing_reason TEXT,
                    source_reliability REAL NOT NULL,
                    label_revision_risk REAL NOT NULL,
                    confirmation_count INTEGER NOT NULL,
                    impact_half_life_sec INTEGER,
                    payload_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_observation_pit
                    ON raw_observations(name, available_at, sequence);
                CREATE INDEX IF NOT EXISTS idx_observation_event
                    ON raw_observations(name, event_time, revision_id);
                CREATE TABLE IF NOT EXISTS feature_snapshots(
                    snapshot_id TEXT PRIMARY KEY,
                    simulated_time TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    data_coverage REAL NOT NULL,
                    data_quality REAL NOT NULL,
                    max_feature_age_sec INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    warnings_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS feature_values(
                    snapshot_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    observation_id TEXT,
                    value REAL,
                    quality REAL NOT NULL,
                    staleness_sec INTEGER,
                    missing_reason TEXT,
                    PRIMARY KEY(snapshot_id, name),
                    FOREIGN KEY(snapshot_id) REFERENCES feature_snapshots(snapshot_id)
                );
                CREATE TABLE IF NOT EXISTS data_quality_events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    recovered_at TEXT
                );
                """
            )

    def append(self, observation: FeatureObservation) -> bool:
        definition = self.registry.require(observation.name)
        if definition.unit != observation.unit:
            raise ValueError(f"unit mismatch for {observation.name}: {observation.unit} != {definition.unit}")
        payload = observation.model_dump(mode="json")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT payload_sha256 FROM raw_observations WHERE observation_id=?",
                (observation.observation_id,),
            ).fetchone()
            if row:
                if row["payload_sha256"] != digest:
                    raise ObservationConflict("observation_id already exists with different content")
                return False
            connection.execute(
                """INSERT INTO raw_observations(
                    observation_id, name, value, unit, event_time, published_at, available_at,
                    ingested_at, source, source_tier, revision_id, quality, missing_reason,
                    source_reliability, label_revision_risk, confirmation_count,
                    impact_half_life_sec, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    observation.observation_id, observation.name, observation.value, observation.unit,
                    iso(observation.event_time), iso(observation.published_at), iso(observation.available_at),
                    iso(observation.ingested_at), observation.source, observation.source_tier,
                    observation.revision_id, observation.quality, observation.missing_reason,
                    observation.source_reliability, observation.label_revision_risk,
                    observation.confirmation_count, observation.impact_half_life_sec, digest,
                ),
            )
            return True

    def source_event(self, source: str, status: str, reason: str, available_at: datetime) -> None:
        if status not in {"ok", "degraded", "outage"}:
            raise ValueError("unsupported source status")
        with self.transaction(immediate=True) as connection:
            if status == "ok":
                connection.execute(
                    """UPDATE data_quality_events SET recovered_at=?
                       WHERE source=? AND recovered_at IS NULL""",
                    (iso(available_at), source),
                )
            else:
                connection.execute(
                    "INSERT INTO data_quality_events(source, status, reason, available_at) VALUES (?, ?, ?, ?)",
                    (source, status, reason, iso(available_at)),
                )

    def _active_outages(self, simulated_time: datetime) -> list[str]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """SELECT source, reason FROM data_quality_events
                   WHERE status='outage' AND available_at<=?
                     AND (recovered_at IS NULL OR recovered_at>?)""",
                (iso(simulated_time), iso(simulated_time)),
            ).fetchall()
        return [f"source_outage:{row['source']}:{row['reason']}" for row in rows]

    def snapshot(
        self,
        names: list[str],
        simulated_time: datetime,
        *,
        minimum_coverage: float = 0.9,
        minimum_quality: float = 0.8,
    ) -> FeatureSnapshot:
        cutoff = utc(simulated_time)
        definitions = [self.registry.require(name) for name in names]
        values: dict[str, SnapshotValue] = {}
        warnings = self._active_outages(cutoff)
        qualities: list[float] = []
        ages: list[int] = []
        with closing(self.connect()) as connection:
            for definition in definitions:
                row = connection.execute(
                    """SELECT * FROM raw_observations
                       WHERE name=? AND available_at<=?
                       ORDER BY available_at DESC, sequence DESC LIMIT 1""",
                    (definition.name, iso(cutoff)),
                ).fetchone()
                if row is None:
                    values[definition.name] = SnapshotValue(
                        definition.name, None, definition.unit, None, definition.semantics,
                        0.0, None, "not_available_at_cutoff", None, None,
                    )
                    warnings.append(f"missing:{definition.name}")
                    continue
                age = max(0, int((cutoff - parse_time(row["available_at"])).total_seconds()))
                missing_reason = row["missing_reason"]
                effective_quality = float(row["quality"]) * float(row["source_reliability"])
                if age > definition.maximum_age_sec:
                    missing_reason = missing_reason or "stale_at_cutoff"
                    warnings.append(f"stale:{definition.name}:{age}")
                if effective_quality < definition.minimum_quality:
                    warnings.append(f"low_quality:{definition.name}:{effective_quality:.3f}")
                values[definition.name] = SnapshotValue(
                    name=definition.name,
                    value=row["value"],
                    unit=row["unit"],
                    observation_id=row["observation_id"],
                    semantics=definition.semantics,
                    quality=effective_quality,
                    staleness_sec=age,
                    missing_reason=missing_reason,
                    available_at=parse_time(row["available_at"]),
                    revision_id=row["revision_id"],
                )
                if row["value"] is not None and missing_reason is None:
                    qualities.append(effective_quality)
                    ages.append(age)

        total = len(definitions)
        usable = len(qualities)
        coverage = usable / total if total else 1.0
        quality = sum(qualities) / usable if usable else 0.0
        max_age = max(ages) if ages else 0
        if warnings and any(item.startswith("source_outage:") for item in warnings):
            status = "blocked"
        elif coverage < minimum_coverage or quality < minimum_quality:
            status = "blocked"
        elif warnings:
            status = "degraded"
        else:
            status = "ok"
        identity = json.dumps(
            {"cutoff": iso(cutoff), "values": {name: value.observation_id for name, value in values.items()}},
            sort_keys=True,
        )
        snapshot_id = f"fs_{hashlib.sha256(identity.encode()).hexdigest()[:32]}"
        created_at = datetime.now(timezone.utc)
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT OR IGNORE INTO feature_snapshots(
                    snapshot_id, simulated_time, created_at, data_coverage, data_quality,
                    max_feature_age_sec, status, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (snapshot_id, iso(cutoff), iso(created_at), coverage, quality, max_age, status, json.dumps(warnings)),
            )
            for value in values.values():
                connection.execute(
                    """INSERT OR IGNORE INTO feature_values(
                        snapshot_id, name, observation_id, value, quality, staleness_sec, missing_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot_id, value.name, value.observation_id, value.value,
                        value.quality, value.staleness_sec, value.missing_reason,
                    ),
                )
        return FeatureSnapshot(
            snapshot_id, cutoff, created_at, values, coverage, quality, max_age, status, tuple(warnings)
        )
