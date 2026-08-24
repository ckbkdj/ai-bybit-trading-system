from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from core.providers.bybit_public_pit import BybitPublicPITStore, CaptureConflict


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("capture audit timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored capture timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class LiveCaptureAuditEvidence:
    audit_id: str
    created_at: str
    snapshot_maximum_raw_sequence: int
    snapshot_maximum_feature_sequence: int
    snapshot_maximum_invalidation_rowid: int
    first_received_at: str
    last_received_at: str
    maximum_gap_sec: float
    raw_event_count: int
    symbols: tuple[str, ...]
    topic_counts: Mapping[str, int]
    event_type_counts: Mapping[str, int]
    interval_count: int
    longest_interval_sec: float
    manifest_sha256: str
    status: str = "completed"
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PITImportEvidence:
    import_id: str
    imported_at: str
    source_database: str
    source_audit_id: str
    selection: Mapping[str, object]
    source_counts: Mapping[str, int]
    inserted_counts: Mapping[str, int]
    manifest_sha256: str
    status: str = "completed"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _watermarks(connection: sqlite3.Connection) -> tuple[int, int, int]:
    raw = int(
        connection.execute(
            "SELECT COALESCE(MAX(sequence),0) FROM bybit_raw_public_events"
        ).fetchone()[0]
    )
    feature = int(
        connection.execute(
            "SELECT COALESCE(MAX(sequence),0) FROM bybit_feature_observations"
        ).fetchone()[0]
    )
    invalidation = int(
        connection.execute(
            "SELECT COALESCE(MAX(rowid),0) FROM bybit_feature_invalidations"
        ).fetchone()[0]
    )
    return raw, feature, invalidation


def audit_live_capture(
    store: BybitPublicPITStore,
    *,
    maximum_gap_sec: float = 90.0,
) -> LiveCaptureAuditEvidence:
    """Seal stopped public-WebSocket activity into hashed continuity intervals."""

    if maximum_gap_sec <= 0 or maximum_gap_sec > 300:
        raise ValueError("live capture audit maximum gap must be in (0, 300] seconds")
    store.flush()
    with closing(store.connect()) as source:
        running = int(
            source.execute(
                "SELECT COUNT(*) FROM bybit_capture_sessions WHERE status='running'"
            ).fetchone()[0]
        )
        if running:
            raise RuntimeError("running capture sessions cannot be sealed as release evidence")
        maximum_raw, maximum_feature, maximum_invalidation = _watermarks(source)
        if maximum_raw <= 0:
            raise RuntimeError("cannot audit an empty Bybit raw event journal")
        sessions = source.execute(
            """SELECT session_id,endpoint,symbols_json,started_at,ended_at,status,error
                   FROM bybit_capture_sessions
                  ORDER BY started_at,session_id"""
        ).fetchall()

        digest = hashlib.sha256()
        for row in sessions:
            digest.update(_canonical(dict(row)).encode("utf-8"))
        topic_counts: Counter[str] = Counter()
        event_type_counts: Counter[str] = Counter()
        symbols: set[str] = set()
        intervals: list[dict[str, object]] = []
        interval_start: datetime | None = None
        interval_end: datetime | None = None
        interval_events = 0
        prior_received: datetime | None = None
        raw_event_count = 0

        for session in sessions:
            rows = source.execute(
                """SELECT sequence,event_id,session_id,topic,symbol,event_type,
                          received_at,payload_json,payload_sha256
                     FROM bybit_raw_public_events
                    WHERE session_id=? AND sequence<=?
                    ORDER BY received_at,sequence""",
                (session["session_id"], maximum_raw),
            )
            for row in rows:
                encoded = str(row["payload_json"]).encode("utf-8")
                if hashlib.sha256(encoded).hexdigest() != str(row["payload_sha256"]):
                    raise CaptureConflict("raw public event payload hash mismatch")
                received = _timestamp(row["received_at"])
                if prior_received is not None and received < prior_received:
                    raise RuntimeError("capture sessions overlap or raw chronology is reversed")
                if (
                    prior_received is None
                    or (received - prior_received).total_seconds() > maximum_gap_sec
                ):
                    if interval_start is not None and interval_end is not None:
                        intervals.append(
                            {
                                "started_at": _iso(interval_start),
                                "ended_at": _iso(interval_end),
                                "raw_event_count": interval_events,
                            }
                        )
                    interval_start = received
                    interval_events = 0
                interval_end = received
                interval_events += 1
                prior_received = received
                raw_event_count += 1
                topic_counts[str(row["topic"])] += 1
                event_type_counts[str(row["event_type"])] += 1
                symbols.add(str(row["symbol"]).upper())
                digest.update(
                    _canonical(
                        {
                            "sequence": int(row["sequence"]),
                            "event_id": row["event_id"],
                            "session_id": row["session_id"],
                            "topic": row["topic"],
                            "symbol": row["symbol"],
                            "event_type": row["event_type"],
                            "received_at": row["received_at"],
                            "payload_sha256": row["payload_sha256"],
                        }
                    ).encode("utf-8")
                )
        if interval_start is not None and interval_end is not None:
            intervals.append(
                {
                    "started_at": _iso(interval_start),
                    "ended_at": _iso(interval_end),
                    "raw_event_count": interval_events,
                }
            )
        if not intervals:
            raise RuntimeError("capture audit found no raw event intervals")
        manifest_sha256 = digest.hexdigest()
        longest_interval_sec = max(
            (_timestamp(item["ended_at"]) - _timestamp(item["started_at"])).total_seconds()
            for item in intervals
        )
        audit_id = "bca_" + hashlib.sha256(
            _canonical(
                {
                    "maximum_gap_sec": maximum_gap_sec,
                    "maximum_raw": maximum_raw,
                    "maximum_feature": maximum_feature,
                    "maximum_invalidation": maximum_invalidation,
                    "manifest_sha256": manifest_sha256,
                }
            ).encode("utf-8")
        ).hexdigest()[:48]
        now = _iso(datetime.now(timezone.utc))
        evidence = LiveCaptureAuditEvidence(
            audit_id=audit_id,
            created_at=now,
            snapshot_maximum_raw_sequence=maximum_raw,
            snapshot_maximum_feature_sequence=maximum_feature,
            snapshot_maximum_invalidation_rowid=maximum_invalidation,
            first_received_at=str(intervals[0]["started_at"]),
            last_received_at=str(intervals[-1]["ended_at"]),
            maximum_gap_sec=float(maximum_gap_sec),
            raw_event_count=raw_event_count,
            symbols=tuple(sorted(symbols)),
            topic_counts=dict(sorted(topic_counts.items())),
            event_type_counts=dict(sorted(event_type_counts.items())),
            interval_count=len(intervals),
            longest_interval_sec=float(longest_interval_sec),
            manifest_sha256=manifest_sha256,
        )

    with store.connect() as destination:
        destination.execute("BEGIN IMMEDIATE")
        if _watermarks(destination) != (
            maximum_raw,
            maximum_feature,
            maximum_invalidation,
        ):
            raise CaptureConflict("Bybit journals changed while capture audit was running")
        if destination.execute(
            "SELECT 1 FROM bybit_capture_sessions WHERE status='running' LIMIT 1"
        ).fetchone():
            raise CaptureConflict("a capture session started while audit was being sealed")
        existing = destination.execute(
            "SELECT manifest_sha256 FROM bybit_live_capture_audits WHERE audit_id=?",
            (audit_id,),
        ).fetchone()
        if existing and str(existing["manifest_sha256"]) != manifest_sha256:
            raise CaptureConflict("capture audit id already has another manifest")
        destination.execute(
            """INSERT OR IGNORE INTO bybit_live_capture_audits(
                   audit_id,created_at,snapshot_maximum_raw_sequence,
                   snapshot_maximum_feature_sequence,snapshot_maximum_invalidation_rowid,
                   first_received_at,last_received_at,maximum_gap_sec,raw_event_count,
                   symbols_json,topic_counts_json,event_type_counts_json,interval_count,
                   longest_interval_sec,manifest_sha256,status,error
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                audit_id,
                now,
                maximum_raw,
                maximum_feature,
                maximum_invalidation,
                evidence.first_received_at,
                evidence.last_received_at,
                maximum_gap_sec,
                raw_event_count,
                _canonical(evidence.symbols),
                _canonical(evidence.topic_counts),
                _canonical(evidence.event_type_counts),
                len(intervals),
                longest_interval_sec,
                manifest_sha256,
                "completed",
                None,
            ),
        )
        for index, interval in enumerate(intervals):
            destination.execute(
                """INSERT OR IGNORE INTO bybit_live_capture_intervals(
                       audit_id,interval_index,started_at,ended_at,raw_event_count
                   ) VALUES (?,?,?,?,?)""",
                (
                    audit_id,
                    index,
                    interval["started_at"],
                    interval["ended_at"],
                    interval["raw_event_count"],
                ),
            )
        destination.commit()
    return evidence


def _row_payload(row: sqlite3.Row, columns: Sequence[str]) -> tuple[object, ...]:
    return tuple(row[column] for column in columns)


def _copy_rows(
    destination: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    columns: Sequence[str],
    rows: Iterable[sqlite3.Row],
) -> tuple[int, int, list[dict[str, object]]]:
    inserted = 0
    total = 0
    manifest_rows: list[dict[str, object]] = []
    placeholders = ",".join("?" for _ in columns)
    for row in rows:
        total += 1
        payload = {column: row[column] for column in columns}
        manifest_rows.append(payload)
        existing = destination.execute(
            f"SELECT {','.join(columns)} FROM {table} WHERE {key_column}=?",
            (row[key_column],),
        ).fetchone()
        if existing:
            if _row_payload(existing, columns) != _row_payload(row, columns):
                raise CaptureConflict(f"{table} key already exists with different content")
            continue
        destination.execute(
            f"INSERT INTO {table}({','.join(columns)}) VALUES ({placeholders})",
            _row_payload(row, columns),
        )
        inserted += 1
    return total, inserted, manifest_rows


def merge_audited_liquidation_capture(
    source_path: Path,
    destination_path: Path,
    *,
    audit_id: str | None = None,
) -> PITImportEvidence:
    """Copy only sealed liquidation evidence into a historical development PIT store."""

    source_path = Path(source_path).expanduser().resolve()
    destination_path = Path(destination_path).expanduser().resolve()
    if source_path == destination_path:
        raise ValueError("source and destination Bybit PIT stores must differ")
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    destination_store = BybitPublicPITStore(destination_path)
    source_uri = f"file:{source_path.as_posix()}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True, timeout=30)) as source:
        source.row_factory = sqlite3.Row
        if audit_id is None:
            audit = source.execute(
                """SELECT * FROM bybit_live_capture_audits
                    WHERE status='completed'
                    ORDER BY longest_interval_sec DESC,created_at DESC LIMIT 1"""
            ).fetchone()
        else:
            audit = source.execute(
                "SELECT * FROM bybit_live_capture_audits WHERE audit_id=?",
                (audit_id,),
            ).fetchone()
        if not audit or str(audit["status"]) != "completed":
            raise RuntimeError("a completed live capture audit is required before merge")
        selected_audit_id = str(audit["audit_id"])
        maximum_raw = int(audit["snapshot_maximum_raw_sequence"])
        maximum_feature = int(audit["snapshot_maximum_feature_sequence"])
        maximum_invalidation = int(audit["snapshot_maximum_invalidation_rowid"])
        session_columns = (
            "session_id", "endpoint", "symbols_json", "started_at", "ended_at", "status", "error"
        )
        raw_columns = (
            "event_id", "session_id", "topic", "symbol", "event_type", "exchange_time",
            "received_at", "update_id", "cross_sequence", "book_state_valid", "payload_json",
            "payload_sha256",
        )
        feature_columns = (
            "observation_id", "symbol", "name", "value", "unit", "event_time", "available_at",
            "ingested_at", "source", "quality", "payload_sha256", "provenance_kind", "archive_id",
            "api_batch_id",
        )
        invalidation_columns = (
            "observation_id", "invalidated_at", "reason", "correction_version"
        )
        audit_columns = (
            "audit_id", "created_at", "snapshot_maximum_raw_sequence",
            "snapshot_maximum_feature_sequence", "snapshot_maximum_invalidation_rowid",
            "first_received_at", "last_received_at", "maximum_gap_sec", "raw_event_count",
            "symbols_json", "topic_counts_json", "event_type_counts_json", "interval_count",
            "longest_interval_sec", "manifest_sha256", "status", "error",
        )
        interval_columns = (
            "audit_id", "interval_index", "started_at", "ended_at", "raw_event_count"
        )
        queries = {
            "sessions": source.execute(
                f"""SELECT {','.join(session_columns)}
                       FROM bybit_capture_sessions
                      WHERE started_at<=?
                      ORDER BY started_at,session_id""",
                (audit["last_received_at"],),
            ).fetchall(),
            "raw_events": source.execute(
                f"""SELECT {','.join(raw_columns)}
                       FROM bybit_raw_public_events
                      WHERE sequence<=? AND event_type='liquidation'
                      ORDER BY sequence""",
                (maximum_raw,),
            ).fetchall(),
            "features": source.execute(
                f"""SELECT {','.join(feature_columns)}
                       FROM bybit_feature_observations
                      WHERE sequence<=? AND name='liquidation_imbalance_5m'
                      ORDER BY sequence""",
                (maximum_feature,),
            ).fetchall(),
            "audit": [audit],
            "intervals": source.execute(
                f"""SELECT {','.join(interval_columns)}
                       FROM bybit_live_capture_intervals
                      WHERE audit_id=? ORDER BY interval_index""",
                (selected_audit_id,),
            ).fetchall(),
        }
        observation_ids = [str(row["observation_id"]) for row in queries["features"]]
        if observation_ids:
            placeholders = ",".join("?" for _ in observation_ids)
            queries["invalidations"] = source.execute(
                f"""SELECT {','.join(invalidation_columns)}
                       FROM bybit_feature_invalidations
                      WHERE rowid<=? AND observation_id IN ({placeholders})
                      ORDER BY rowid""",
                (maximum_invalidation, *observation_ids),
            ).fetchall()
        else:
            queries["invalidations"] = []

    table_specs = (
        ("sessions", "bybit_capture_sessions", "session_id", session_columns),
        ("raw_events", "bybit_raw_public_events", "event_id", raw_columns),
        ("features", "bybit_feature_observations", "observation_id", feature_columns),
        ("invalidations", "bybit_feature_invalidations", "observation_id", invalidation_columns),
        ("audit", "bybit_live_capture_audits", "audit_id", audit_columns),
    )
    manifest = hashlib.sha256()
    source_counts: dict[str, int] = {}
    inserted_counts: dict[str, int] = {}
    with destination_store.connect() as destination:
        destination.execute("BEGIN IMMEDIATE")
        for label, table, key, columns in table_specs:
            total, inserted, rows = _copy_rows(
                destination,
                table=table,
                key_column=key,
                columns=columns,
                rows=queries[label],
            )
            source_counts[label] = total
            inserted_counts[label] = inserted
            for row in rows:
                manifest.update(_canonical({"table": table, "row": row}).encode("utf-8"))
        interval_total = 0
        interval_inserted = 0
        for row in queries["intervals"]:
            interval_total += 1
            existing = destination.execute(
                """SELECT audit_id,interval_index,started_at,ended_at,raw_event_count
                     FROM bybit_live_capture_intervals
                    WHERE audit_id=? AND interval_index=?""",
                (row["audit_id"], row["interval_index"]),
            ).fetchone()
            if existing:
                if _row_payload(existing, interval_columns) != _row_payload(row, interval_columns):
                    raise CaptureConflict("capture interval already exists with different content")
            else:
                destination.execute(
                    """INSERT INTO bybit_live_capture_intervals(
                           audit_id,interval_index,started_at,ended_at,raw_event_count
                       ) VALUES (?,?,?,?,?)""",
                    _row_payload(row, interval_columns),
                )
                interval_inserted += 1
            manifest.update(
                _canonical(
                    {"table": "bybit_live_capture_intervals", "row": dict(row)}
                ).encode("utf-8")
            )
        source_counts["intervals"] = interval_total
        inserted_counts["intervals"] = interval_inserted
        manifest_sha256 = manifest.hexdigest()
        selection = {
            "event_type": "liquidation",
            "feature": "liquidation_imbalance_5m",
            "snapshot_maximum_raw_sequence": maximum_raw,
            "snapshot_maximum_feature_sequence": maximum_feature,
            "snapshot_maximum_invalidation_rowid": maximum_invalidation,
        }
        import_id = "bpi_" + hashlib.sha256(
            _canonical(
                {
                    "source_audit_id": selected_audit_id,
                    "manifest_sha256": manifest_sha256,
                    "selection": selection,
                }
            ).encode("utf-8")
        ).hexdigest()[:48]
        now = _iso(datetime.now(timezone.utc))
        existing_import = destination.execute(
            "SELECT manifest_sha256 FROM bybit_pit_imports WHERE import_id=?",
            (import_id,),
        ).fetchone()
        if existing_import and str(existing_import["manifest_sha256"]) != manifest_sha256:
            raise CaptureConflict("PIT import id already exists with another manifest")
        destination.execute(
            """INSERT OR IGNORE INTO bybit_pit_imports(
                   import_id,imported_at,source_database,source_audit_id,selection_json,
                   source_counts_json,inserted_counts_json,manifest_sha256,status
               ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                import_id,
                now,
                str(source_path),
                selected_audit_id,
                _canonical(selection),
                _canonical(source_counts),
                _canonical(inserted_counts),
                manifest_sha256,
                "completed",
            ),
        )
        destination.commit()
    destination_store.close()
    return PITImportEvidence(
        import_id=import_id,
        imported_at=now,
        source_database=str(source_path),
        source_audit_id=selected_audit_id,
        selection=selection,
        source_counts=source_counts,
        inserted_counts=inserted_counts,
        manifest_sha256=manifest_sha256,
    )


__all__ = (
    "LiveCaptureAuditEvidence",
    "PITImportEvidence",
    "audit_live_capture",
    "merge_audited_liquidation_capture",
)
