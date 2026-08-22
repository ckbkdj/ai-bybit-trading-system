"""Read-only integrity audit for the project's historical SQLite evidence.

This is intentionally schema-aware.  It reports evidence quality and coverage; it
does not claim that a stored prediction was an executed trade.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


EXPECTED_MS = {"3m": 180_000, "15m": 900_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000}
EXPECTED_SECONDS = {key: value // 1_000 for key, value in EXPECTED_MS.items()}


@contextmanager
def connect_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        yield connection
    finally:
        connection.close()


def rows(connection: sqlite3.Connection, sql: str, parameters: Iterable[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(sql, tuple(parameters)).fetchall()]


def scalar(connection: sqlite3.Connection, sql: str, parameters: Iterable[Any] = ()) -> Any:
    row = connection.execute(sql, tuple(parameters)).fetchone()
    return None if row is None else row[0]


def capture(result: dict[str, Any], key: str, query) -> None:
    """Keep auditing other evidence when one corrupt table/page cannot be read."""

    try:
        result[key] = query()
    except sqlite3.DatabaseError as exc:
        result[f"{key}_error"] = f"{type(exc).__name__}: {exc}"


def fast_fingerprint(path: Path) -> dict[str, Any]:
    """Fingerprint metadata plus the first/last MiB without scanning multi-GB files."""

    size = path.stat().st_size
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read(1_048_576))
        if size > 1_048_576:
            handle.seek(max(0, size - 1_048_576))
            digest.update(handle.read(1_048_576))
    return {
        "path": path.as_posix(),
        "size_bytes": size,
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        "edge_sha256": digest.hexdigest(),
    }


def table_names(connection: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def audit_legacy_kline(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "legacy_kline_cache", "tables": []}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        for table in table_names(connection):
            if not table.startswith("k_"):
                continue
            timeframe = table[2:]
            expected = EXPECTED_SECONDS.get(timeframe)
            quoted = '"' + table.replace('"', '""') + '"'
            summary = dict(
                connection.execute(
                    f"""SELECT COUNT(*) AS rows, COUNT(DISTINCT ts) AS distinct_timestamps,
                        MIN(ts) AS first_timestamp, MAX(ts) AS last_timestamp,
                        SUM(CASE WHEN CAST(open AS REAL)<=0 OR CAST(high AS REAL)<=0
                            OR CAST(low AS REAL)<=0 OR CAST(close AS REAL)<=0
                            OR CAST(volume AS REAL)<0 THEN 1 ELSE 0 END) AS nonpositive_values,
                        SUM(CASE WHEN CAST(high AS REAL)<MAX(CAST(open AS REAL),CAST(close AS REAL),CAST(low AS REAL))
                            OR CAST(low AS REAL)>MIN(CAST(open AS REAL),CAST(close AS REAL),CAST(high AS REAL))
                            THEN 1 ELSE 0 END) AS invalid_ohlc
                        FROM {quoted}"""
                ).fetchone()
            )
            gap_summary = {"gap_count": None, "missing_intervals": None}
            if expected:
                gap_summary = dict(
                    connection.execute(
                        f"""WITH ordered AS (
                            SELECT CAST(strftime('%s', ts) AS INTEGER) AS epoch,
                                   LAG(CAST(strftime('%s', ts) AS INTEGER)) OVER (ORDER BY ts) AS previous
                            FROM {quoted})
                            SELECT SUM(CASE WHEN previous IS NOT NULL AND epoch-previous>{expected} THEN 1 ELSE 0 END) AS gap_count,
                                   SUM(CASE WHEN previous IS NOT NULL AND epoch-previous>{expected}
                                       THEN CAST((epoch-previous)/{expected} AS INTEGER)-1 ELSE 0 END) AS missing_intervals
                            FROM ordered"""
                    ).fetchone()
                )
            result["tables"].append({"timeframe": timeframe, **summary, **gap_summary})
    return result


def audit_feature_store(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "versioned_kline_feature_store"}
    with connect_read_only(path) as connection:
        capture(result, "quick_check", lambda: rows(connection, "PRAGMA quick_check"))
        capture(result, "raw_kline", lambda: rows(
            connection,
            """SELECT symbol,timeframe,source,COUNT(*) AS rows,MIN(open_time) AS first_open_time,
                MAX(open_time) AS last_open_time,
                SUM(CASE WHEN open<=0 OR high<=0 OR low<=0 OR close<=0 OR volume<0 THEN 1 ELSE 0 END) AS nonpositive_values,
                SUM(CASE WHEN high<MAX(open,close,low) OR low>MIN(open,close,high) THEN 1 ELSE 0 END) AS invalid_ohlc,
                SUM(CASE WHEN close_time<=open_time THEN 1 ELSE 0 END) AS invalid_close_time
                FROM raw_kline GROUP BY symbol,timeframe,source ORDER BY symbol,timeframe,source""",
        ))
        capture(result, "raw_gaps", lambda: rows(
            connection,
            """WITH ordered AS (
                    SELECT symbol,timeframe,source,open_time,
                           LAG(open_time) OVER (PARTITION BY symbol,timeframe,source ORDER BY open_time) AS previous
                    FROM raw_kline), classified AS (
                    SELECT symbol,timeframe,source,open_time-previous AS delta,
                           CASE timeframe WHEN '3m' THEN 180000 WHEN '15m' THEN 900000
                               WHEN '2h' THEN 7200000 WHEN '4h' THEN 14400000
                               WHEN '1d' THEN 86400000 END AS expected
                    FROM ordered WHERE previous IS NOT NULL)
                SELECT symbol,timeframe,source,
                       SUM(CASE WHEN expected IS NOT NULL AND delta>expected THEN 1 ELSE 0 END) AS gap_count,
                       SUM(CASE WHEN expected IS NOT NULL AND delta>expected THEN CAST(delta/expected AS INTEGER)-1 ELSE 0 END) AS missing_intervals,
                       SUM(CASE WHEN expected IS NOT NULL AND delta<expected THEN 1 ELSE 0 END) AS irregular_short_intervals
                FROM classified GROUP BY symbol,timeframe,source ORDER BY symbol,timeframe,source""",
        ))
        capture(result, "enhanced_kline", lambda: rows(
            connection,
            """SELECT symbol,timeframe,source,feature_version,schema_version,COUNT(*) AS rows,
                MIN(open_time) AS first_open_time,MAX(open_time) AS last_open_time,
                COUNT(DISTINCT config_hash) AS config_hashes,COUNT(DISTINCT feature_set_hash) AS feature_sets,
                SUM(CASE WHEN json_valid(features_json)=0 THEN 1 ELSE 0 END) AS invalid_feature_json
                FROM enhanced_kline GROUP BY symbol,timeframe,source,feature_version,schema_version
                ORDER BY symbol,timeframe,source,feature_version,schema_version""",
        ))
        capture(result, "model_registry_status", lambda: rows(
            connection,
            "SELECT model_kind,status,COUNT(*) AS records FROM model_registry GROUP BY model_kind,status ORDER BY model_kind,status",
        ))
        capture(result, "model_registry_total", lambda: scalar(connection, "SELECT COUNT(*) FROM model_registry"))
    return result


def audit_online_predictions(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "online_prediction_observations"}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        result["summary"] = dict(
            connection.execute(
                """SELECT COUNT(*) AS rows,MIN(created_at) AS first_created_at,MAX(created_at) AS last_created_at,
                    SUM(settled=1) AS settled_rows,
                    SUM(settled=1 AND actual_return IS NOT NULL) AS settled_with_actual_return,
                    SUM(settled=1 AND predicted_direction IS NOT NULL) AS settled_with_predicted_direction,
                    SUM(settled=1 AND actual_direction IS NOT NULL) AS settled_with_actual_direction,
                    SUM(settled=1 AND hit IS NOT NULL) AS settled_with_hit,
                    SUM(settled=1 AND cost_adjusted_return IS NOT NULL) AS settled_with_cost_adjusted_return,
                    SUM(settled=1 AND settled_at IS NOT NULL) AS settled_with_settled_at,
                    SUM(settled=0 AND settle_at<strftime('%s','now')) AS overdue_unsettled_rows
                    FROM predictions"""
            ).fetchone()
        )
        result["by_symbol_mode"] = rows(
            connection,
            """SELECT symbol,mode,timeframe,COALESCE(model_version,'<legacy-missing>') AS model_version,
                COUNT(*) AS rows,SUM(settled=1) AS settled_rows,
                SUM(settled=1 AND predicted_direction IS NOT NULL) AS direction_complete_rows
                FROM predictions GROUP BY symbol,mode,timeframe,COALESCE(model_version,'<legacy-missing>')
                ORDER BY symbol,mode,model_version""",
        )
        result["settled_return_diagnostics"] = rows(
            connection,
            """SELECT COALESCE(model_version,'<legacy-missing>') AS model_version,
                COUNT(*) AS rows,AVG(actual_return) AS mean_actual_return,
                MIN(actual_return) AS min_actual_return,MAX(actual_return) AS max_actual_return,
                AVG(cost_adjusted_return) AS mean_cost_adjusted_return,AVG(hit) AS hit_rate
                FROM predictions WHERE settled=1 GROUP BY COALESCE(model_version,'<legacy-missing>')
                ORDER BY model_version""",
        )
        result["duplicate_observation_rows"] = scalar(
            connection,
            """SELECT COALESCE(SUM(c-1),0) FROM (
                SELECT COUNT(*) AS c FROM predictions
                GROUP BY created_at,symbol,timeframe,mode,predicted_return,last_price,horizon_seconds
                HAVING COUNT(*)>1)""",
        )
    return result


def audit_brain_history(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "brain_training_history"}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        result["summary"] = dict(
            connection.execute(
                """SELECT COUNT(*) AS rows,MIN(created_at) AS first_created_at,MAX(created_at) AS last_created_at,
                    COUNT(DISTINCT data_signature) AS distinct_data_signatures,
                    SUM(status='trained') AS trained_events,
                    COUNT(DISTINCT CASE WHEN status='trained' THEN data_signature END) AS distinct_trained_signatures
                    FROM brain_training_runs"""
            ).fetchone()
        )
        result["by_symbol_mode_status"] = rows(
            connection,
            """SELECT symbol,mode,status,COUNT(*) AS events,COUNT(DISTINCT data_signature) AS distinct_signatures,
                MIN(rows) AS min_rows,MAX(rows) AS max_rows,MIN(feature_count) AS min_features,MAX(feature_count) AS max_features
                FROM brain_training_runs GROUP BY symbol,mode,status ORDER BY symbol,mode,status""",
        )
        result["repeated_trained_signatures"] = scalar(
            connection,
            """SELECT COALESCE(SUM(c-1),0) FROM (
                SELECT COUNT(*) AS c FROM brain_training_runs WHERE status='trained'
                GROUP BY symbol,mode,data_signature HAVING COUNT(*)>1)""",
        )
    return result


def audit_control_plane(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "control_plane"}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        result["table_counts"] = {table: scalar(connection, f'SELECT COUNT(*) FROM "{table}"') for table in table_names(connection)}
    return result


def audit_price_changes(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {**fast_fingerprint(path), "kind": "legacy_unlabelled_price_samples"}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        result["summary"] = dict(
            connection.execute(
                """SELECT COUNT(*) AS rows,COUNT(DISTINCT timestamp) AS distinct_timestamps,
                    MIN(timestamp) AS first_timestamp,MAX(timestamp) AS last_timestamp,
                    MIN(price) AS min_price,MAX(price) AS max_price,
                    SUM(price<=0) AS nonpositive_prices FROM price_changes"""
            ).fetchone()
        )
        result["adjacent_jumps_over_20pct"] = scalar(
            connection,
            """WITH ordered AS (SELECT timestamp,price,LAG(price) OVER (ORDER BY timestamp,id) AS previous FROM price_changes)
                SELECT SUM(previous>0 AND ABS(price/previous-1)>0.20) FROM ordered""",
        )
        result["structural_limit"] = "No symbol, side, quantity, fee, order ID or fill ID: this is not an execution ledger."
    return result


def audit_database(path: Path) -> dict[str, Any]:
    name = path.name.lower()
    if name.endswith("usdt.sqlite"):
        return audit_legacy_kline(path)
    if name.startswith("kline_feature_store") and name.endswith(".sqlite3"):
        return audit_feature_store(path)
    if name == "online_learning.sqlite3":
        return audit_online_predictions(path)
    if name == "brain_training_history.sqlite3":
        return audit_brain_history(path)
    if name == "control_plane.sqlite3":
        return audit_control_plane(path)
    if name in {"price_changes.db", "price_changes_old.db"}:
        return audit_price_changes(path)
    result = {**fast_fingerprint(path), "kind": "generic_sqlite"}
    with connect_read_only(path) as connection:
        result["quick_check"] = scalar(connection, "PRAGMA quick_check")
        result["table_counts"] = {table: scalar(connection, f'SELECT COUNT(*) FROM "{table}"') for table in table_names(connection)}
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("databases", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    missing = [str(path) for path in args.databases if not path.is_file()]
    if missing:
        parser.error(f"missing databases: {missing}")
    audited = []
    for path in args.databases:
        try:
            audited.append(audit_database(path))
        except (sqlite3.DatabaseError, OSError) as exc:
            audited.append({
                **fast_fingerprint(path),
                "kind": "unreadable_sqlite",
                "audit_error": f"{type(exc).__name__}: {exc}",
            })
    payload = {
        "schema_version": "runtime-data-audit.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "SQLite mode=ro, query_only=ON; no database mutations",
        "interpretation_boundary": "Predictions and prices are observations, not proof of exchange fills or profit.",
        "databases": audited,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
