from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from core.features.registry import FactorRegistry, default_registry


BYBIT_FEATURE_SOURCE_CONTRACTS: Mapping[str, str] = {
    "orderbook_spread_bps": "bybit.public.orderbook",
    "bybit_orderbook_delta_l5": "bybit.public.orderbook",
    "orderbook_imbalance_l5": "bybit.public.orderbook",
    "ofi_1m": "bybit.public.orderbook",
    "orderbook_depth_usdt_l5": "bybit.public.orderbook",
    "microprice_deviation_bps": "bybit.public.orderbook",
    "fill_probability": "bybit.public.orderbook",
    "expected_slippage_bps": "bybit.public.orderbook",
    "public_trade_imbalance_1m": "bybit.public.trades",
    "aggressive_cvd_1m": "bybit.public.trades",
    "perpetual_basis_bps": "bybit.public.ticker",
    "funding_rate": "bybit.public.ticker",
    "open_interest_change_1h": "bybit.public.ticker",
    "liquidation_imbalance_5m": "bybit.public.liquidations.v2",
}

BYBIT_FEATURE_ALLOWED_SOURCES: Mapping[str, tuple[str, ...]] = {
    **{name: (source,) for name, source in BYBIT_FEATURE_SOURCE_CONTRACTS.items()},
    "perpetual_basis_bps": (
        "bybit.public.ticker",
        "bybit.public.mark_index_kline",
    ),
    "funding_rate": (
        "bybit.public.ticker",
        "bybit.public.funding_history",
    ),
    "open_interest_change_1h": (
        "bybit.public.ticker",
        "bybit.public.open_interest_history",
    ),
}


def _completed_day_continuity(values: Sequence[object]) -> dict[str, object]:
    days = sorted({pd.Timestamp(value).date() for value in values})
    if not days:
        return {
            "completed_source_day_count": 0,
            "longest_consecutive_completed_days": 0,
            "completed_source_day_ratio": 0.0,
            "missing_source_day_count": 0,
        }
    longest = 1
    current = 1
    for prior, following in zip(days, days[1:]):
        if (following - prior).days == 1:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
    calendar_days = (days[-1] - days[0]).days + 1
    return {
        "completed_source_day_count": len(days),
        "longest_consecutive_completed_days": longest,
        "completed_source_day_ratio": float(len(days) / calendar_days),
        "missing_source_day_count": int(calendar_days - len(days)),
        "first_completed_source_day": days[0].isoformat(),
        "last_completed_source_day": days[-1].isoformat(),
    }


def _liquidation_observation_id(raw_event_id: str, symbol: str) -> str:
    event_id = f"{raw_event_id}:bybit-liquidation-side-v2"
    token = hashlib.sha256(
        f"{event_id}|{symbol}|liquidation_imbalance_5m".encode()
    ).hexdigest()[:48]
    return f"bp_{token}"


def _is_sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _expected_capture_audit_id(record: Mapping[str, object]) -> str:
    payload = {
        "maximum_gap_sec": float(record["maximum_gap_sec"]),
        "maximum_raw": int(record["snapshot_maximum_raw_sequence"]),
        "maximum_feature": int(record["snapshot_maximum_feature_sequence"]),
        "maximum_invalidation": int(
            record["snapshot_maximum_invalidation_rowid"]
        ),
        "manifest_sha256": str(record["manifest_sha256"]),
    }
    token = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()[:48]
    return f"bca_{token}"


def _expected_pit_import_id(record: Mapping[str, object]) -> str:
    payload = {
        "source_audit_id": str(record["source_audit_id"]),
        "manifest_sha256": str(record["manifest_sha256"]),
        "selection": dict(record["selection"]),
    }
    token = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()[:48]
    return f"bpi_{token}"


class BybitPITFeatureSource:
    """Read symbol-partitioned Bybit observations with strict as-of joins."""

    def __init__(
        self,
        path: Path,
        *,
        registry: FactorRegistry | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.registry = registry or default_registry()
        if not self.path.is_file():
            raise FileNotFoundError(self.path)

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def snapshot_watermarks(self) -> tuple[int, int]:
        """Freeze observation and invalidation journals in one SQLite snapshot."""

        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("Bybit PIT database predates symbol-partitioned features")
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM bybit_feature_observations"
            ).fetchone()
            invalidation_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_invalidations'"""
            ).fetchone()
            invalidation_row = (
                connection.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM bybit_feature_invalidations"
                ).fetchone()
                if invalidation_table
                else (0,)
            )
        return int(row[0]), int(invalidation_row[0])

    def maximum_sequence(self) -> int:
        return self.snapshot_watermarks()[0]

    def maximum_invalidation_rowid(self) -> int:
        return self.snapshot_watermarks()[1]

    def evidence_watermarks(self) -> tuple[int, int]:
        """Freeze append-only capture-audit and cross-store import receipts."""

        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            values: list[int] = []
            for table in ("bybit_live_capture_audits", "bybit_pit_imports"):
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                values.append(
                    int(
                        connection.execute(
                            f"SELECT COALESCE(MAX(rowid),0) FROM {table}"
                        ).fetchone()[0]
                    )
                    if exists
                    else 0
                )
        return values[0], values[1]

    @staticmethod
    def _snapshot_digest(
        frame: pd.DataFrame,
        *,
        maximum_sequence: int,
        maximum_invalidation_rowid: int,
        maximum_capture_audit_rowid: int,
        maximum_pit_import_rowid: int,
    ) -> str:
        columns = [
            "sequence",
            "observation_id",
            "symbol",
            "name",
            "value",
            "unit",
            "event_time",
            "available_at",
            "ingested_at",
            "source",
            "quality",
            "provenance_kind",
            "archive_id",
            "api_batch_id",
        ]
        payload = frame[columns].copy().sort_values("sequence")
        for column in ("event_time", "available_at", "ingested_at"):
            payload[column] = pd.to_datetime(payload[column], utc=True).astype(str)
        digest = hashlib.sha256()
        digest.update(
            json.dumps(columns, separators=(",", ":")).encode("utf-8")
        )
        digest.update(
            json.dumps(
                {
                    "maximum_capture_audit_rowid": maximum_capture_audit_rowid,
                    "maximum_invalidation_rowid": maximum_invalidation_rowid,
                    "maximum_pit_import_rowid": maximum_pit_import_rowid,
                    "maximum_sequence": maximum_sequence,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(
            pd.util.hash_pandas_object(payload, index=False, categorize=True)
            .to_numpy(dtype="uint64")
            .tobytes()
        )
        return digest.hexdigest()

    def load(
        self,
        names: Sequence[str],
        *,
        maximum_sequence: int | None = None,
        maximum_invalidation_rowid: int | None = None,
        maximum_capture_audit_rowid: int | None = None,
        maximum_pit_import_rowid: int | None = None,
        minimum_decision_at: object | None = None,
        maximum_decision_at: object | None = None,
        symbols: Sequence[str] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        requested = tuple(dict.fromkeys(str(name) for name in names))
        if not requested:
            raise ValueError("at least one Bybit PIT feature is required")
        for name in requested:
            self.registry.require(name)
            if name not in BYBIT_FEATURE_SOURCE_CONTRACTS:
                raise ValueError(f"Bybit PIT feature has no source contract: {name}")
        placeholders = ",".join("?" for _ in requested)
        requested_symbols = tuple(
            dict.fromkeys(str(symbol).strip().upper() for symbol in (symbols or ()))
        )
        if symbols is not None and not requested_symbols:
            raise ValueError("Bybit PIT symbol filter cannot be empty")
        if (maximum_sequence is None) != (maximum_invalidation_rowid is None):
            raise ValueError(
                "Bybit PIT observation and invalidation watermarks must be frozen together"
            )
        if (maximum_capture_audit_rowid is None) != (
            maximum_pit_import_rowid is None
        ):
            raise ValueError(
                "Bybit audit and import receipt watermarks must be frozen together"
            )
        symbol_clause = ""
        symbol_parameters: tuple[object, ...] = ()
        if requested_symbols:
            symbol_placeholders = ",".join("?" for _ in requested_symbols)
            symbol_clause = f" AND o.symbol IN ({symbol_placeholders})"
            symbol_parameters = requested_symbols
        current_sequence, current_invalidation_rowid = self.snapshot_watermarks()
        current_capture_audit_rowid, current_pit_import_rowid = (
            self.evidence_watermarks()
        )
        frozen_sequence = (
            current_sequence if maximum_sequence is None else int(maximum_sequence)
        )
        frozen_invalidation_rowid = (
            current_invalidation_rowid
            if maximum_invalidation_rowid is None
            else int(maximum_invalidation_rowid)
        )
        frozen_capture_audit_rowid = (
            current_capture_audit_rowid
            if maximum_capture_audit_rowid is None
            else int(maximum_capture_audit_rowid)
        )
        frozen_pit_import_rowid = (
            current_pit_import_rowid
            if maximum_pit_import_rowid is None
            else int(maximum_pit_import_rowid)
        )
        if frozen_sequence < 0:
            raise ValueError("Bybit PIT maximum_sequence cannot be negative")
        if frozen_invalidation_rowid < 0:
            raise ValueError("Bybit PIT maximum_invalidation_rowid cannot be negative")
        if frozen_capture_audit_rowid < 0 or frozen_pit_import_rowid < 0:
            raise ValueError("Bybit evidence receipt watermarks cannot be negative")
        decision_minimum = pd.to_datetime(
            minimum_decision_at, utc=True, errors="coerce"
        )
        decision_maximum = pd.to_datetime(
            maximum_decision_at, utc=True, errors="coerce"
        )
        if minimum_decision_at is not None and pd.isna(decision_minimum):
            raise ValueError("Bybit PIT minimum decision timestamp is invalid")
        if maximum_decision_at is not None and pd.isna(decision_maximum):
            raise ValueError("Bybit PIT maximum decision timestamp is invalid")
        if (
            minimum_decision_at is not None
            and maximum_decision_at is not None
            and decision_minimum > decision_maximum
        ):
            raise ValueError("Bybit PIT decision timestamp bounds are reversed")
        available_minimum = None
        if minimum_decision_at is not None:
            maximum_age_sec = max(
                self.registry.require(name).maximum_age_sec for name in requested
            )
            available_minimum = decision_minimum - pd.Timedelta(seconds=maximum_age_sec)

        def sql_timestamp(value: object | None) -> str | None:
            if value is None or pd.isna(value):
                return None
            return pd.Timestamp(value).isoformat().replace("+00:00", "Z")

        available_minimum_text = sql_timestamp(available_minimum)
        available_maximum_text = sql_timestamp(decision_maximum)
        time_clauses: list[str] = []
        time_parameters: list[object] = []
        if available_minimum_text is not None:
            time_clauses.append("o.available_at>=?")
            time_parameters.append(available_minimum_text)
        if available_maximum_text is not None:
            time_clauses.append("o.available_at<=?")
            time_parameters.append(available_maximum_text)
        time_clause = "" if not time_clauses else " AND " + " AND ".join(time_clauses)
        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("Bybit PIT database predates symbol-partitioned features")
            feature_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(bybit_feature_observations)"
                ).fetchall()
            }
            provenance_expression = (
                "provenance_kind"
                if "provenance_kind" in feature_columns
                else "'legacy_live_capture' AS provenance_kind"
            )
            archive_expression = (
                "archive_id" if "archive_id" in feature_columns else "NULL AS archive_id"
            )
            api_batch_expression = (
                "api_batch_id"
                if "api_batch_id" in feature_columns
                else "NULL AS api_batch_id"
            )
            invalidation_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_invalidations'"""
            ).fetchone()
            invalidation_clause = (
                "AND o.observation_id NOT IN (SELECT observation_id "
                "FROM bybit_feature_invalidations WHERE rowid<=?)"
                if invalidation_table
                else ""
            )
            invalidation_parameters: tuple[object, ...] = (
                (frozen_invalidation_rowid,) if invalidation_table else ()
            )
            invalidated_observation_count = (
                int(
                    connection.execute(
                        f"""SELECT COUNT(*)
                               FROM bybit_feature_invalidations i
                              JOIN bybit_feature_observations o
                                 ON o.observation_id=i.observation_id
                              WHERE i.rowid<=? AND o.name IN ({placeholders})
                                    AND o.sequence<=?
                                    {symbol_clause}
                                    {time_clause}""",
                        (frozen_invalidation_rowid,)
                        + requested
                        + (frozen_sequence,)
                        + symbol_parameters
                        + tuple(time_parameters),
                    ).fetchone()[0]
                )
                if invalidation_table
                else 0
            )
            rows = connection.execute(
                f"""SELECT sequence,observation_id,symbol,name,value,unit,event_time,
                            available_at,ingested_at,source,quality,
                            {provenance_expression},{archive_expression},
                            {api_batch_expression}
                       FROM bybit_feature_observations o
                       WHERE o.name IN ({placeholders}) AND o.sequence<=?
                             {symbol_clause}
                             {time_clause}
                             {invalidation_clause}
                      ORDER BY symbol,name,available_at,sequence""",
                requested
                + (frozen_sequence,)
                + symbol_parameters
                + tuple(time_parameters)
                + invalidation_parameters,
            ).fetchall()
        observation_columns = [
            "sequence",
            "observation_id",
            "symbol",
            "name",
            "value",
            "unit",
            "event_time",
            "available_at",
            "ingested_at",
            "source",
            "quality",
            "provenance_kind",
            "archive_id",
            "api_batch_id",
        ]
        frame = pd.DataFrame([dict(row) for row in rows], columns=observation_columns)
        if frame.empty:
            return frame, {
                "source": "bybit.public.pit",
                "database": str(self.path),
                "requested_features": list(requested),
                "requested_symbols": list(requested_symbols),
                "observation_count": 0,
                "symbol_count": 0,
                "feature_coverage": {},
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_maximum_invalidation_rowid": frozen_invalidation_rowid,
                "snapshot_maximum_capture_audit_rowid": frozen_capture_audit_rowid,
                "snapshot_maximum_pit_import_rowid": frozen_pit_import_rowid,
                "minimum_decision_at": sql_timestamp(decision_minimum),
                "maximum_decision_at": sql_timestamp(decision_maximum),
                "effective_available_at_minimum": available_minimum_text,
                "snapshot_sha256": self._snapshot_digest(
                    frame,
                    maximum_sequence=frozen_sequence,
                    maximum_invalidation_rowid=frozen_invalidation_rowid,
                    maximum_capture_audit_rowid=frozen_capture_audit_rowid,
                    maximum_pit_import_rowid=frozen_pit_import_rowid,
                ),
                "invalidated_observation_count": invalidated_observation_count,
            }
        accepted_source = frame.apply(
            lambda row: str(row["source"])
            in BYBIT_FEATURE_ALLOWED_SOURCES[str(row["name"])],
            axis=1,
        )
        rejected_source_contract_count = int((~accepted_source).sum())
        frame = frame.loc[accepted_source].copy()
        if frame.empty:
            return frame, {
                "source": "bybit.public.pit",
                "database": str(self.path),
                "requested_features": list(requested),
                "requested_symbols": list(requested_symbols),
                "observation_count": 0,
                "symbol_count": 0,
                "feature_coverage": {},
                "rejected_source_contract_count": rejected_source_contract_count,
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_maximum_invalidation_rowid": frozen_invalidation_rowid,
                "snapshot_maximum_capture_audit_rowid": frozen_capture_audit_rowid,
                "snapshot_maximum_pit_import_rowid": frozen_pit_import_rowid,
                "minimum_decision_at": sql_timestamp(decision_minimum),
                "maximum_decision_at": sql_timestamp(decision_maximum),
                "effective_available_at_minimum": available_minimum_text,
                "snapshot_sha256": self._snapshot_digest(
                    frame,
                    maximum_sequence=frozen_sequence,
                    maximum_invalidation_rowid=frozen_invalidation_rowid,
                    maximum_capture_audit_rowid=frozen_capture_audit_rowid,
                    maximum_pit_import_rowid=frozen_pit_import_rowid,
                ),
                "invalidated_observation_count": invalidated_observation_count,
            }
        for column in ("event_time", "available_at", "ingested_at"):
            frame[column] = pd.to_datetime(
                frame[column], utc=True, errors="coerce", format="mixed"
            )
        if frame[["event_time", "available_at", "ingested_at"]].isna().any().any():
            raise RuntimeError("Bybit PIT feature timestamps are invalid")
        violation = ~(
            (frame["event_time"] <= frame["available_at"])
            & (frame["available_at"] <= frame["ingested_at"])
        )
        if violation.any():
            raise RuntimeError("Bybit PIT feature chronology invariant failed")
        liquidation_frame = frame[
            (frame["name"] == "liquidation_imbalance_5m")
            & (frame["source"] == "bybit.public.liquidations.v2")
        ].copy()
        if not liquidation_frame.empty:
            symbols_in_frame = tuple(
                sorted(set(liquidation_frame["symbol"].astype(str)))
            )
            symbol_placeholders = ",".join("?" for _ in symbols_in_frame)
            received_start = (
                liquidation_frame["available_at"]
                .min()
                .isoformat()
                .replace("+00:00", "Z")
            )
            received_end = (
                liquidation_frame["available_at"]
                .max()
                .isoformat()
                .replace("+00:00", "Z")
            )
            with closing(self._connect()) as connection:
                raw_table = connection.execute(
                    """SELECT 1 FROM sqlite_master
                         WHERE type='table' AND name='bybit_raw_public_events'"""
                ).fetchone()
                if not raw_table:
                    raise RuntimeError("liquidation feature raw journal is missing")
                raw_rows = connection.execute(
                    f"""SELECT event_id,symbol,received_at
                           FROM bybit_raw_public_events
                          WHERE event_type='liquidation'
                            AND symbol IN ({symbol_placeholders})
                            AND received_at>=? AND received_at<=?""",
                    symbols_in_frame + (received_start, received_end),
                ).fetchall()
            raw_links = {
                _liquidation_observation_id(
                    str(row["event_id"]), str(row["symbol"]).upper()
                ): (str(row["symbol"]).upper(), str(row["received_at"]))
                for row in raw_rows
            }
            for row in liquidation_frame.itertuples(index=False):
                link = raw_links.get(str(row.observation_id))
                if link is None:
                    raise RuntimeError(
                        "liquidation feature has no deterministic raw-event link"
                    )
                available_at = row.available_at.isoformat().replace("+00:00", "Z")
                if link != (str(row.symbol).upper(), available_at):
                    raise RuntimeError("liquidation feature/raw chronology mismatch")
        allowed_provenance = {
            "live_capture",
            "legacy_live_capture",
            "historical_archive_replay",
            "historical_api_replay",
        }
        if not set(frame["provenance_kind"].astype(str)).issubset(allowed_provenance):
            raise RuntimeError("Bybit PIT feature provenance kind is invalid")
        archive_frame = frame[
            frame["provenance_kind"] == "historical_archive_replay"
        ].copy()
        archive_provenance: list[dict[str, object]] = []
        by_id: dict[str, dict[str, object]] = {}
        if not archive_frame.empty:
            if archive_frame["archive_id"].isna().any():
                raise RuntimeError("historical archive feature has no archive_id")
            archive_ids = tuple(sorted(set(archive_frame["archive_id"].astype(str))))
            placeholders = ",".join("?" for _ in archive_ids)
            with closing(self._connect()) as connection:
                archive_rows = connection.execute(
                    f"""SELECT archive_id,data_kind,market,symbol,trading_date,
                                source_url,fetched_at,content_length,content_sha256,
                                rows_read,feature_observation_count,status
                           FROM bybit_historical_archive_files
                          WHERE archive_id IN ({placeholders})""",
                    archive_ids,
                ).fetchall()
            by_id = {str(row["archive_id"]): dict(row) for row in archive_rows}
            missing_archive_ids = sorted(set(archive_ids).difference(by_id))
            if missing_archive_ids:
                raise RuntimeError("historical archive feature provenance record is missing")
            incomplete = [
                archive_id
                for archive_id, row in by_id.items()
                if str(row["status"]) != "completed"
            ]
            if incomplete:
                raise RuntimeError("historical archive feature references an incomplete file")
            for archive_id, group in archive_frame.groupby("archive_id", sort=True):
                record = by_id[str(archive_id)]
                if set(group["symbol"].astype(str)) != {str(record["symbol"])}:
                    raise RuntimeError("historical archive feature symbol provenance mismatch")
                event_dates = set(group["event_time"].dt.date.astype(str))
                if event_dates != {str(record["trading_date"])}:
                    raise RuntimeError("historical archive feature day provenance mismatch")
                archive_provenance.append(record)
        api_frame = frame[frame["provenance_kind"] == "historical_api_replay"].copy()
        api_provenance: list[dict[str, object]] = []
        api_response_provenance: list[dict[str, object]] = []
        api_by_id: dict[str, dict[str, object]] = {}
        if not api_frame.empty:
            if api_frame["api_batch_id"].isna().any():
                raise RuntimeError("historical API feature has no api_batch_id")
            api_batch_ids = tuple(sorted(set(api_frame["api_batch_id"].astype(str))))
            api_placeholders = ",".join("?" for _ in api_batch_ids)
            with closing(self._connect()) as connection:
                api_rows = connection.execute(
                    f"""SELECT batch_id,data_kind,market,symbol,trading_date,
                                endpoint_group,requested_at,completed_at,
                                first_event_time,last_event_time,response_count,
                                rows_read,feature_observation_count,
                                request_manifest_sha256,status
                           FROM bybit_historical_api_batches
                          WHERE batch_id IN ({api_placeholders})""",
                    api_batch_ids,
                ).fetchall()
                response_rows = connection.execute(
                    f"""SELECT response_id,batch_id,request_url,requested_at,
                                received_at,http_status,content_length,
                                content_sha256,rows_read,ret_code
                           FROM bybit_historical_api_responses
                          WHERE batch_id IN ({api_placeholders})
                          ORDER BY batch_id,response_id""",
                    api_batch_ids,
                ).fetchall()
            api_by_id = {str(row["batch_id"]): dict(row) for row in api_rows}
            if set(api_batch_ids).difference(api_by_id):
                raise RuntimeError("historical API feature provenance record is missing")
            responses_by_batch: dict[str, list[dict[str, object]]] = {}
            for row in response_rows:
                record = dict(row)
                responses_by_batch.setdefault(str(record["batch_id"]), []).append(record)
            for batch_id, group in api_frame.groupby("api_batch_id", sort=True):
                record = api_by_id[str(batch_id)]
                if str(record["status"]) != "completed":
                    raise RuntimeError("historical API feature references an incomplete batch")
                if set(group["symbol"].astype(str)) != {str(record["symbol"])}:
                    raise RuntimeError("historical API feature symbol provenance mismatch")
                if set(group["event_time"].dt.date.astype(str)) != {
                    str(record["trading_date"])
                }:
                    raise RuntimeError("historical API feature day provenance mismatch")
                responses = responses_by_batch.get(str(batch_id), [])
                if len(responses) != int(record["response_count"]):
                    raise RuntimeError("historical API response provenance count mismatch")
                if any(
                    int(response["http_status"]) != 200
                    or int(response["ret_code"]) != 0
                    for response in responses
                ):
                    raise RuntimeError("historical API batch contains a failed response")
                api_provenance.append(record)
                api_response_provenance.extend(responses)
        expected_units = {
            name: self.registry.require(name).unit for name in requested
        }
        unit_violation = frame.apply(
            lambda row: str(row["unit"]) != expected_units[str(row["name"])], axis=1
        )
        if unit_violation.any():
            raise RuntimeError("Bybit PIT feature unit contract failed")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        if frame["value"].isna().any():
            raise RuntimeError("Bybit PIT feature contains non-numeric values")
        frame["quality"] = pd.to_numeric(frame["quality"], errors="coerce")
        if frame["quality"].isna().any():
            raise RuntimeError("Bybit PIT feature contains invalid quality values")
        accepted_quality = frame.apply(
            lambda row: float(row["quality"])
            >= self.registry.require(str(row["name"])).minimum_quality,
            axis=1,
        )
        rejected_low_quality_count = int((~accepted_quality).sum())
        frame = frame.loc[accepted_quality].copy()
        live_capture_audits: list[dict[str, object]] = []
        pit_imports: list[dict[str, object]] = []
        with closing(self._connect()) as connection:
            audit_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_live_capture_audits'"""
            ).fetchone()
            if audit_table:
                audit_rows = connection.execute(
                    """SELECT audit_id,created_at,snapshot_maximum_raw_sequence,
                              snapshot_maximum_feature_sequence,
                              snapshot_maximum_invalidation_rowid,first_received_at,
                              last_received_at,maximum_gap_sec,raw_event_count,
                              liquidation_feature_count,symbols_json,topic_counts_json,
                              event_type_counts_json,interval_count,longest_interval_sec,
                              manifest_sha256,status,error
                         FROM bybit_live_capture_audits
                        WHERE status='completed' AND rowid<=?
                        ORDER BY created_at,audit_id""",
                    (frozen_capture_audit_rowid,),
                ).fetchall()
                for audit_row in audit_rows:
                    record = dict(audit_row)
                    interval_rows = connection.execute(
                        """SELECT interval_index,started_at,ended_at,raw_event_count
                             FROM bybit_live_capture_intervals
                            WHERE audit_id=? ORDER BY interval_index""",
                        (record["audit_id"],),
                    ).fetchall()
                    if len(interval_rows) != int(record["interval_count"]):
                        raise RuntimeError("live capture audit interval count mismatch")
                    intervals = [dict(row) for row in interval_rows]
                    if not intervals or [
                        int(item["interval_index"]) for item in intervals
                    ] != list(range(len(intervals))):
                        raise RuntimeError("live capture audit interval indices are invalid")
                    interval_bounds = [
                        (
                            pd.Timestamp(item["started_at"]),
                            pd.Timestamp(item["ended_at"]),
                        )
                        for item in intervals
                    ]
                    if any(start > end for start, end in interval_bounds):
                        raise RuntimeError("live capture audit interval chronology failed")
                    record["symbols"] = json.loads(str(record.pop("symbols_json")))
                    if (
                        not isinstance(record["symbols"], list)
                        or not record["symbols"]
                        or any(
                            not str(symbol).strip()
                            or str(symbol).upper() != str(symbol)
                            for symbol in record["symbols"]
                        )
                        or len(set(record["symbols"])) != len(record["symbols"])
                    ):
                        raise RuntimeError("live capture audit symbols are invalid")
                    record["topic_counts"] = json.loads(
                        str(record.pop("topic_counts_json"))
                    )
                    record["event_type_counts"] = json.loads(
                        str(record.pop("event_type_counts_json"))
                    )
                    if not isinstance(record["topic_counts"], dict) or not isinstance(
                        record["event_type_counts"], dict
                    ):
                        raise RuntimeError("live capture audit count maps are invalid")
                    raw_event_count = int(record["raw_event_count"])
                    interval_event_count = sum(
                        int(item["raw_event_count"]) for item in intervals
                    )
                    topic_count = sum(
                        int(value)
                        for value in dict(record["topic_counts"]).values()
                    )
                    event_type_count = sum(
                        int(value)
                        for value in dict(record["event_type_counts"]).values()
                    )
                    if (
                        raw_event_count <= 0
                        or interval_event_count != raw_event_count
                        or topic_count != raw_event_count
                        or event_type_count != raw_event_count
                    ):
                        raise RuntimeError("live capture audit event counts do not reconcile")
                    if any(
                        int(value) < 0
                        for counts in (
                            record["topic_counts"],
                            record["event_type_counts"],
                        )
                        for value in dict(counts).values()
                    ):
                        raise RuntimeError("live capture audit contains a negative count")
                    maximum_gap_sec = float(record["maximum_gap_sec"])
                    if not 0 < maximum_gap_sec <= 300:
                        raise RuntimeError("live capture audit gap contract is invalid")
                    if any(
                        (following[0] - prior[1]).total_seconds()
                        <= maximum_gap_sec
                        for prior, following in zip(
                            interval_bounds, interval_bounds[1:]
                        )
                    ):
                        raise RuntimeError("live capture audit intervals were split incorrectly")
                    longest_interval_sec = max(
                        (end - start).total_seconds()
                        for start, end in interval_bounds
                    )
                    if not math.isclose(
                        longest_interval_sec,
                        float(record["longest_interval_sec"]),
                        rel_tol=0.0,
                        abs_tol=1e-6,
                    ):
                        raise RuntimeError("live capture audit longest interval mismatch")
                    if (
                        str(record["first_received_at"])
                        != str(intervals[0]["started_at"])
                        or str(record["last_received_at"])
                        != str(intervals[-1]["ended_at"])
                    ):
                        raise RuntimeError("live capture audit boundary mismatch")
                    if (
                        not _is_sha256(record["manifest_sha256"])
                        or str(record["audit_id"])
                        != _expected_capture_audit_id(record)
                    ):
                        raise RuntimeError("live capture audit identity is invalid")
                    liquidation_feature_count = int(
                        record["liquidation_feature_count"]
                    )
                    liquidation_raw_count = sum(
                        int(count)
                        for topic, count in dict(record["topic_counts"]).items()
                        if str(topic).startswith("allLiquidation.")
                    )
                    if not 0 <= liquidation_feature_count <= liquidation_raw_count:
                        raise RuntimeError(
                            "live capture audit liquidation counts are invalid"
                        )
                    record["intervals"] = intervals
                    live_capture_audits.append(record)
            import_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_pit_imports'"""
            ).fetchone()
            if import_table:
                for import_row in connection.execute(
                    """SELECT import_id,imported_at,source_database,source_audit_id,
                              selection_json,source_counts_json,inserted_counts_json,
                              manifest_sha256,status
                         FROM bybit_pit_imports
                        WHERE status='completed' AND rowid<=?
                         ORDER BY imported_at,import_id""",
                    (frozen_pit_import_rowid,),
                ).fetchall():
                    record = dict(import_row)
                    for key in ("selection", "source_counts", "inserted_counts"):
                        record[key] = json.loads(str(record.pop(f"{key}_json")))
                    if (
                        not _is_sha256(record["manifest_sha256"])
                        or str(record["import_id"]) != _expected_pit_import_id(record)
                    ):
                        raise RuntimeError("Bybit PIT import identity is invalid")
                    if str(record["source_audit_id"]) not in {
                        str(item["audit_id"]) for item in live_capture_audits
                    }:
                        raise RuntimeError("Bybit PIT import references an unavailable audit")
                    selection = dict(record["selection"])
                    if (
                        selection.get("event_type") != "liquidation"
                        or selection.get("feature") != "liquidation_imbalance_5m"
                    ):
                        raise RuntimeError("Bybit PIT import selection contract is invalid")
                    source_counts = {
                        str(key): int(value)
                        for key, value in dict(record["source_counts"]).items()
                    }
                    inserted_counts = {
                        str(key): int(value)
                        for key, value in dict(record["inserted_counts"]).items()
                    }
                    if any(value < 0 for value in source_counts.values()) or any(
                        value < 0 or value > source_counts.get(key, -1)
                        for key, value in inserted_counts.items()
                    ):
                        raise RuntimeError("Bybit PIT import counts are invalid")
                    pit_imports.append(record)
        coverage: dict[str, object] = {}
        for (symbol, name), group in frame.groupby(["symbol", "name"], sort=True):
            start = group["available_at"].min()
            end = group["available_at"].max()
            completed_days: list[object] = []
            for archive_id in group["archive_id"].dropna().astype(str).unique():
                if archive_id in by_id:
                    completed_days.append(by_id[archive_id]["trading_date"])
            for api_batch_id in group["api_batch_id"].dropna().astype(str).unique():
                if api_batch_id in api_by_id:
                    completed_days.append(api_by_id[api_batch_id]["trading_date"])
            ordered_available = group["available_at"].sort_values()
            maximum_observation_gap_sec = (
                float(ordered_available.diff().dt.total_seconds().max())
                if len(ordered_available) > 1
                else None
            )
            coverage[f"{symbol}:{name}"] = {
                "observations": len(group),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "coverage_days": float((end - start).total_seconds() / 86_400.0),
                "maximum_observation_gap_sec": maximum_observation_gap_sec,
                **_completed_day_continuity(completed_days),
                "maximum_age_sec": self.registry.require(str(name)).maximum_age_sec,
            }
        return frame, {
            "source": "bybit.public.pit",
            "database": str(self.path),
            "requested_features": list(requested),
            "requested_symbols": list(requested_symbols),
            "observation_count": len(frame),
            "symbol_count": int(frame["symbol"].nunique()),
            "feature_coverage": coverage,
            "rejected_low_quality_count": rejected_low_quality_count,
            "rejected_source_contract_count": rejected_source_contract_count,
            "invalidated_observation_count": invalidated_observation_count,
            "feature_source_contracts": {
                name: list(BYBIT_FEATURE_ALLOWED_SOURCES[name]) for name in requested
            },
            "provenance_observation_counts": {
                str(kind): int(count)
                for kind, count in frame["provenance_kind"].value_counts().items()
            },
            "historical_archive_files": archive_provenance,
            "historical_archive_file_count": len(archive_provenance),
            "historical_archive_claim": (
                "exchange-event-time replay with conservative simulated availability; not live capture"
                if archive_provenance
                else None
            ),
            "historical_api_batches": api_provenance,
            "historical_api_batch_count": len(api_provenance),
            "historical_api_responses": api_response_provenance,
            "historical_api_response_count": len(api_response_provenance),
            "historical_api_claim": (
                "exchange-event-time replay from hashed official REST responses with conservative simulated availability; not live capture"
                if api_provenance
                else None
            ),
            "live_capture_audits": live_capture_audits,
            "live_capture_audit_count": len(live_capture_audits),
            "pit_imports": pit_imports,
            "pit_import_count": len(pit_imports),
            "snapshot_maximum_sequence": frozen_sequence,
            "snapshot_maximum_invalidation_rowid": frozen_invalidation_rowid,
            "snapshot_maximum_capture_audit_rowid": frozen_capture_audit_rowid,
            "snapshot_maximum_pit_import_rowid": frozen_pit_import_rowid,
            "minimum_decision_at": sql_timestamp(decision_minimum),
            "maximum_decision_at": sql_timestamp(decision_maximum),
            "effective_available_at_minimum": available_minimum_text,
            "snapshot_sha256": self._snapshot_digest(
                frame,
                maximum_sequence=frozen_sequence,
                maximum_invalidation_rowid=frozen_invalidation_rowid,
                maximum_capture_audit_rowid=frozen_capture_audit_rowid,
                maximum_pit_import_rowid=frozen_pit_import_rowid,
            ),
            "pit_policy": "symbol-specific latest available_at at or before decision_at with registry staleness cutoff",
        }

    def join(
        self,
        decisions: pd.DataFrame,
        *,
        names: Sequence[str],
        history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        required = {"symbol", "decision_at"}
        if missing := sorted(required.difference(decisions.columns)):
            raise ValueError(f"Bybit PIT join missing columns: {missing}")
        observations = history if history is not None else self.load(names)[0]
        output = decisions.copy().reset_index(drop=False).rename(columns={"index": "_row_id"})
        output["symbol"] = output["symbol"].astype(str).str.upper()
        output["decision_at"] = pd.to_datetime(
            output["decision_at"], utc=True, errors="coerce"
        )
        if output["decision_at"].isna().any():
            raise ValueError("Bybit PIT decisions contain invalid timestamps")
        for name in names:
            subset = observations[observations["name"] == name][
                ["symbol", "available_at", "value"]
            ].copy()
            if subset.empty:
                output[name] = float("nan")
                output[f"{name}__available_at"] = pd.NaT
                continue
            subset = subset.rename(
                columns={"value": name, "available_at": f"{name}__available_at"}
            )
            output = pd.merge_asof(
                output.sort_values(["decision_at", "symbol"]),
                subset.sort_values([f"{name}__available_at", "symbol"]),
                left_on="decision_at",
                right_on=f"{name}__available_at",
                by="symbol",
                direction="backward",
                tolerance=pd.Timedelta(
                    seconds=self.registry.require(name).maximum_age_sec
                ),
                allow_exact_matches=True,
            )
            violation = output[f"{name}__available_at"].notna() & (
                output[f"{name}__available_at"] > output["decision_at"]
            )
            if violation.any():
                raise RuntimeError(f"PIT violation while joining {name}")
        return output.sort_values("_row_id").drop(columns=["_row_id"]).reset_index(drop=True)

    def latest(
        self,
        symbol: str,
        names: Sequence[str],
        *,
        decision_at: object,
    ) -> tuple[dict[str, float], dict[str, object]]:
        decision = pd.to_datetime(decision_at, utc=True, errors="coerce")
        if pd.isna(decision):
            raise ValueError("Bybit PIT runtime decision timestamp is invalid")
        normalized_symbol = symbol.strip().upper()
        values: dict[str, float] = {}
        available: dict[str, str] = {}
        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            snapshot_maximum_sequence = int(
                connection.execute(
                    "SELECT COALESCE(MAX(sequence),0) FROM bybit_feature_observations"
                ).fetchone()[0]
            )
            invalidation_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_invalidations'"""
            ).fetchone()
            invalidation_clause = (
                "AND observation_id NOT IN (SELECT observation_id FROM bybit_feature_invalidations)"
                if invalidation_table
                else ""
            )
            snapshot_maximum_invalidation_rowid = (
                int(
                    connection.execute(
                        "SELECT COALESCE(MAX(rowid),0) FROM bybit_feature_invalidations"
                    ).fetchone()[0]
                )
                if invalidation_table
                else 0
            )
            for name in names:
                definition = self.registry.require(name)
                expected_sources = BYBIT_FEATURE_ALLOWED_SOURCES.get(name)
                if expected_sources is None:
                    raise ValueError(f"Bybit PIT feature has no source contract: {name}")
                maximum_age = definition.maximum_age_sec
                source_placeholders = ",".join("?" for _ in expected_sources)
                row = connection.execute(
                    f"""SELECT value,unit,event_time,available_at,ingested_at,quality,source
                         FROM bybit_feature_observations
                         WHERE symbol=? AND name=? AND source IN ({source_placeholders})
                           AND available_at<=? AND quality>=?
                           {invalidation_clause}
                         ORDER BY available_at DESC,sequence DESC LIMIT 1""",
                    (
                        normalized_symbol,
                        name,
                        *expected_sources,
                        decision.isoformat().replace("+00:00", "Z"),
                        definition.minimum_quality,
                    ),
                ).fetchone()
                if not row:
                    continue
                event_time = pd.to_datetime(row["event_time"], utc=True, errors="raise")
                available_at = pd.to_datetime(row["available_at"], utc=True, errors="raise")
                ingested_at = pd.to_datetime(row["ingested_at"], utc=True, errors="raise")
                if not event_time <= available_at <= ingested_at:
                    raise RuntimeError(f"Bybit PIT chronology invariant failed for {name}")
                if str(row["unit"]) != definition.unit:
                    raise RuntimeError(f"Bybit PIT unit contract failed for {name}")
                age = (decision - available_at).total_seconds()
                if age < 0:
                    raise RuntimeError(f"PIT violation while loading latest {name}")
                if age > maximum_age:
                    continue
                values[name] = float(row["value"])
                available[name] = available_at.isoformat().replace("+00:00", "Z")
        return values, {
            "source": "bybit.public.pit",
            "database": str(self.path),
            "symbol": normalized_symbol,
            "decision_at": decision.isoformat().replace("+00:00", "Z"),
            "requested_features": list(names),
            "available_features": sorted(values),
            "feature_available_at": available,
            "snapshot_maximum_sequence": snapshot_maximum_sequence,
            "snapshot_maximum_invalidation_rowid": (
                snapshot_maximum_invalidation_rowid
            ),
        }


__all__: Sequence[str] = (
    "BYBIT_FEATURE_ALLOWED_SOURCES",
    "BYBIT_FEATURE_SOURCE_CONTRACTS",
    "BybitPITFeatureSource",
)
