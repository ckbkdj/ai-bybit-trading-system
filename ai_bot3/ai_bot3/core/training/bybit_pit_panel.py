from __future__ import annotations

import hashlib
import json
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

    def maximum_sequence(self) -> int:
        """Freeze an append-only observation boundary for one experiment."""

        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("Bybit PIT database predates symbol-partitioned features")
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM bybit_feature_observations"
            ).fetchone()
        return int(row[0])

    @staticmethod
    def _snapshot_digest(frame: pd.DataFrame) -> str:
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
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        requested = tuple(dict.fromkeys(str(name) for name in names))
        if not requested:
            raise ValueError("at least one Bybit PIT feature is required")
        for name in requested:
            self.registry.require(name)
            if name not in BYBIT_FEATURE_SOURCE_CONTRACTS:
                raise ValueError(f"Bybit PIT feature has no source contract: {name}")
        placeholders = ",".join("?" for _ in requested)
        frozen_sequence = (
            self.maximum_sequence()
            if maximum_sequence is None
            else int(maximum_sequence)
        )
        if frozen_sequence < 0:
            raise ValueError("Bybit PIT maximum_sequence cannot be negative")
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
                "AND observation_id NOT IN (SELECT observation_id FROM bybit_feature_invalidations)"
                if invalidation_table
                else ""
            )
            invalidated_observation_count = (
                int(
                    connection.execute(
                        f"""SELECT COUNT(*)
                               FROM bybit_feature_invalidations i
                               JOIN bybit_feature_observations o
                                 ON o.observation_id=i.observation_id
                              WHERE o.name IN ({placeholders}) AND o.sequence<=?""",
                        requested + (frozen_sequence,),
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
                       FROM bybit_feature_observations
                       WHERE name IN ({placeholders}) AND sequence<=?
                             {invalidation_clause}
                      ORDER BY symbol,name,available_at,sequence""",
                requested + (frozen_sequence,),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return frame, {
                "source": "bybit.public.pit",
                "database": str(self.path),
                "requested_features": list(requested),
                "observation_count": 0,
                "symbol_count": 0,
                "feature_coverage": {},
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_sha256": hashlib.sha256(b"").hexdigest(),
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
                "observation_count": 0,
                "symbol_count": 0,
                "feature_coverage": {},
                "rejected_source_contract_count": rejected_source_contract_count,
                "snapshot_maximum_sequence": frozen_sequence,
                "snapshot_sha256": hashlib.sha256(b"").hexdigest(),
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
        coverage: dict[str, object] = {}
        for (symbol, name), group in frame.groupby(["symbol", "name"], sort=True):
            start = group["available_at"].min()
            end = group["available_at"].max()
            coverage[f"{symbol}:{name}"] = {
                "observations": len(group),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "coverage_days": float((end - start).total_seconds() / 86_400.0),
                "maximum_age_sec": self.registry.require(str(name)).maximum_age_sec,
            }
        return frame, {
            "source": "bybit.public.pit",
            "database": str(self.path),
            "requested_features": list(requested),
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
            "snapshot_maximum_sequence": frozen_sequence,
            "snapshot_sha256": self._snapshot_digest(frame),
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
            invalidation_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_invalidations'"""
            ).fetchone()
            invalidation_clause = (
                "AND observation_id NOT IN (SELECT observation_id FROM bybit_feature_invalidations)"
                if invalidation_table
                else ""
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
        }


__all__: Sequence[str] = (
    "BYBIT_FEATURE_ALLOWED_SOURCES",
    "BYBIT_FEATURE_SOURCE_CONTRACTS",
    "BybitPITFeatureSource",
)
