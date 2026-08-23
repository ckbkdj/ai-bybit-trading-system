from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

from core.features.registry import FactorRegistry, default_registry


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

    def load(
        self,
        names: Sequence[str],
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        requested = tuple(dict.fromkeys(str(name) for name in names))
        if not requested:
            raise ValueError("at least one Bybit PIT feature is required")
        for name in requested:
            self.registry.require(name)
        placeholders = ",".join("?" for _ in requested)
        with closing(self._connect()) as connection:
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                     WHERE type='table' AND name='bybit_feature_observations'"""
            ).fetchone()
            if not table:
                raise RuntimeError("Bybit PIT database predates symbol-partitioned features")
            rows = connection.execute(
                f"""SELECT sequence,observation_id,symbol,name,value,unit,event_time,
                            available_at,ingested_at,source,quality
                       FROM bybit_feature_observations
                      WHERE name IN ({placeholders})
                      ORDER BY symbol,name,available_at,sequence""",
                requested,
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
            }
        for column in ("event_time", "available_at", "ingested_at"):
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
        if frame[["event_time", "available_at", "ingested_at"]].isna().any().any():
            raise RuntimeError("Bybit PIT feature timestamps are invalid")
        violation = ~(
            (frame["event_time"] <= frame["available_at"])
            & (frame["available_at"] <= frame["ingested_at"])
        )
        if violation.any():
            raise RuntimeError("Bybit PIT feature chronology invariant failed")
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
            for name in names:
                definition = self.registry.require(name)
                maximum_age = definition.maximum_age_sec
                row = connection.execute(
                    """SELECT value,unit,event_time,available_at,ingested_at,quality,source
                         FROM bybit_feature_observations
                         WHERE symbol=? AND name=? AND available_at<=? AND quality>=?
                         ORDER BY available_at DESC,sequence DESC LIMIT 1""",
                    (
                        normalized_symbol,
                        name,
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


__all__: Sequence[str] = ("BybitPITFeatureSource",)
