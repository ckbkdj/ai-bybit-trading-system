from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from contracts.horizons import HORIZONS_SEC, HORIZON_TIMEFRAME

REQUIRED_CONTEXT_COLUMNS = (
    "symbol",
    "horizon_sec",
    "decision_at",
    "available_at",
    "label_available_at",
    "liquidity",
    "volatility",
    "session",
    "regime",
    "net_return",
    "mae",
    "mfe",
)


def causal_regime_labels(
    frame: pd.DataFrame,
    *,
    minimum_history: int = 8,
    high_volatility_quantile: float = 0.70,
) -> pd.Series:
    """Classify each row using only volatility observations strictly before it."""

    if minimum_history <= 0 or not 0 < high_volatility_quantile < 1:
        raise ValueError("invalid causal regime configuration")
    required = {"symbol", "decision_at", "volatility"}
    if not required.issubset(frame.columns):
        raise ValueError(f"causal regime requires columns: {sorted(required)}")
    ordered = frame[["symbol", "decision_at", "volatility"]].copy()
    ordered["_original_index"] = np.arange(len(ordered))
    ordered["decision_at"] = _as_utc(ordered["decision_at"], "decision_at")
    ordered["volatility"] = pd.to_numeric(ordered["volatility"], errors="coerce")
    ordered = ordered.sort_values(["symbol", "decision_at", "_original_index"])
    threshold = ordered.groupby("symbol", sort=False)["volatility"].transform(
        lambda values: values.shift(1).expanding(min_periods=minimum_history).quantile(
            high_volatility_quantile
        )
    )
    labels = np.where(
        threshold.isna(),
        "insufficient_history",
        np.where(ordered["volatility"] > threshold, "high_volatility", "normal"),
    )
    result = pd.Series(labels, index=ordered["_original_index"].to_numpy(), dtype="object")
    return result.sort_index().reset_index(drop=True)


@dataclass(frozen=True)
class WalkForwardFold:
    fold_id: str
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    purge_sec: int
    embargo_sec: int


@dataclass(frozen=True)
class HorizonDataset:
    horizon_sec: int
    development: pd.DataFrame
    lockbox: pd.DataFrame
    folds: tuple[WalkForwardFold, ...]
    development_fingerprint: str
    lockbox_fingerprint: str | None
    lockbox_start: str
    lockbox_labels_materialized: bool = True


def _as_utc(series: pd.Series, name: str) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{name} contains invalid timestamps")
    return parsed


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    """Fingerprint labels, features, PIT timestamps and schema without huge JSON."""

    columns = sorted(str(column) for column in frame.columns)
    payload = frame[columns].copy()
    for column in columns:
        if isinstance(payload[column].dtype, pd.DatetimeTZDtype):
            payload[column] = pd.to_datetime(payload[column], utc=True).astype("int64")
    digest = hashlib.sha256()
    schema = [(column, str(payload[column].dtype)) for column in columns]
    digest.update(json.dumps(schema, separators=(",", ":")).encode("utf-8"))
    digest.update(
        pd.util.hash_pandas_object(payload, index=False, categorize=True)
        .to_numpy(dtype="uint64")
        .tobytes()
    )
    return digest.hexdigest()


class PooledPanelBuilder:
    """Build one cross-symbol panel per fixed horizon.

    The final lockbox is selected once by wall-clock time and excluded before
    folds are built.  Callers receive a fingerprint, not a tuning handle, so
    the experiment ledger can prove which immutable lockbox was consumed.
    """

    def __init__(
        self,
        *,
        lockbox_fraction: float = 0.15,
        purge_multiplier: float = 1.0,
        embargo_multiplier: float = 0.25,
        minimum_train_rows: int = 200,
        minimum_test_rows: int = 50,
        maximum_folds: int = 6,
    ) -> None:
        if not 0.05 <= lockbox_fraction <= 0.35:
            raise ValueError("lockbox_fraction must be between 5% and 35%")
        if purge_multiplier < 1 or embargo_multiplier < 0:
            raise ValueError("purge must cover the horizon and embargo cannot be negative")
        if min(minimum_train_rows, minimum_test_rows, maximum_folds) <= 0:
            raise ValueError("minimum rows and maximum folds must be positive")
        self.lockbox_fraction = lockbox_fraction
        self.purge_multiplier = purge_multiplier
        self.embargo_multiplier = embargo_multiplier
        self.minimum_train_rows = minimum_train_rows
        self.minimum_test_rows = minimum_test_rows
        self.maximum_folds = maximum_folds

    @staticmethod
    def enrich_context(frame: pd.DataFrame) -> pd.DataFrame:
        data = frame.copy()
        if "decision_at" not in data:
            raise ValueError("decision_at is required")
        data["decision_at"] = _as_utc(data["decision_at"], "decision_at")
        if "symbol" not in data:
            raise ValueError("symbol is required")
        data["symbol"] = data["symbol"].astype(str).str.upper()
        if "liquidity" not in data:
            close = pd.to_numeric(
                data["close"] if "close" in data else pd.Series(0.0, index=data.index),
                errors="coerce",
            ).fillna(0.0)
            volume = pd.to_numeric(
                data["volume"] if "volume" in data else pd.Series(0.0, index=data.index),
                errors="coerce",
            ).fillna(0.0)
            data["liquidity"] = (close * volume).clip(lower=0.0)
        if "volatility" not in data:
            close = pd.to_numeric(data.get("close"), errors="coerce")
            data["volatility"] = (
                data.assign(_close=close)
                .groupby("symbol", sort=False)["_close"]
                .pct_change()
                .groupby(data["symbol"], sort=False)
                .rolling(24, min_periods=4)
                .std()
                .reset_index(level=0, drop=True)
                .fillna(0.0)
            )
        if "session" not in data:
            hour = data["decision_at"].dt.hour
            data["session"] = np.select(
                [hour < 8, hour < 16], ["asia", "europe"], default="americas"
            )
        if "regime" not in data:
            data["regime"] = causal_regime_labels(data).to_numpy()
        return data

    @staticmethod
    def validate(frame: pd.DataFrame, horizon_sec: int) -> pd.DataFrame:
        if horizon_sec not in HORIZONS_SEC:
            raise ValueError(f"unsupported horizon: {horizon_sec}")
        data = PooledPanelBuilder.enrich_context(frame)
        missing = [column for column in REQUIRED_CONTEXT_COLUMNS if column not in data]
        if missing:
            raise ValueError(f"pooled panel is missing columns: {missing}")
        data["available_at"] = _as_utc(data["available_at"], "available_at")
        data["label_available_at"] = _as_utc(data["label_available_at"], "label_available_at")
        if (data["available_at"] > data["decision_at"]).any():
            raise ValueError("PIT violation: feature available_at exceeds decision_at")
        if (data["label_available_at"] <= data["decision_at"]).any():
            raise ValueError("labels must become available after the decision")
        if not (pd.to_numeric(data["horizon_sec"], errors="coerce") == horizon_sec).all():
            raise ValueError("panel mixes horizons")
        if data["symbol"].nunique() < 2:
            raise ValueError("pooled panel requires at least two symbols")
        numeric = ("liquidity", "volatility", "net_return", "mae", "mfe")
        for column in numeric:
            data[column] = pd.to_numeric(data[column], errors="coerce")
        if data[list(numeric)].isna().any().any():
            raise ValueError("panel contains non-numeric label/context values")
        return data.sort_values(["decision_at", "symbol"]).reset_index(drop=True)

    def build_horizon(self, frame: pd.DataFrame, horizon_sec: int) -> HorizonDataset:
        data = self.validate(frame, horizon_sec)
        unique_times = data["decision_at"].drop_duplicates().sort_values().reset_index(drop=True)
        if len(unique_times) < 10:
            raise ValueError("too few unique timestamps for development and lockbox")
        lockbox_position = min(
            len(unique_times) - 1,
            max(1, int(round(len(unique_times) * (1.0 - self.lockbox_fraction)))),
        )
        lockbox_start = unique_times.iloc[lockbox_position]
        purge = int(round(horizon_sec * self.purge_multiplier))
        embargo = int(round(horizon_sec * self.embargo_multiplier))
        development_cutoff = lockbox_start - timedelta(seconds=purge)
        development = data[
            (data["decision_at"] < development_cutoff)
            & (data["label_available_at"] < lockbox_start)
        ].copy().reset_index(drop=True)
        lockbox = data[data["decision_at"] >= lockbox_start].copy().reset_index(drop=True)
        if len(development) < self.minimum_train_rows + self.minimum_test_rows:
            raise ValueError("development panel is too small after lockbox purge")
        if len(lockbox) < self.minimum_test_rows:
            raise ValueError("lockbox is too small")
        folds = self._walk_forward_folds(development, horizon_sec, purge, embargo)
        if not folds:
            raise ValueError("no valid purged walk-forward folds")
        return HorizonDataset(
            horizon_sec=horizon_sec,
            development=development,
            lockbox=lockbox,
            folds=folds,
            development_fingerprint=_frame_fingerprint(development),
            lockbox_fingerprint=_frame_fingerprint(lockbox),
            lockbox_start=lockbox_start.isoformat().replace("+00:00", "Z"),
            lockbox_labels_materialized=True,
        )

    def build_sealed_development(
        self,
        frame: pd.DataFrame,
        horizon_sec: int,
        *,
        lockbox_start: object,
    ) -> HorizonDataset:
        """Build folds without materializing or fingerprinting lockbox labels."""

        data = self.validate(frame, horizon_sec)
        boundary = pd.to_datetime(lockbox_start, utc=True, errors="coerce")
        if pd.isna(boundary):
            raise ValueError("sealed lockbox boundary is invalid")
        development = data[
            (data["decision_at"] < boundary)
            & (data["label_available_at"] < boundary)
        ].copy().reset_index(drop=True)
        if len(development) < self.minimum_train_rows + self.minimum_test_rows:
            raise ValueError("sealed development panel is too small")
        purge = int(round(horizon_sec * self.purge_multiplier))
        embargo = int(round(horizon_sec * self.embargo_multiplier))
        folds = self._walk_forward_folds(development, horizon_sec, purge, embargo)
        if not folds:
            raise ValueError("no valid sealed-development walk-forward folds")
        return HorizonDataset(
            horizon_sec=horizon_sec,
            development=development,
            lockbox=pd.DataFrame(columns=development.columns),
            folds=folds,
            development_fingerprint=_frame_fingerprint(development),
            lockbox_fingerprint=None,
            lockbox_start=boundary.isoformat().replace("+00:00", "Z"),
            lockbox_labels_materialized=False,
        )

    @staticmethod
    def fingerprint(frame: pd.DataFrame) -> str:
        return _frame_fingerprint(frame)

    def _walk_forward_folds(
        self,
        development: pd.DataFrame,
        horizon_sec: int,
        purge_sec: int,
        embargo_sec: int,
    ) -> tuple[WalkForwardFold, ...]:
        times = development["decision_at"].drop_duplicates().sort_values().reset_index(drop=True)
        test_time_count = max(1, len(times) // (self.maximum_folds + 2))
        first_test = max(test_time_count * 2, 1)
        folds: list[WalkForwardFold] = []
        cursor = first_test
        while cursor < len(times) and len(folds) < self.maximum_folds:
            test_start = times.iloc[cursor]
            test_end_position = min(len(times), cursor + test_time_count)
            test_end = times.iloc[test_end_position - 1]
            train_cutoff = test_start - timedelta(seconds=purge_sec)
            train_mask = (
                (development["decision_at"] < train_cutoff)
                & (development["label_available_at"] < test_start)
            )
            test_mask = (
                (development["decision_at"] >= test_start)
                & (development["decision_at"] <= test_end)
            )
            train_indices = tuple(int(value) for value in development.index[train_mask])
            test_indices = tuple(int(value) for value in development.index[test_mask])
            if len(train_indices) >= self.minimum_train_rows and len(test_indices) >= self.minimum_test_rows:
                folds.append(
                    WalkForwardFold(
                        fold_id=f"h{horizon_sec}-wf-{len(folds) + 1:02d}",
                        train_indices=train_indices,
                        test_indices=test_indices,
                        train_start=development.loc[train_indices[0], "decision_at"].isoformat().replace("+00:00", "Z"),
                        train_end=development.loc[train_indices[-1], "decision_at"].isoformat().replace("+00:00", "Z"),
                        test_start=test_start.isoformat().replace("+00:00", "Z"),
                        test_end=test_end.isoformat().replace("+00:00", "Z"),
                        purge_sec=purge_sec,
                        embargo_sec=embargo_sec,
                    )
                )
            next_start = test_end + timedelta(seconds=embargo_sec)
            cursor = int(times.searchsorted(next_start, side="left"))
            if cursor <= test_end_position - 1:
                cursor = test_end_position
        return tuple(folds)

    def build(self, panels: Mapping[int, pd.DataFrame]) -> dict[int, HorizonDataset]:
        missing = [horizon for horizon in HORIZONS_SEC if horizon not in panels]
        if missing:
            raise ValueError(f"missing fixed horizons: {missing}")
        return {horizon: self.build_horizon(panels[horizon], horizon) for horizon in HORIZONS_SEC}


def dataset_manifest(dataset: HorizonDataset) -> dict[str, object]:
    return {
        "horizon_sec": dataset.horizon_sec,
        "development_rows": len(dataset.development),
        "development_fingerprint": dataset.development_fingerprint,
        "lockbox_rows": len(dataset.lockbox),
        "lockbox_start": dataset.lockbox_start,
        "lockbox_fingerprint": dataset.lockbox_fingerprint,
        "lockbox_labels_materialized": dataset.lockbox_labels_materialized,
        "lockbox_status": (
            "MATERIALIZED"
            if dataset.lockbox_labels_materialized
            else "SEALED_UNLABELED"
        ),
        "folds": [
            {
                "fold_id": fold.fold_id,
                "train_rows": len(fold.train_indices),
                "test_rows": len(fold.test_indices),
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "test_start": fold.test_start,
                "test_end": fold.test_end,
                "purge_sec": fold.purge_sec,
                "embargo_sec": fold.embargo_sec,
            }
            for fold in dataset.folds
        ],
    }


__all__: Sequence[str] = (
    "HORIZONS_SEC",
    "HORIZON_TIMEFRAME",
    "HorizonDataset",
    "PooledPanelBuilder",
    "WalkForwardFold",
    "causal_regime_labels",
    "dataset_manifest",
)
