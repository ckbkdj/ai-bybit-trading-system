from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from core.models.two_stage import TwoStagePrediction


@dataclass(frozen=True)
class CalibrationCoveragePolicy:
    minimum_independent_timestamps_per_group: int = 30
    aggregate_p10_p90_tolerance: float = 0.05
    aggregate_p50_tolerance: float = 0.075
    subgroup_p10_p90_tolerance: float = 0.075
    subgroup_p50_tolerance: float = 0.10
    confidence: float = 0.95
    wilson_z: float = 1.959963984540054

    def __post_init__(self) -> None:
        if self.minimum_independent_timestamps_per_group < 30:
            raise ValueError("calibration groups require at least 30 timestamps")
        if not 0 < self.aggregate_p10_p90_tolerance <= 0.05:
            raise ValueError("aggregate p10/p90 tolerance cannot exceed 5%")
        if not 0 < self.aggregate_p50_tolerance <= 0.075:
            raise ValueError("aggregate p50 tolerance cannot exceed 7.5%")
        if not 0 < self.subgroup_p10_p90_tolerance <= 0.075:
            raise ValueError("subgroup p10/p90 tolerance cannot exceed 7.5%")
        if not 0 < self.subgroup_p50_tolerance <= 0.10:
            raise ValueError("subgroup p50 tolerance cannot exceed 10%")
        if self.confidence < 0.95 or self.wilson_z < 1.9599:
            raise ValueError("calibration confidence cannot be below 95%")


def directional_calibration_rows(
    frame: pd.DataFrame,
    predictions: Sequence[TwoStagePrediction],
) -> list[dict[str, object]]:
    """Select one direction-consistent action for each symbol/decision cluster."""

    if len(frame) != len(predictions):
        raise ValueError("prediction and calibration frame lengths differ")
    required = {
        "symbol",
        "horizon_sec",
        "decision_at",
        "regime",
        "side",
        "net_return",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"calibration frame is missing columns: {missing}")
    data = frame.reset_index(drop=True).copy()
    data["__prediction"] = list(predictions)
    output: list[dict[str, object]] = []
    for (_, _), group in data.groupby(["symbol", "decision_at"], sort=False):
        first_prediction = group.iloc[0]["__prediction"]
        selected_side = (
            "BUY"
            if float(first_prediction.p_up) >= float(first_prediction.p_down)
            else "SELL"
        )
        selected = group[group["side"].astype(str).str.upper() == selected_side]
        if len(selected) != 1:
            raise ValueError(
                "paired calibration actions must contain exactly one BUY and one SELL"
            )
        row = selected.iloc[0]
        prediction = row["__prediction"]
        values = np.asarray(
            [
                row["net_return"],
                prediction.return_p10,
                prediction.return_p50,
                prediction.return_p90,
            ],
            dtype=float,
        )
        if not np.isfinite(values).all():
            raise ValueError("calibration outcomes and quantiles must be finite")
        if not values[1] <= values[2] <= values[3]:
            raise ValueError("predicted return quantiles must be monotonic")
        decision_at = pd.to_datetime(row["decision_at"], utc=True, errors="coerce")
        if pd.isna(decision_at):
            raise ValueError("calibration decision timestamps must be valid UTC values")
        output.append(
            {
                "horizon_sec": int(row["horizon_sec"]),
                "symbol": str(row["symbol"]).upper(),
                "regime": str(row["regime"]),
                "decision_at": pd.Timestamp(decision_at),
                "actual_net_return": float(values[0]),
                "return_p10": float(values[1]),
                "return_p50": float(values[2]),
                "return_p90": float(values[3]),
                "selected_side": selected_side,
            }
        )
    return output


def _wilson_interval(successes: float, observations: int, z: float) -> tuple[float, float]:
    if observations <= 0:
        return 0.0, 1.0
    proportion = successes / observations
    denominator = 1.0 + z * z / observations
    centre = (proportion + z * z / (2.0 * observations)) / denominator
    radius = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / observations
            + z * z / (4.0 * observations * observations)
        )
        / denominator
    )
    return max(0.0, centre - radius), min(1.0, centre + radius)


def _group_coverage(
    group: pd.DataFrame,
    *,
    scope: str,
    group_key: Mapping[str, object],
    policy: CalibrationCoveragePolicy,
) -> dict[str, object]:
    clustered = pd.DataFrame(
        {
            "decision_at": group["decision_at"],
            "p10": group["actual_net_return"] <= group["return_p10"],
            "p50": group["actual_net_return"] <= group["return_p50"],
            "p90": group["actual_net_return"] <= group["return_p90"],
        }
    ).groupby("decision_at", sort=True)[["p10", "p50", "p90"]].mean()
    independent_count = int(len(clustered))
    aggregate = scope == "horizon"
    tolerances = {
        "p10": (
            policy.aggregate_p10_p90_tolerance
            if aggregate
            else policy.subgroup_p10_p90_tolerance
        ),
        "p50": (
            policy.aggregate_p50_tolerance
            if aggregate
            else policy.subgroup_p50_tolerance
        ),
        "p90": (
            policy.aggregate_p10_p90_tolerance
            if aggregate
            else policy.subgroup_p10_p90_tolerance
        ),
    }
    targets = {"p10": 0.10, "p50": 0.50, "p90": 0.90}
    quantiles: dict[str, object] = {}
    enough_samples = independent_count >= policy.minimum_independent_timestamps_per_group
    for name, target in targets.items():
        actual = float(clustered[name].mean()) if independent_count else 0.0
        lower, upper = _wilson_interval(
            actual * independent_count,
            independent_count,
            policy.wilson_z,
        )
        quantiles[name] = {
            "target_coverage": target,
            "actual_coverage": actual,
            "absolute_error": abs(actual - target),
            "maximum_absolute_error": tolerances[name],
            "wilson_lower": lower,
            "wilson_upper": upper,
            "target_inside_wilson_interval": lower <= target <= upper,
            "passed": bool(
                enough_samples
                and abs(actual - target) <= tolerances[name]
                and lower <= target <= upper
            ),
        }
    passed = enough_samples and all(
        bool(item["passed"]) for item in quantiles.values()
    )
    return {
        "scope": scope,
        "group": dict(group_key),
        "prediction_count": int(len(group)),
        "independent_timestamp_count": independent_count,
        "minimum_independent_timestamp_count": (
            policy.minimum_independent_timestamps_per_group
        ),
        "simultaneous_symbols_clustered": True,
        "quantiles": quantiles,
        "passed": passed,
        "failure_reason": None if passed else (
            "quantile_coverage_outside_preregistered_bounds"
            if enough_samples
            else "insufficient_independent_timestamps"
        ),
    }


def evaluate_quantile_coverage(
    rows: Sequence[Mapping[str, object]],
    *,
    required_horizons: Sequence[int],
    policy: CalibrationCoveragePolicy | None = None,
) -> dict[str, object]:
    cfg = policy or CalibrationCoveragePolicy()
    required = tuple(sorted({int(value) for value in required_horizons}))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return {
            "status": "FAILED",
            "passed": False,
            "complete": False,
            "reason": "no_outer_oos_calibration_rows",
            "required_horizons": list(required),
            "observed_horizons": [],
            "policy": asdict(cfg),
            "groups": [],
        }
    expected_columns = {
        "horizon_sec",
        "symbol",
        "regime",
        "decision_at",
        "actual_net_return",
        "return_p10",
        "return_p50",
        "return_p90",
    }
    missing = sorted(expected_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"calibration evidence is missing columns: {missing}")
    frame["decision_at"] = pd.to_datetime(
        frame["decision_at"], utc=True, errors="coerce"
    )
    numeric_columns = [
        "actual_net_return",
        "return_p10",
        "return_p50",
        "return_p90",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    invalid_row_count = int(
        frame[["decision_at", *numeric_columns]].isna().any(axis=1).sum()
    )
    monotonic_failure_count = int(
        (~(
            (frame["return_p10"] <= frame["return_p50"])
            & (frame["return_p50"] <= frame["return_p90"])
        )).sum()
    )
    observed = tuple(
        sorted(pd.to_numeric(frame["horizon_sec"], errors="coerce").dropna().astype(int).unique())
    )
    scopes = (
        ("horizon", ("horizon_sec",)),
        ("horizon_symbol", ("horizon_sec", "symbol")),
        ("horizon_regime", ("horizon_sec", "regime")),
        (
            "horizon_symbol_regime",
            ("horizon_sec", "symbol", "regime"),
        ),
    )
    groups: list[dict[str, object]] = []
    for scope, columns in scopes:
        grouper: object = columns[0] if len(columns) == 1 else list(columns)
        for raw_key, group in frame.groupby(grouper, sort=True, dropna=False):
            values = raw_key if isinstance(raw_key, tuple) else (raw_key,)
            key = {column: value for column, value in zip(columns, values)}
            groups.append(
                _group_coverage(
                    group,
                    scope=scope,
                    group_key=key,
                    policy=cfg,
                )
            )
    horizons_complete = observed == required
    passed = bool(
        invalid_row_count == 0
        and monotonic_failure_count == 0
        and horizons_complete
        and groups
        and all(bool(group["passed"]) for group in groups)
    )
    return {
        "schema_version": "profitability-calibration-coverage.v1",
        "status": "PASSED" if passed else "FAILED",
        "passed": passed,
        "complete": bool(
            invalid_row_count == 0
            and monotonic_failure_count == 0
            and horizons_complete
            and groups
        ),
        "method": (
            "outer-OOS directional action quantile coverage; simultaneous symbols "
            "clustered by decision timestamp; 95% Wilson interval"
        ),
        "tuning_use": False,
        "record_count": int(len(frame)),
        "unique_decision_timestamp_count": int(frame["decision_at"].nunique()),
        "required_horizons": list(required),
        "observed_horizons": list(observed),
        "invalid_row_count": invalid_row_count,
        "monotonic_failure_count": monotonic_failure_count,
        "policy": asdict(cfg),
        "failed_group_count": sum(not bool(group["passed"]) for group in groups),
        "groups": groups,
    }


__all__ = (
    "CalibrationCoveragePolicy",
    "directional_calibration_rows",
    "evaluate_quantile_coverage",
)
