from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class DistributionCheck:
    score: float
    violation_fraction: float
    maximum_excess: float
    method: str


def scaled_feature_ood_score(values: Any, scaler: Any) -> DistributionCheck:
    """Return a bounded, deterministic OOD alarm from fitted-scaler space.

    This is deliberately a conservative monitoring heuristic, not a claim of
    statistical density estimation. Min-max scalers are checked against their
    fitted feature range. Standardized inputs are checked outside +/-3.5 sigma.
    Non-finite inputs fail closed with score 1.
    """

    array = np.asarray(values, dtype=float)
    if array.size == 0 or not np.isfinite(array).all():
        return DistributionCheck(1.0, 1.0, 1.0, "invalid_or_empty")

    if hasattr(scaler, "feature_range"):
        low, high = (float(v) for v in scaler.feature_range)
        width = max(high - low, 1e-12)
        below = np.maximum(low - array, 0.0) / width
        above = np.maximum(array - high, 0.0) / width
        excess = np.maximum(below, above)
        method = "minmax_training_range"
    else:
        threshold = 3.5
        excess = np.maximum(np.abs(array) - threshold, 0.0) / threshold
        method = "standardized_3_5_sigma"

    violation_fraction = float(np.mean(excess > 0))
    maximum_excess = float(np.max(excess))
    score = min(1.0, violation_fraction + maximum_excess)
    return DistributionCheck(score, violation_fraction, maximum_excess, method)


def factor_group_scores(snapshot: Mapping[str, Any], factor_bias: float, llm_signal: float) -> dict[str, float]:
    """Expose auditable factor-family scores without inventing missing data."""

    def value(key: str) -> float:
        try:
            candidate = float(snapshot.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return float(np.clip(candidate, -1.0, 1.0))

    positioning = np.mean(
        [
            value("funding_acceleration"),
            value("long_short_ratio_change"),
            value("open_interest_change"),
            value("taker_buy_sell_ratio"),
        ]
    )
    liquidation = np.mean(
        [
            value("liquidation_imbalance"),
            value("liq_short_pressure_log") - value("liq_long_pressure_log"),
        ]
    )
    context = np.mean(
        [
            value("news_sentiment"),
            value("financial_calendar_score"),
            value("whale_net_flow_score"),
            value("fear_greed_score"),
        ]
    )
    return {
        "technical_and_market_fusion": float(np.clip(factor_bias, -1.0, 1.0)),
        "derivatives_positioning": float(np.clip(positioning, -1.0, 1.0)),
        "liquidation_structure": float(np.clip(liquidation, -1.0, 1.0)),
        "news_macro_onchain_context": float(np.clip(context, -1.0, 1.0)),
        "llm_auxiliary": float(np.clip(llm_signal, -1.0, 1.0)),
    }


def source_is_reliable(status: Any, age_seconds: Any, *, maximum_age_seconds: int = 120) -> bool:
    try:
        age = float(age_seconds)
    except (TypeError, ValueError):
        return False
    return str(status or "").lower() == "ok" and 0.0 <= age <= maximum_age_seconds


__all__: Sequence[str] = (
    "DistributionCheck",
    "factor_group_scores",
    "scaled_feature_ood_score",
    "source_is_reliable",
)
