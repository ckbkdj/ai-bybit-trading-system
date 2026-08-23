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


def scaled_feature_range_guard_score(values: Any, scaler: Any) -> DistributionCheck:
    """Return a bounded training-range guard from fitted-scaler space.

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


def scaled_feature_ood_score(values: Any, scaler: Any) -> DistributionCheck:
    """Compatibility alias; this is a range guard, not statistical OOD detection."""

    return scaled_feature_range_guard_score(values, scaler)


def population_stability_index(reference: Any, current: Any, bins: int = 10) -> float:
    ref = np.asarray(reference, dtype=float).ravel()
    cur = np.asarray(current, dtype=float).ravel()
    ref = ref[np.isfinite(ref)]
    cur = cur[np.isfinite(cur)]
    if len(ref) < bins or len(cur) < bins:
        return float("inf")
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0 if np.allclose(ref.mean(), cur.mean()) else float("inf")
    edges[0], edges[-1] = -np.inf, np.inf
    ref_hist = np.histogram(ref, bins=edges)[0] / len(ref)
    cur_hist = np.histogram(cur, bins=edges)[0] / len(cur)
    ref_hist = np.clip(ref_hist, 1e-6, None)
    cur_hist = np.clip(cur_hist, 1e-6, None)
    return float(np.sum((cur_hist - ref_hist) * np.log(cur_hist / ref_hist)))


def quantile_wasserstein_distance(reference: Any, current: Any) -> float:
    ref = np.asarray(reference, dtype=float).ravel()
    cur = np.asarray(current, dtype=float).ravel()
    ref = ref[np.isfinite(ref)]
    cur = cur[np.isfinite(cur)]
    if not len(ref) or not len(cur):
        return float("inf")
    quantiles = np.linspace(0, 1, 101)
    return float(
        np.mean(np.abs(np.quantile(ref, quantiles) - np.quantile(cur, quantiles)))
    )


def predictive_health_metrics(
    probabilities: Any,
    labels: Any,
    *,
    residuals: Any = (),
    conformal_contains: Any = (),
    regimes: Sequence[str] = (),
    bins: int = 10,
) -> dict[str, Any]:
    probs = np.asarray(probabilities, dtype=float)
    truth = np.asarray(labels, dtype=int).ravel()
    if probs.ndim != 2 or len(probs) != len(truth) or not len(truth):
        raise ValueError("probabilities and labels are invalid")
    if not np.isfinite(probs).all() or not np.allclose(probs.sum(axis=1), 1, atol=1e-6):
        raise ValueError("probability rows must be finite and sum to one")
    if truth.min() < 0 or truth.max() >= probs.shape[1]:
        raise ValueError("labels are outside probability columns")
    one_hot = np.eye(probs.shape[1])[truth]
    brier = float(np.mean(np.sum((probs - one_hot) ** 2, axis=1)))
    confidence = probs.max(axis=1)
    correct = probs.argmax(axis=1) == truth
    ece = 0.0
    for low in np.linspace(0, 1, bins, endpoint=False):
        high = low + 1 / bins
        mask = (confidence >= low) & (
            confidence < high if high < 1 else confidence <= high
        )
        if mask.any():
            ece += float(mask.mean()) * abs(
                float(correct[mask].mean()) - float(confidence[mask].mean())
            )
    clipped = np.clip(probs, 1e-12, 1)
    entropy = float(np.mean(-np.sum(clipped * np.log(clipped), axis=1)))
    residual_array = np.asarray(residuals, dtype=float).ravel()
    residual_array = residual_array[np.isfinite(residual_array)]
    conformal = np.asarray(conformal_contains, dtype=bool).ravel()
    regime_coverage = {
        regime: int(sum(item == regime for item in regimes))
        for regime in sorted(set(regimes))
    }
    return {
        "brier_score": brier,
        "expected_calibration_error": ece,
        "output_entropy": entropy,
        "residual_bias": float(residual_array.mean()) if len(residual_array) else None,
        "residual_mae": float(np.abs(residual_array).mean()) if len(residual_array) else None,
        "conformal_coverage": float(conformal.mean()) if len(conformal) else None,
        "regime_sample_coverage": regime_coverage,
    }


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
    "scaled_feature_range_guard_score",
    "population_stability_index",
    "quantile_wasserstein_distance",
    "predictive_health_metrics",
    "source_is_reliable",
)
