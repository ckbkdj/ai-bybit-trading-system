from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class RotationScore:
    value: float
    semantics: str = "inferred_rotation_proxy"


def regime_weighted_rotation(
    normalized_inputs: Mapping[str, float],
    *,
    regime: str,
    trained_weights: Mapping[str, Mapping[str, float]],
) -> RotationScore:
    """Apply externally trained asset/horizon/regime weights; no fixed gold/BTC sign is embedded."""

    weights = trained_weights.get(regime)
    if not weights:
        raise ValueError(f"no calibrated cross-asset weights for regime: {regime}")
    numerator = 0.0
    denominator = 0.0
    for name, weight in weights.items():
        if name not in normalized_inputs:
            continue
        numerator += float(normalized_inputs[name]) * float(weight)
        denominator += abs(float(weight))
    if denominator == 0:
        raise ValueError("no overlapping cross-asset inputs and weights")
    return RotationScore(max(-1.0, min(1.0, numerator / denominator)))


def macro_surprise(actual: float, previous: float, consensus: float | None) -> dict[str, float | None]:
    return {
        "actual_minus_previous": actual - previous,
        # Missing consensus remains missing; never fabricate a survey surprise.
        "actual_minus_consensus": None if consensus is None else actual - consensus,
    }
