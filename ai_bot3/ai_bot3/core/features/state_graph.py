from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping


STATE_NAMES = (
    "crypto_liquidity_score",
    "usd_liquidity_score",
    "risk_appetite_score",
    "inflation_shock_score",
    "gold_safety_rotation_score",
    "china_growth_liquidity_score",
    "healthcare_defensive_score",
    "geopolitical_event_score",
)


@dataclass(frozen=True)
class StateInput:
    value: float
    reliability: float
    age_sec: float
    half_life_sec: float
    regime_weight: float
    semantics: str

    def __post_init__(self):
        if not -10 <= self.value <= 10:
            raise ValueError("state input must be normalized to a robust score")
        if not 0 <= self.reliability <= 1 or self.age_sec < 0 or self.half_life_sec <= 0:
            raise ValueError("invalid reliability or freshness inputs")
        if self.semantics not in {"direct_observation", "directly_observed_flow", "inferred_rotation_proxy"}:
            raise ValueError("factor semantics must distinguish observation from inference")

    @property
    def freshness(self) -> float:
        return math.exp(-self.age_sec / self.half_life_sec)

    @property
    def effect(self) -> float:
        return self.value * self.reliability * self.freshness * self.regime_weight


@dataclass(frozen=True)
class MarketStateScore:
    name: str
    value: float
    coverage: float
    directly_observed_count: int
    inferred_count: int


def aggregate_state(name: str, inputs: Mapping[str, StateInput], expected_factor_count: int) -> MarketStateScore:
    if name not in STATE_NAMES:
        raise ValueError(f"unknown market state: {name}")
    if expected_factor_count <= 0 or len(inputs) > expected_factor_count:
        raise ValueError("invalid expected factor count")
    weights = [abs(item.regime_weight) * item.reliability * item.freshness for item in inputs.values()]
    denominator = sum(weights)
    value = sum(item.effect for item in inputs.values()) / denominator if denominator else 0.0
    direct = sum(item.semantics != "inferred_rotation_proxy" for item in inputs.values())
    return MarketStateScore(
        name=name,
        value=max(-1.0, min(1.0, value)),
        coverage=len(inputs) / expected_factor_count,
        directly_observed_count=direct,
        inferred_count=len(inputs) - direct,
    )
