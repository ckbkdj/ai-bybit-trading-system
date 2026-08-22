from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Optional


FactorSemantics = Literal["direct_observation", "directly_observed_flow", "inferred_rotation_proxy", "quality"]


FACTOR_SETS = (
    "market.price_technical.v1",
    "market.microstructure.v1",
    "crypto.derivatives.v1",
    "crypto.onchain.v1",
    "macro.us_liquidity.v1",
    "macro.us_growth_inflation.v1",
    "crossasset.risk_appetite.v1",
    "commodity.gold.v1",
    "commodity.oil.v1",
    "equity.us.v1",
    "equity.china.v1",
    "sector.healthcare.v1",
    "news.event_impact.v1",
    "calendar.session.v1",
    "data.quality.v1",
)


@dataclass(frozen=True)
class FactorDefinition:
    name: str
    factor_set: str
    unit: str
    semantics: FactorSemantics
    maximum_age_sec: int
    minimum_quality: float = 0.0
    description: str = ""

    def __post_init__(self):
        if self.factor_set not in FACTOR_SETS:
            raise ValueError(f"unknown factor set: {self.factor_set}")
        if not self.name or self.maximum_age_sec <= 0:
            raise ValueError("factor name and positive maximum age are required")
        if not 0 <= self.minimum_quality <= 1:
            raise ValueError("minimum_quality must be in [0, 1]")


class FactorRegistry:
    def __init__(self, definitions: Iterable[FactorDefinition] = ()):
        self._definitions: dict[str, FactorDefinition] = {}
        for definition in definitions:
            self.register(definition)

    def register(self, definition: FactorDefinition) -> None:
        existing = self._definitions.get(definition.name)
        if existing and existing != definition:
            raise ValueError(f"factor is already registered with a different definition: {definition.name}")
        self._definitions[definition.name] = definition

    def get(self, name: str) -> Optional[FactorDefinition]:
        return self._definitions.get(name)

    def require(self, name: str) -> FactorDefinition:
        definition = self.get(name)
        if definition is None:
            raise KeyError(f"factor is not registered: {name}")
        return definition

    def by_set(self, factor_set: str) -> tuple[FactorDefinition, ...]:
        if factor_set not in FACTOR_SETS:
            raise KeyError(factor_set)
        return tuple(item for item in self._definitions.values() if item.factor_set == factor_set)

    def all(self) -> tuple[FactorDefinition, ...]:
        return tuple(self._definitions.values())


def default_registry() -> FactorRegistry:
    return FactorRegistry(
        [
            FactorDefinition("orderbook_spread_bps", "market.microstructure.v1", "bps", "direct_observation", 30, 0.8),
            FactorDefinition("orderbook_imbalance_l5", "market.microstructure.v1", "ratio", "direct_observation", 30, 0.8),
            FactorDefinition("aggressive_cvd_1m", "market.microstructure.v1", "base_asset", "directly_observed_flow", 120, 0.8),
            FactorDefinition("open_interest_change_1h", "crypto.derivatives.v1", "ratio", "direct_observation", 900, 0.8),
            FactorDefinition("funding_rate", "crypto.derivatives.v1", "ratio", "direct_observation", 3600, 0.8),
            FactorDefinition("stablecoin_exchange_netflow_1h", "crypto.onchain.v1", "usd", "directly_observed_flow", 7200, 0.7),
            FactorDefinition("usd_liquidity_proxy", "macro.us_liquidity.v1", "zscore", "inferred_rotation_proxy", 172800, 0.8),
            FactorDefinition("us_risk_appetite_score", "crossasset.risk_appetite.v1", "score", "inferred_rotation_proxy", 3600, 0.8),
            FactorDefinition("gold_rotation_score", "commodity.gold.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("oil_inflation_shock", "commodity.oil.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("china_risk_score", "equity.china.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("healthcare_defensive_score", "sector.healthcare.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("major_news_impact", "news.event_impact.v1", "score", "inferred_rotation_proxy", 21600, 0.8),
        ]
    )
