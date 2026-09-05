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
            FactorDefinition("bybit_orderbook_delta_l5", "market.microstructure.v1", "ratio", "directly_observed_flow", 30, 0.9),
            FactorDefinition("orderbook_imbalance_l5", "market.microstructure.v1", "ratio", "direct_observation", 30, 0.8),
            FactorDefinition("public_trade_imbalance_1m", "market.microstructure.v1", "ratio", "directly_observed_flow", 120, 0.9),
            FactorDefinition("ofi_1m", "market.microstructure.v1", "base_asset", "directly_observed_flow", 120, 0.9),
            FactorDefinition("aggressive_cvd_1m", "market.microstructure.v1", "base_asset", "directly_observed_flow", 120, 0.8),
            FactorDefinition("orderbook_depth_usdt_l5", "market.microstructure.v1", "usd", "direct_observation", 30, 0.9),
            FactorDefinition("microprice_deviation_bps", "market.microstructure.v1", "bps", "direct_observation", 30, 0.9),
            FactorDefinition("fill_probability", "market.microstructure.v1", "probability", "quality", 30, 0.9),
            FactorDefinition("expected_slippage_bps", "market.microstructure.v1", "bps", "quality", 30, 0.9),
            FactorDefinition("perpetual_basis_bps", "crypto.derivatives.v1", "bps", "direct_observation", 300, 0.8),
            FactorDefinition("open_interest_change_1h", "crypto.derivatives.v1", "ratio", "direct_observation", 900, 0.8),
            # The official history endpoint contains settled rates. Major linear
            # contracts commonly settle every eight hours, so the last actual
            # observation remains PIT-valid until the next scheduled settlement.
            FactorDefinition("funding_rate", "crypto.derivatives.v1", "ratio", "direct_observation", 32400, 0.8),
            FactorDefinition("liquidation_imbalance_5m", "crypto.derivatives.v1", "ratio", "directly_observed_flow", 600, 0.8),
            FactorDefinition("stablecoin_exchange_netflow_1h", "crypto.onchain.v1", "usd", "directly_observed_flow", 7200, 0.7),
            FactorDefinition("crypto_etf_netflow_daily", "crypto.onchain.v1", "usd", "directly_observed_flow", 172800, 0.8),
            FactorDefinition("usd_liquidity_proxy", "macro.us_liquidity.v1", "zscore", "inferred_rotation_proxy", 172800, 0.8),
            FactorDefinition("spy_return", "equity.us.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("qqq_return", "equity.us.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("vix_level", "crossasset.risk_appetite.v1", "index_points", "direct_observation", 432000, 0.9),
            FactorDefinition("tlt_return", "macro.us_liquidity.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("real_yield_10y", "macro.us_growth_inflation.v1", "percent", "direct_observation", 172800, 0.9),
            FactorDefinition("uup_return", "macro.us_liquidity.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("gld_return", "commodity.gold.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("uso_return", "commodity.oil.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("xlv_return", "sector.healthcare.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("ibb_return", "sector.healthcare.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("fxi_return", "equity.china.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("kweb_return", "equity.china.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("coin_return", "equity.us.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("mstr_return", "equity.us.v1", "return", "direct_observation", 3600, 0.9),
            FactorDefinition("fred_cpi_first_release_yoy_ratio", "macro.us_growth_inflation.v1", "ratio", "direct_observation", 3888000, 0.9),
            FactorDefinition("fred_payrolls_first_release_change_thousands", "macro.us_growth_inflation.v1", "thousands_of_persons", "direct_observation", 3888000, 0.9),
            FactorDefinition("fred_unemployment_first_release_pct", "macro.us_growth_inflation.v1", "percent", "direct_observation", 3888000, 0.9),
            FactorDefinition("alfred_cpi_mean_revision_delta", "macro.us_growth_inflation.v1", "source_units", "direct_observation", 34560000, 0.9),
            FactorDefinition("alfred_payrolls_mean_revision_delta", "macro.us_growth_inflation.v1", "source_units", "direct_observation", 3888000, 0.9),
            FactorDefinition("us_risk_appetite_score", "crossasset.risk_appetite.v1", "score", "inferred_rotation_proxy", 3600, 0.8),
            FactorDefinition("gold_rotation_score", "commodity.gold.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("oil_inflation_shock", "commodity.oil.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("china_risk_score", "equity.china.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("healthcare_defensive_score", "sector.healthcare.v1", "score", "inferred_rotation_proxy", 86400, 0.7),
            FactorDefinition("major_news_impact", "news.event_impact.v1", "score", "inferred_rotation_proxy", 21600, 0.8),
            FactorDefinition("tier_a_event_state", "news.event_impact.v1", "binary_24h_post_release_window", "direct_observation", 3888000, 0.9),
            FactorDefinition("fomc_statement_event_state", "news.event_impact.v1", "binary_24h_post_release_window", "direct_observation", 6048000, 1.0),
        ]
    )
