from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceRole:
    role: str
    preferred_tier: str
    factor_sets: tuple[str, ...]
    permits_blackout_confirmation: bool = False


SOURCE_ROLES = {
    "exchange_first_party": SourceRole(
        "exchange_first_party", "A",
        ("market.microstructure.v1", "crypto.derivatives.v1"),
        True,
    ),
    "macro_official": SourceRole(
        "macro_official", "A",
        ("macro.us_liquidity.v1", "macro.us_growth_inflation.v1"),
        True,
    ),
    "regulatory_official": SourceRole(
        "regulatory_official", "A",
        ("news.event_impact.v1", "sector.healthcare.v1"),
        True,
    ),
    "professional_market_data": SourceRole(
        "professional_market_data", "B",
        ("crypto.onchain.v1", "crossasset.risk_appetite.v1", "commodity.gold.v1", "commodity.oil.v1"),
    ),
    "news_or_social_discovery": SourceRole(
        "news_or_social_discovery", "C", ("news.event_impact.v1",), False
    ),
}


def require_source_role(role: str) -> SourceRole:
    try:
        return SOURCE_ROLES[role]
    except KeyError as exc:
        raise ValueError(f"unknown source role: {role}") from exc
