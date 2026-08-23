from __future__ import annotations

from datetime import datetime

from contracts.event_impact_v1 import EventImpactVector

from .scenario_builder import normalize_scenarios
from .source_ranker import EvidenceSource, aggregate_reliability, verified_primary_source


def build_impact_vector(
    *,
    event_id: str,
    revision: int,
    event_type: str,
    data_cutoff: datetime,
    created_at: datetime,
    novelty: float,
    sources: list[EvidenceSource],
    affected_assets: dict,
    scenarios: list[dict],
    event_blackout: bool = False,
    blackout_until: datetime | None = None,
    provisional_risk_reduction: bool = False,
) -> EventImpactVector:
    primary = verified_primary_source(sources)
    if event_blackout and not primary:
        raise ValueError("event blackout requires a verified Tier A source")
    verified_tier_b = sum(
        source.tier.value == "B" and source.verified for source in sources
    )
    if provisional_risk_reduction and (primary or verified_tier_b < 2):
        raise ValueError(
            "provisional risk reduction requires two verified Tier B sources and no Tier A"
        )
    return EventImpactVector.model_validate(
        {
            "event_id": event_id,
            "revision": revision,
            "event_type": event_type,
            "data_cutoff": data_cutoff,
            "created_at": created_at,
            "novelty": novelty,
            "source_reliability": aggregate_reliability(sources),
            "confirmation_count": len(sources),
            "primary_source_verified": primary,
            "affected_assets": affected_assets,
            "scenarios": normalize_scenarios(scenarios),
            "event_blackout": event_blackout,
            "risk_directive": (
                "BLACKOUT"
                if event_blackout
                else "REDUCE_ONLY" if provisional_risk_reduction else "NONE"
            ),
            "blackout_until": blackout_until,
            "evidence_source_ids": [source.source_id for source in sources],
        }
    )
