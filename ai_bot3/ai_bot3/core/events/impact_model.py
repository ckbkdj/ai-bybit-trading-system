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
) -> EventImpactVector:
    primary = verified_primary_source(sources)
    if event_blackout and not primary:
        raise ValueError("event blackout requires a verified Tier A source")
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
            "blackout_until": blackout_until,
            "evidence_source_ids": [source.source_id for source in sources],
        }
    )
