from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class SourceTier(str, Enum):
    A = "A"
    B = "B"
    C = "C"


@dataclass(frozen=True)
class EvidenceSource:
    source_id: str
    tier: SourceTier
    published_at: datetime
    verified: bool
    reliability: float

    def __post_init__(self):
        if not self.source_id or not 0 <= self.reliability <= 1:
            raise ValueError("invalid evidence source")


def verified_primary_source(sources: list[EvidenceSource]) -> bool:
    return any(source.tier is SourceTier.A and source.verified for source in sources)


def aggregate_reliability(sources: list[EvidenceSource]) -> float:
    if not sources:
        return 0.0
    tier_weight = {SourceTier.A: 1.0, SourceTier.B: 0.75, SourceTier.C: 0.35}
    weighted = [source.reliability * tier_weight[source.tier] for source in sources]
    return max(0.0, min(1.0, sum(weighted) / len(weighted)))
