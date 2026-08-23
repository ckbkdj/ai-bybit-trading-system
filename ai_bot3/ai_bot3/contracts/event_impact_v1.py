from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from .common import ContractModel, ensure_utc


class DirectionDistribution(ContractModel):
    positive: float = Field(ge=0, le=1)
    neutral: float = Field(ge=0, le=1)
    negative: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def sums_to_one(self):
        if abs(self.positive + self.neutral + self.negative - 1) > 1e-6:
            raise ValueError("event direction probabilities must sum to one")
        return self


class AssetImpact(ContractModel):
    direction_distribution: DirectionDistribution
    impact_strength: float = Field(ge=0, le=1)
    start_delay_sec: int = Field(default=0, ge=0)
    half_life_sec: int = Field(gt=0)


class EventScenario(ContractModel):
    name: str = Field(min_length=1)
    probability: float = Field(ge=0, le=1)
    description: str = ""


class EventImpactVector(ContractModel):
    schema_version: Literal["event-impact-vector.v1"] = "event-impact-vector.v1"
    event_id: str = Field(min_length=4, max_length=100)
    revision: int = Field(ge=1)
    event_type: str
    data_cutoff: datetime
    created_at: datetime
    novelty: float = Field(ge=0, le=1)
    source_reliability: float = Field(ge=0, le=1)
    confirmation_count: int = Field(ge=0)
    primary_source_verified: bool
    affected_assets: Dict[str, AssetImpact]
    scenarios: List[EventScenario]
    event_blackout: bool = False
    risk_directive: Literal["NONE", "REDUCE_ONLY", "BLACKOUT"] = "NONE"
    blackout_until: Optional[datetime] = None
    evidence_source_ids: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    @field_validator("data_cutoff", "created_at", "blackout_until")
    @classmethod
    def utc_datetimes(cls, value):
        return ensure_utc(value) if value is not None else value

    @model_validator(mode="after")
    def scenario_probabilities_and_blackout(self):
        if not self.scenarios or abs(sum(item.probability for item in self.scenarios) - 1) > 1e-6:
            raise ValueError("scenario probabilities must sum to one")
        if self.event_blackout and self.blackout_until is None:
            raise ValueError("event blackout requires blackout_until")
        if self.event_blackout != (self.risk_directive == "BLACKOUT"):
            raise ValueError("event_blackout and BLACKOUT risk directive must be consistent")
        if self.risk_directive == "REDUCE_ONLY" and self.primary_source_verified:
            raise ValueError("verified Tier A evidence must use a final NONE/BLACKOUT directive")
        if self.blackout_until is not None and self.blackout_until <= self.created_at:
            raise ValueError("blackout_until must be after creation")
        return self
