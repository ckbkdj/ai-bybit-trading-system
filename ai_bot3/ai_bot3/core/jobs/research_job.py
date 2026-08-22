from __future__ import annotations

from enum import Enum


class ResearchState(str, Enum):
    DETECTED = "DETECTED"
    DEDUPLICATED = "DEDUPLICATED"
    TRIAGED = "TRIAGED"
    PRIMARY_SOURCE_VERIFYING = "PRIMARY_SOURCE_VERIFYING"
    ENTITY_RESOLVED = "ENTITY_RESOLVED"
    SCENARIOS_BUILDING = "SCENARIOS_BUILDING"
    IMPACT_QUANTIFYING = "IMPACT_QUANTIFYING"
    COMPLETED = "COMPLETED"
    SUPERSEDED = "SUPERSEDED"
    FAILED = "FAILED"


TERMINAL_RESEARCH_STATES = {
    ResearchState.COMPLETED,
    ResearchState.SUPERSEDED,
    ResearchState.FAILED,
}


NEXT_STATES = {
    ResearchState.DETECTED: {ResearchState.DEDUPLICATED, ResearchState.SUPERSEDED, ResearchState.FAILED},
    ResearchState.DEDUPLICATED: {ResearchState.TRIAGED, ResearchState.SUPERSEDED, ResearchState.FAILED},
    ResearchState.TRIAGED: {ResearchState.PRIMARY_SOURCE_VERIFYING, ResearchState.SUPERSEDED, ResearchState.FAILED},
    ResearchState.PRIMARY_SOURCE_VERIFYING: {
        ResearchState.ENTITY_RESOLVED, ResearchState.SUPERSEDED, ResearchState.FAILED,
    },
    ResearchState.ENTITY_RESOLVED: {
        ResearchState.SCENARIOS_BUILDING, ResearchState.SUPERSEDED, ResearchState.FAILED,
    },
    ResearchState.SCENARIOS_BUILDING: {
        ResearchState.IMPACT_QUANTIFYING, ResearchState.SUPERSEDED, ResearchState.FAILED,
    },
    ResearchState.IMPACT_QUANTIFYING: {
        ResearchState.COMPLETED, ResearchState.SUPERSEDED, ResearchState.FAILED,
    },
}


def require_research_transition(current: ResearchState, target: ResearchState) -> None:
    if current == target:
        return
    if target not in NEXT_STATES.get(current, set()):
        raise ValueError(f"research state cannot move from {current.value} to {target.value}")
