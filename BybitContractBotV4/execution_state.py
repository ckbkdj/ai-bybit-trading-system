from __future__ import annotations

from enum import Enum


class ExecutionState(str, Enum):
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    CLAIMED = "CLAIMED"
    RISK_APPROVED = "RISK_APPROVED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    SUPERSEDED = "SUPERSEDED"
    RISK_BLOCKED = "RISK_BLOCKED"


TERMINAL_STATES = {
    ExecutionState.FILLED,
    ExecutionState.REJECTED,
    ExecutionState.EXPIRED,
    ExecutionState.CANCELLED,
    ExecutionState.FAILED,
    ExecutionState.SUPERSEDED,
    ExecutionState.RISK_BLOCKED,
}


ALLOWED_TRANSITIONS = {
    ExecutionState.RECEIVED: {
        ExecutionState.VALIDATED, ExecutionState.REJECTED, ExecutionState.EXPIRED,
        ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.VALIDATED: {
        ExecutionState.CLAIMED, ExecutionState.REJECTED, ExecutionState.EXPIRED,
        ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.CLAIMED: {
        ExecutionState.RISK_APPROVED, ExecutionState.RISK_BLOCKED, ExecutionState.EXPIRED,
        ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.RISK_APPROVED: {
        ExecutionState.SUBMITTING, ExecutionState.RISK_BLOCKED, ExecutionState.EXPIRED,
        ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.SUBMITTING: {
        ExecutionState.SUBMITTED, ExecutionState.ACKNOWLEDGED,
        ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED,
        ExecutionState.CANCELLED, ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.SUBMITTED: {
        ExecutionState.ACKNOWLEDGED, ExecutionState.PARTIALLY_FILLED,
        ExecutionState.FILLED, ExecutionState.CANCELLED, ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.ACKNOWLEDGED: {
        ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED,
        ExecutionState.CANCELLED, ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    ExecutionState.PARTIALLY_FILLED: {
        ExecutionState.FILLED, ExecutionState.CANCELLED, ExecutionState.SUPERSEDED, ExecutionState.FAILED,
    },
    # An actual fill wins a cancel/fill race; this is the only terminal upgrade.
    ExecutionState.CANCELLED: {ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED},
    ExecutionState.SUPERSEDED: {ExecutionState.PARTIALLY_FILLED, ExecutionState.FILLED},
}


class InvalidTransition(RuntimeError):
    pass


def can_transition(current: ExecutionState, target: ExecutionState) -> bool:
    return current == target or target in ALLOWED_TRANSITIONS.get(current, set())


def require_transition(current: ExecutionState, target: ExecutionState) -> None:
    if not can_transition(current, target):
        raise InvalidTransition(f"execution state cannot move from {current.value} to {target.value}")
