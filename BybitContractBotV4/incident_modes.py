from __future__ import annotations

from enum import Enum


class IncidentMode(str, Enum):
    NORMAL = "NORMAL"
    FREEZE_NEW_RISK = "FREEZE_NEW_RISK"
    CANCEL_ENTRIES = "CANCEL_ENTRIES"
    PROTECT_ONLY = "PROTECT_ONLY"
    FLATTEN = "FLATTEN"
    MANUAL_HANDOVER = "MANUAL_HANDOVER"


def action_allowed(mode: str, action: str) -> bool:
    incident = IncidentMode(str(mode).upper())
    normalized = str(action).upper()
    if incident is IncidentMode.NORMAL:
        return True
    if incident is IncidentMode.MANUAL_HANDOVER:
        return False
    if incident is IncidentMode.CANCEL_ENTRIES:
        return normalized == "CANCEL"
    if incident is IncidentMode.FLATTEN:
        return normalized in {"CANCEL", "REDUCE", "CLOSE"}
    # FREEZE_NEW_RISK and PROTECT_ONLY keep deterministic risk-reduction paths alive.
    return normalized in {"CANCEL", "REDUCE", "CLOSE"}
