from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class ResourceDecision:
    disk_used_percent: float
    predictor_realtime_allowed: bool
    backfill_allowed: bool
    research_allowed: bool
    large_integrity_check_allowed: bool
    reason: str


class ResourceGovernor:
    """Fail optional workloads before they can starve real-time prediction."""

    def __init__(
        self,
        *,
        stop_backfill_at_percent: float = 80.0,
        stop_research_at_percent: float = 90.0,
    ):
        if not 0 < stop_backfill_at_percent < stop_research_at_percent < 100:
            raise ValueError("disk pressure thresholds must satisfy 0 < backfill < research < 100")
        self.stop_backfill_at_percent = float(stop_backfill_at_percent)
        self.stop_research_at_percent = float(stop_research_at_percent)

    def decide(
        self,
        *,
        total_bytes: int,
        free_bytes: int,
        realtime_window_active: bool = True,
    ) -> ResourceDecision:
        if total_bytes <= 0 or not 0 <= free_bytes <= total_bytes:
            raise ValueError("disk capacity values are invalid")
        used = 100.0 * (total_bytes - free_bytes) / total_bytes
        backfill_allowed = used < self.stop_backfill_at_percent
        research_allowed = used < self.stop_research_at_percent
        reasons = []
        if not backfill_allowed:
            reasons.append("disk_at_or_above_backfill_gate")
        if not research_allowed:
            reasons.append("disk_at_or_above_research_gate")
        if realtime_window_active:
            reasons.append("large_integrity_check_blocked_during_realtime_window")
        return ResourceDecision(
            disk_used_percent=used,
            predictor_realtime_allowed=True,
            backfill_allowed=backfill_allowed,
            research_allowed=research_allowed,
            large_integrity_check_allowed=not realtime_window_active,
            reason=",".join(reasons) or "capacity_available",
        )

    def decide_path(
        self, path: Path, *, realtime_window_active: bool = True
    ) -> ResourceDecision:
        usage = shutil.disk_usage(Path(path))
        return self.decide(
            total_bytes=usage.total,
            free_bytes=usage.free,
            realtime_window_active=realtime_window_active,
        )


def validate_separate_data_roots(roots: Mapping[str, Path]) -> dict[str, str]:
    """Reject shared roots and Windows network shares for production SQLite data."""

    resolved: dict[str, str] = {}
    for name, raw_path in roots.items():
        raw = str(raw_path)
        if raw.startswith("\\\\") or raw.lower().startswith(("smb://", "nfs://")):
            raise ValueError(f"{name} data root cannot use SMB/NFS")
        normalized = str(Path(raw_path).expanduser().resolve())
        if normalized in resolved.values():
            raise ValueError(f"{name} data root is shared with another service")
        resolved[name] = normalized
    return resolved
