from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DataQualityScore:
    coverage: float
    source_reliability: float
    freshness: float
    revision_stability: float
    source_outage: bool = False

    def __post_init__(self):
        values = (self.coverage, self.source_reliability, self.freshness, self.revision_stability)
        if any(not 0 <= value <= 1 for value in values):
            raise ValueError("quality components must be in [0, 1]")

    @property
    def value(self) -> float:
        if self.source_outage:
            return 0.0
        # Geometric form prevents one critically weak component being hidden by averages.
        return (self.coverage * self.source_reliability * self.freshness * self.revision_stability) ** 0.25

    def permits_ticket(self, minimum: float) -> bool:
        return not self.source_outage and self.value >= minimum
