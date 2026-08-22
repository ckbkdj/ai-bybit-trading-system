from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OnchainFlow:
    inflow: float
    outflow: float
    unit: str
    source_reliability: float
    label_revision_risk: float
    confirmation_count: int

    @property
    def netflow(self) -> float:
        # Positive means more funds moved into the labelled exchange set; it does not prove a sale.
        return self.inflow - self.outflow

    def warnings(self) -> tuple[str, ...]:
        warnings = ["exchange_netflow_is_potential_pressure_not_confirmed_trade"]
        if self.label_revision_risk > 0.3:
            warnings.append("address_label_revision_risk")
        if self.confirmation_count < 2:
            warnings.append("low_confirmation_count")
        return tuple(warnings)
