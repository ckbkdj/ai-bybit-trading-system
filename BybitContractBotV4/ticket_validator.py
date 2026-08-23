from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from contracts.operation_ticket_v1 import OperationTicket


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    reason_code: str
    reason_detail: str


class TicketValidator:
    def __init__(self, approved_strategy_release_id: str = ""):
        self.approved_strategy_release_id = approved_strategy_release_id.strip()

    def validate(self, ticket: OperationTicket, *, now: datetime | None = None) -> ValidationResult:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if ticket.schema_version != "operation-ticket.v1":
            return ValidationResult(False, "UNSUPPORTED_SCHEMA", ticket.schema_version)
        if current < ticket.valid_from:
            return ValidationResult(False, "NOT_YET_VALID", "valid_from is in the future")
        if current >= ticket.expires_at:
            return ValidationResult(False, "TICKET_EXPIRED", "expires_at has passed")
        if ticket.instrument.exchange != "bybit" or ticket.instrument.category != "linear":
            return ValidationResult(False, "UNSUPPORTED_INSTRUMENT", "only Bybit linear instruments are supported")
        if (
            self.approved_strategy_release_id
            and ticket.strategy_release_id != self.approved_strategy_release_id
        ):
            return ValidationResult(
                False,
                "UNAPPROVED_STRATEGY_RELEASE",
                "ticket strategy_release_id is not the configured approved release",
            )
        if ticket.intent.action in {"OPEN", "INCREASE", "REPLACE"}:
            if ticket.entry is None or ticket.protection is None or ticket.protection.stop_loss is None:
                return ValidationResult(False, "MISSING_RISK_FIELDS", "entry and stop loss are required")
        return ValidationResult(True, "VALID", "ticket contract and validity checks passed")
