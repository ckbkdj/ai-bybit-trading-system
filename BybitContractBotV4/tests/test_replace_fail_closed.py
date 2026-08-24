from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE))

from contracts.operation_ticket_v1 import OperationTicket
from tests.test_execution_engine import NOW, ticket_payload
from ticket_validator import TicketValidator


def test_replace_is_rejected_until_atomic_amend_or_cancel_replace_exists():
    payload = ticket_payload("tk_replace_fail_closed_001")
    payload["supersedes_ticket_id"] = "tk_replace_target_001"
    payload["intent"].update(
        action="REPLACE",
        position_effect="REPLACE_ONLY",
        target_order_link_id="qt_replace_target_001",
    )
    ticket = OperationTicket.model_validate(payload)

    result = TicketValidator().validate(ticket, now=NOW)

    assert result.accepted is False
    assert result.reason_code == "REPLACE_NOT_IMPLEMENTED"
    assert "atomic" in result.reason_detail.lower()


def test_increase_is_rejected_until_averaging_down_can_be_ruled_out():
    payload = ticket_payload("tk_increase_fail_closed_001")
    payload["intent"].update(
        action="INCREASE",
        position_effect="OPEN_OR_INCREASE",
    )
    payload["guards"]["require_flat_position"] = False
    ticket = OperationTicket.model_validate(payload)

    result = TicketValidator().validate(ticket, now=NOW)

    assert result.accepted is False
    assert result.reason_code == "INCREASE_NOT_IMPLEMENTED"
    assert "averaging down" in result.reason_detail.lower()
