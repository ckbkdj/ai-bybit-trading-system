from __future__ import annotations

import sys
import tempfile
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE))

from bybit_executor import BybitExecutor
from contracts.common import order_link_id
from contracts.operation_ticket_v1 import OperationTicket
from durable_execution_store import DurableExecutionStore
from exchange_gateway import ExchangeGateway
from execution_state import ExecutionState
from tests.test_execution_engine import NOW, StaticContext, ticket_payload
from ticket_consumer import TicketConsumer


def _cancel_ticket(target_link_id: str) -> OperationTicket:
    payload = ticket_payload("tk_child_cancel_command_001")
    payload["intent"].update(
        action="CANCEL",
        side="SELL",
        position_effect="CANCEL_ONLY",
        target_exposure_pct=0,
        risk_budget_pct=0,
        target_order_link_id=target_link_id,
    )
    payload["entry"] = None
    payload["protection"] = None
    payload["economics"] = {
        "expected_return_bps": 0,
        "estimated_fee_bps": 0,
        "estimated_slippage_bps": 0,
        "estimated_funding_bps": 0,
        "model_error_buffer_bps": 0,
        "expected_return_after_cost_bps": 0,
    }
    return OperationTicket.model_validate(payload)


def test_cancelling_take_profit_child_does_not_cancel_filled_entry_ticket():
    with tempfile.TemporaryDirectory() as directory:
        store = DurableExecutionStore(Path(directory) / "execution.sqlite3")
        client = ExchangeGateway(mode="shadow")
        context = StaticContext()
        executor = BybitExecutor(client, store)
        consumer = TicketConsumer(
            consumer_id="consumer-a",
            store=store,
            context=context,
            executor=executor,
        )

        original = OperationTicket.model_validate(
            ticket_payload("tk_child_cancel_parent_001")
        )
        assert consumer.process(original, now=NOW) == ExecutionState.SUBMITTED
        entry_link_id = order_link_id(original.ticket_id)
        entry = store.order(entry_link_id)
        store.record_fill(
            exec_id="exec-child-cancel-parent-001",
            order_link_id=entry_link_id,
            quantity=float(entry["quantity"]),
            price=float(original.entry.reference_price),
            fee=0.01,
            executed_at=NOW,
            bybit_order_id=entry["bybit_order_id"],
        )
        assert store.state(original.ticket_id) == ExecutionState.FILLED

        tp = executor.submit_take_profits(
            original,
            context.instrument_rules(original),
            position_quantity=Decimal(str(entry["quantity"])),
        )[0]
        assert store.order(tp.order_link_id)["role"] == "take_profit_1"

        cancel = _cancel_ticket(tp.order_link_id)
        assert consumer.process(cancel, now=NOW) == ExecutionState.CANCELLED

        assert store.state(cancel.ticket_id) == ExecutionState.CANCELLED
        assert store.order(tp.order_link_id)["order_status"] == "CANCELLED"
        assert store.state(original.ticket_id) == ExecutionState.FILLED
        assert store.order(entry_link_id)["order_status"] == "FILLED"
