from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE))

from bybit_executor import BybitExecutor
from contracts.common import order_link_id
from contracts.operation_ticket_v1 import OperationTicket
from exchange_gateway import ExchangeGateway
from execution_reconciler import ExecutionReconciler
from execution_state import ExecutionState
from tests.test_execution_engine import NOW, StaticContext, ticket_payload
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


class CancelConfirmedGateway:
    def __init__(self, target_order_id: str):
        self.target_order_id = target_order_id

    def find_order(self, symbol: str, order_link_id_value: str):
        return {
            "id": self.target_order_id,
            "status": "canceled",
            "info": {
                "orderId": self.target_order_id,
                "orderLinkId": order_link_id_value,
                "orderStatus": "Cancelled",
            },
        }

    def fetch_executions(self, symbol: str, order_link_id_value: str):
        return []

    def cancel_order(self, symbol: str, bybit_order_id: str):
        raise AssertionError("a pending cancel must be reconciled, not submitted twice")


def _cancel_ticket(target_link_id: str) -> OperationTicket:
    payload = ticket_payload("tk_async_cancel_command_001")
    payload["intent"].update(
        action="CANCEL",
        side="BUY",
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


def test_non_shadow_cancel_rest_ack_is_not_final_confirmation():
    with tempfile.TemporaryDirectory() as directory:
        store = ExecutionStore(Path(directory) / "execution.sqlite3")
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
            ticket_payload("tk_async_cancel_target_001")
        )
        assert consumer.process(original, now=NOW) == ExecutionState.SUBMITTED
        target_link_id = order_link_id(original.ticket_id)
        target_order_id = store.order(target_link_id)["bybit_order_id"]

        # Keep the deterministic in-memory exchange, but exercise production
        # cancellation semantics: a REST acknowledgement is asynchronous.
        client.mode = "testnet"
        cancel = _cancel_ticket(target_link_id)
        assert consumer.process(cancel, now=NOW) == ExecutionState.SUBMITTING
        assert store.state(original.ticket_id) == ExecutionState.SUBMITTED
        assert store.order(target_link_id)["order_status"] == "CANCEL_REQUESTED"
        assert store.command(order_link_id(cancel.ticket_id, "cancel_1"))["status"] == "REST_ACCEPTED"

        state = ExecutionReconciler(
            store, CancelConfirmedGateway(target_order_id)
        ).reconcile_ticket(cancel.ticket_id, now=NOW)

        assert state == ExecutionState.CANCELLED
        assert store.state(original.ticket_id) == ExecutionState.CANCELLED
        assert store.order(target_link_id)["order_status"] == "CANCELLED"
        assert store.command(order_link_id(cancel.ticket_id, "cancel_1"))["status"] == "CONFIRMED"
