from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
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
from execution_reconciler import ExecutionReconciler
from execution_state import ExecutionState
from tests.test_execution_engine import NOW, StaticContext, ticket_payload
from ticket_consumer import TicketConsumer


class AcceptedCancelButStillOpenGateway:
    def __init__(self, bybit_order_id: str):
        self.bybit_order_id = bybit_order_id
        self.cancel_calls = 0

    def cancel_order(self, symbol: str, bybit_order_id: str):
        self.cancel_calls += 1
        assert bybit_order_id == self.bybit_order_id
        # This is only a REST acknowledgement, even though a wrapper labels it
        # canceled.  The subsequent exchange query remains authoritative.
        return {"id": bybit_order_id, "status": "canceled", "info": {}}

    def find_order(self, symbol: str, order_link_id_value: str):
        return {
            "id": self.bybit_order_id,
            "status": "open",
            "info": {
                "orderId": self.bybit_order_id,
                "orderLinkId": order_link_id_value,
                "orderStatus": "New",
            },
        }

    def fetch_executions(self, symbol: str, order_link_id_value: str):
        return []


def test_entry_timeout_cancel_waits_for_exchange_terminal_state():
    with tempfile.TemporaryDirectory() as directory:
        store = DurableExecutionStore(Path(directory) / "execution.sqlite3")
        client = ExchangeGateway(mode="shadow")
        executor = BybitExecutor(client, store)
        consumer = TicketConsumer(
            consumer_id="consumer-a",
            store=store,
            context=StaticContext(),
            executor=executor,
        )
        ticket = OperationTicket.model_validate(
            ticket_payload("tk_timeout_cancel_confirm_001")
        )
        assert consumer.process(ticket, now=NOW) == ExecutionState.SUBMITTED
        entry_link = order_link_id(ticket.ticket_id)
        bybit_order_id = store.order(entry_link)["bybit_order_id"]
        gateway = AcceptedCancelButStillOpenGateway(bybit_order_id)

        state = ExecutionReconciler(store, gateway).reconcile_ticket(
            ticket.ticket_id,
            now=NOW + timedelta(seconds=ticket.entry.max_wait_sec + 1),
        )

        assert gateway.cancel_calls == 1
        assert state is not ExecutionState.CANCELLED
        assert store.order(entry_link)["order_status"] != "CANCELLED"
