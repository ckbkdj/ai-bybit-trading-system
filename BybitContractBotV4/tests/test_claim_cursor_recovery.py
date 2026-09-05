from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(WORKSPACE))

from contracts.operation_ticket_v1 import OperationTicket
from execution_service import TicketConsumerService
from tests.test_execution_engine import ticket_payload
from ticket_client import TicketPage, TicketPageItem
from ticket_store import ExecutionStore


class _Executor:
    def set_lease_session(self, consumer_id, service_session_id):
        self.consumer_id = consumer_id
        self.service_session_id = service_session_id


class _Consumer:
    def __init__(self):
        self.executor = _Executor()
        self.processed: list[str] = []

    def process(self, ticket, **kwargs):
        self.processed.append(ticket.ticket_id)


class _Reporter:
    def build(self, ticket_id):
        return None


class _LeaseThenRecoveryClient:
    def __init__(self, ticket: OperationTicket):
        self.ticket = ticket
        self.claim_attempts = 0

    def fetch_page(self, cursor, consumer_id, limit):
        if cursor >= 7:
            return TicketPage([], cursor, {})
        return TicketPage([TicketPageItem(7, self.ticket)], 7, {})

    def claim(self, ticket_id, consumer_id, lease_token, lease_sec):
        self.claim_attempts += 1
        # First request represents a crashed prior instance whose ticket lease
        # remains active after consumer ownership moved to this new instance.
        if self.claim_attempts == 1:
            return None
        return 2

    def deliver_receipt(self, payload):
        raise AssertionError("no receipt should exist in this test")


def test_unresolved_claim_does_not_advance_cursor_and_ticket_recovers_once(tmp_path):
    ticket = OperationTicket.model_validate(
        ticket_payload("tk_crash_before_local_receive_001")
    )
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    consumer = _Consumer()
    client = _LeaseThenRecoveryClient(ticket)
    service = TicketConsumerService(
        consumer_id="executor-a",
        client=client,
        consumer=consumer,
        reporter=_Reporter(),
        store=store,
        service_session_id="replacement-instance",
    )

    blocked = service.run_once()
    assert blocked.claim_conflicts == 1
    assert blocked.processed == 0
    assert blocked.cursor_advanced_to == 0
    assert store.consumer_cursor("executor-a") == 0
    assert consumer.processed == []

    recovered = service.run_once()
    assert recovered.claim_conflicts == 0
    assert recovered.processed == 1
    assert recovered.cursor_advanced_to == 7
    assert store.consumer_cursor("executor-a") == 7
    assert consumer.processed == [ticket.ticket_id]

    empty = service.run_once()
    assert empty.processed == 0
    assert consumer.processed == [ticket.ticket_id]
