from __future__ import annotations

from pathlib import Path

from execution_service import TicketConsumerService
from contracts.operation_ticket_v1 import OperationTicket
from ticket_client import (
    ReceiptDeliveryResult,
    TicketHttpClient,
    TicketPage,
)
from ticket_store import ExecutionStore


class _Executor:
    def set_lease_session(self, consumer_id, service_session_id):
        self.consumer_id = consumer_id
        self.service_session_id = service_session_id


class _Consumer:
    def __init__(self):
        self.executor = _Executor()


class _Reporter:
    def build(self, ticket_id):
        return None


class _PoisonThenSuccessClient:
    def __init__(self):
        self.deliveries = 0
        self.fetches = 0

    def deliver_receipt(self, payload):
        self.deliveries += 1
        if self.deliveries == 1:
            return ReceiptDeliveryResult("dead_letter", 422, "HTTP_422")
        return ReceiptDeliveryResult("delivered", 200, "DELIVERED")

    def fetch_page(self, cursor, consumer_id, limit):
        self.fetches += 1
        return TicketPage([], 25, {"expired_skipped": 10_000})


def _ticket(ticket_id: str, symbol: str) -> OperationTicket:
    from BybitContractBotV4.tests.test_execution_engine import ticket_payload

    payload = ticket_payload(ticket_id)
    payload["instrument"]["symbol"] = symbol
    return OperationTicket.model_validate(payload)


def _seed_receipts(store: ExecutionStore) -> None:
    for index, symbol in enumerate(("BTCUSDT", "ETHUSDT"), 1):
        ticket = _ticket(f"tk_receipt_poison_{index:03d}", symbol)
        store.receive(ticket)
        store.enqueue_receipt(
            f"rc_receipt_poison_{index:03d}",
            ticket.ticket_id,
            {
                "receipt_id": f"rc_receipt_poison_{index:03d}",
                "ticket_id": ticket.ticket_id,
            },
        )


def test_poison_receipt_dead_letters_without_blocking_new_receipts_or_fetch(
    tmp_path: Path,
):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    _seed_receipts(store)
    client = _PoisonThenSuccessClient()
    service = TicketConsumerService(
        consumer_id="executor-a",
        client=client,
        consumer=_Consumer(),
        reporter=_Reporter(),
        store=store,
        service_session_id="session-poison-test",
    )
    result = service.run_once()
    assert result.receipts_delivered == 1
    assert result.cursor_advanced_to == 25
    assert client.fetches == 1
    dead = store.receipt_dead_letters()
    assert len(dead) == 1
    assert dead[0]["last_error_code"] == "HTTP_422"
    assert store.operational_counts()["receipt_outbox_backlog"] == 0


class _Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class _Session:
    def __init__(self, response):
        self.response = response

    def post(self, *args, **kwargs):
        return self.response


def test_receipt_http_status_classification():
    payload = {"receipt_id": "rc_classification_001"}
    cases = (
        (429, {}, "retry"),
        (503, {}, "retry"),
        (401, {}, "security"),
        (403, {}, "security"),
        (404, {}, "dead_letter"),
        (422, {}, "dead_letter"),
        (409, {"idempotent": True}, "delivered"),
        (409, {"detail": "different content"}, "conflict"),
    )
    for status, response_payload, expected in cases:
        client = TicketHttpClient(
            "https://control.internal", session=_Session(_Response(status, response_payload))
        )
        assert client.deliver_receipt(payload).outcome == expected
