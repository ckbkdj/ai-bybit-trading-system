from __future__ import annotations

from pathlib import Path

from contracts.operation_ticket_v1 import OperationTicket
from execution_service import TicketConsumerService
from ticket_client import TicketPage, TicketPageItem
from ticket_store import ExecutionStore
from tests.test_execution_engine import ticket_payload


class _Executor:
    def set_lease_session(self, consumer_id: str, session_id: str) -> None:
        self.consumer_id = consumer_id
        self.session_id = session_id


class _Consumer:
    def __init__(self) -> None:
        self.executor = _Executor()

    def process(self, *args, **kwargs) -> None:  # pragma: no cover - must not run
        raise AssertionError("an actively leased ticket must not be processed")


class _Reporter:
    def build(self, ticket_id: str) -> None:  # pragma: no cover - empty store
        raise AssertionError(f"unexpected receipt build for {ticket_id}")


class _ActiveLeaseClient:
    def __init__(self, ticket: OperationTicket) -> None:
        self.ticket = ticket
        self.claim_calls = 0

    def fetch_page(self, after_cursor: int, consumer_id: str, limit: int) -> TicketPage:
        assert after_cursor == 0
        assert consumer_id == "executor-paper-01"
        return TicketPage(
            [TicketPageItem(1, self.ticket)],
            next_cursor=1,
            backlog={},
        )

    def claim(
        self,
        ticket_id: str,
        consumer_id: str,
        lease_token: str,
        lease_sec: int,
    ) -> None:
        self.claim_calls += 1
        return None


def test_active_claim_conflict_does_not_advance_cursor(tmp_path: Path):
    ticket = OperationTicket.model_validate(
        ticket_payload("tk_active_lease_recovery_001")
    )
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    client = _ActiveLeaseClient(ticket)
    service = TicketConsumerService(
        consumer_id="executor-paper-01",
        client=client,
        consumer=_Consumer(),
        reporter=_Reporter(),
        store=store,
        service_session_id="session-after-crash-001",
    )

    result = service.run_once()

    assert client.claim_calls == 1
    assert result.fetched == 1
    assert result.processed == 0
    assert result.claim_conflicts == 1
    assert result.cursor_advanced_to == 0
    assert store.consumer_cursor("executor-paper-01") == 0
