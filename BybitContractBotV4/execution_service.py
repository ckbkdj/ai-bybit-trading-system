from __future__ import annotations

from dataclasses import dataclass

from execution_reporter import ExecutionReporter
from ticket_client import TicketHttpClient, deterministic_lease_token
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


@dataclass(frozen=True)
class PollResult:
    fetched: int
    processed: int
    claim_conflicts: int
    receipts_delivered: int


class TicketConsumerService:
    def __init__(
        self,
        *,
        consumer_id: str,
        client: TicketHttpClient,
        consumer: TicketConsumer,
        reporter: ExecutionReporter,
        store: ExecutionStore,
    ):
        self.consumer_id = consumer_id
        self.client = client
        self.consumer = consumer
        self.reporter = reporter
        self.store = store

    def flush_receipts(self) -> int:
        # State may advance asynchronously through WebSocket/reconciliation after the
        # delivery cursor moved. Rebuilding is safe because receipt ids include the
        # immutable state/event sequence and the outbox deduplicates them.
        for ticket_id in self.store.all_ticket_ids():
            self.reporter.build(ticket_id)
        delivered = 0
        for item in self.store.pending_receipts():
            if self.client.post_receipt(item["payload"]):
                self.store.mark_receipt_delivered(item["receipt_id"])
                delivered += 1
        return delivered

    def run_once(self, limit: int = 100) -> PollResult:
        delivered = self.flush_receipts()
        cursor = self.store.consumer_cursor(self.consumer_id)
        items = self.client.fetch(cursor, self.consumer_id, limit)
        processed = 0
        conflicts = 0
        for item in items:
            lease = deterministic_lease_token(self.consumer_id, item.ticket.ticket_id)
            if not self.client.claim(item.ticket.ticket_id, self.consumer_id, lease, 60):
                conflicts += 1
                self.store.advance_consumer_cursor(self.consumer_id, item.cursor)
                continue
            self.consumer.process(item.ticket)
            receipt = self.reporter.build(item.ticket.ticket_id)
            try:
                if self.client.post_receipt(receipt):
                    self.store.mark_receipt_delivered(receipt.receipt_id)
                    delivered += 1
            finally:
                # Local durable state is authoritative; remote delivery remains in receipt_outbox on failure.
                self.store.advance_consumer_cursor(self.consumer_id, item.cursor)
            processed += 1
        return PollResult(len(items), processed, conflicts, delivered)
