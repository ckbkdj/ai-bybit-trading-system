from __future__ import annotations

from dataclasses import dataclass
import uuid

from execution_reporter import ExecutionReporter
from incident_modes import IncidentMode
from ticket_client import TicketHttpClient, deterministic_lease_token
from ticket_consumer import TicketConsumer
from ticket_store import ExecutionStore


@dataclass(frozen=True)
class PollResult:
    fetched: int
    processed: int
    claim_conflicts: int
    receipts_delivered: int
    cursor_advanced_to: int = 0


class TicketConsumerService:
    def __init__(
        self,
        *,
        consumer_id: str,
        client: TicketHttpClient,
        consumer: TicketConsumer,
        reporter: ExecutionReporter,
        store: ExecutionStore,
        service_session_id: str | None = None,
    ):
        self.consumer_id = consumer_id
        self.client = client
        self.consumer = consumer
        self.reporter = reporter
        self.store = store
        self.service_session_id = service_session_id or f"session_{uuid.uuid4().hex}"
        self.consumer.executor.set_lease_session(
            consumer_id, self.service_session_id
        )

    def flush_receipts(self) -> int:
        # State may advance asynchronously through WebSocket/reconciliation after the
        # delivery cursor moved. Rebuilding is safe because receipt ids include the
        # immutable state/event sequence and the outbox deduplicates them.
        for ticket_id in self.store.all_ticket_ids():
            self.reporter.build(ticket_id)
        delivered = 0
        for item in self.store.pending_receipts():
            receipt_id = item["receipt_id"]
            try:
                result = self.client.deliver_receipt(item["payload"])
            except Exception as exc:
                self.store.record_receipt_retry(
                    receipt_id,
                    http_status=None,
                    error_code="CLIENT_EXCEPTION",
                    error=f"{type(exc).__name__}: {exc}",
                )
                continue
            if result.delivered:
                self.store.mark_receipt_delivered(item["receipt_id"])
                delivered += 1
            elif result.outcome == "retry":
                self.store.record_receipt_retry(
                    receipt_id,
                    http_status=result.http_status,
                    error_code=result.error_code,
                    error=result.detail or result.error_code,
                )
            elif result.outcome == "security":
                self.store.record_receipt_retry(
                    receipt_id,
                    http_status=result.http_status,
                    error_code=result.error_code,
                    error=result.detail or "control-plane authentication failed",
                )
                self.store.set_incident_mode(
                    IncidentMode.FREEZE_NEW_RISK,
                    "control-plane receipt authentication failed",
                )
                self.store.set_kill_switch(True)
            else:
                self.store.dead_letter_receipt(
                    receipt_id,
                    http_status=result.http_status,
                    error_code=result.error_code,
                    error=result.detail or result.error_code,
                )
                if result.outcome == "conflict":
                    self.store.set_incident_mode(
                        IncidentMode.FREEZE_NEW_RISK,
                        "conflicting receipt entered dead letter",
                    )
                    self.store.set_kill_switch(True)
        return delivered

    def run_once(self, limit: int = 100) -> PollResult:
        delivered = self.flush_receipts()
        starting_cursor = self.store.consumer_cursor(self.consumer_id)
        page = self.client.fetch_page(starting_cursor, self.consumer_id, limit)
        items = page.items
        processed = 0
        conflicts = 0
        cursor_advanced_to = starting_cursor
        blocked_by_active_claim = False

        for item in items:
            lease = deterministic_lease_token(
                self.consumer_id, item.ticket.ticket_id, self.service_session_id
            )
            claim_epoch = self.client.claim(
                item.ticket.ticket_id, self.consumer_id, lease, 60
            )
            if claim_epoch is None:
                # A previous process may have claimed the ticket and crashed before
                # recording it locally.  Advancing the cursor here would lose that
                # work item permanently.  Stop at the first active lease and retry
                # from the same cursor after the lease expires or reconciliation
                # proves the ticket terminal.
                conflicts += 1
                blocked_by_active_claim = True
                break

            self.consumer.process(
                item.ticket,
                claim_epoch=claim_epoch,
                lease_token=lease,
            )
            self.reporter.build(item.ticket.ticket_id)
            try:
                delivered += self.flush_receipts()
            finally:
                # Local durable state is authoritative; remote delivery remains in
                # receipt_outbox on failure.  This ticket alone is now safe to pass.
                self.store.advance_consumer_cursor(self.consumer_id, item.cursor)
                cursor_advanced_to = item.cursor
            processed += 1

        if not blocked_by_active_claim:
            # The server may fast-forward over expired/superseded rows that were not
            # returned in ``items``.  Apply that fast-forward only when no unresolved
            # claim lies between the starting cursor and the page boundary.
            self.store.advance_consumer_cursor(self.consumer_id, page.next_cursor)
            cursor_advanced_to = page.next_cursor

        return PollResult(
            len(items),
            processed,
            conflicts,
            delivered,
            cursor_advanced_to,
        )
