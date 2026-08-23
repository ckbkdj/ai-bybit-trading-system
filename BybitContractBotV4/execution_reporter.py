from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from contracts.execution_receipt_v1 import ExecutionReceipt
from ticket_store import ExecutionStore, parse_time


class ExecutionReporter:
    def __init__(self, store: ExecutionStore, consumer_id: str, mode: str):
        self.store = store
        self.consumer_id = consumer_id
        self.mode = mode

    def build(self, ticket_id: str) -> ExecutionReceipt:
        ticket_record = self.store.ticket_record(ticket_id)
        if not ticket_record:
            raise KeyError(ticket_id)
        ticket = self.store.get_ticket(ticket_id)
        orders = self.store.orders_for_ticket(ticket_id)
        fills = self.store.fills_for_ticket(ticket_id)
        position = self.store.latest_position(ticket.instrument.symbol)
        events = self.store.ticket_events(ticket_id)
        event_sequence = events[-1]["sequence"] if events else 0
        digest = hashlib.sha256(f"{ticket_id}:{ticket_record['state']}:{event_sequence}".encode()).hexdigest()
        receipt_id = f"rc_{digest[:32]}"
        order_payloads = [
            {
                "order_link_id": order["order_link_id"],
                "role": order["role"],
                "bybit_order_id": order["bybit_order_id"],
                "order_status": order["order_status"],
                "side": order["side"].upper(),
                "order_type": order["order_type"].upper(),
                "quantity": order["quantity"],
                "price": order["price"],
                "cum_exec_qty": order["cum_exec_qty"],
                "avg_exec_price": order["avg_exec_price"],
            }
            for order in orders
        ]
        fill_payloads = [
            {
                "exec_id": fill["exec_id"],
                "order_link_id": fill["order_link_id"],
                "quantity": fill["exec_qty"],
                "price": fill["exec_price"],
                "exec_fee": fill["exec_fee"],
                "executed_at": parse_time(fill["executed_at"]),
            }
            for fill in fills
        ]
        receipt = ExecutionReceipt.model_validate(
            {
                "receipt_id": receipt_id,
                "ticket_id": ticket_id,
                "consumer_id": self.consumer_id,
                "mode": self.mode,
                "status": ticket_record["state"],
                "reason_code": ticket_record["reason_code"],
                "reason_detail": ticket_record["reason_detail"],
                "orders": order_payloads,
                "fills": fill_payloads,
                "position_version_after": position["version"],
                "position_qty_after": position["quantity"],
                "total_exec_fee": sum(float(fill["exec_fee"]) for fill in fills),
                "created_at": parse_time(ticket_record["created_at"]),
                "updated_at": parse_time(ticket_record["updated_at"]),
            }
        )
        self.store.enqueue_receipt(receipt.receipt_id, ticket_id, receipt.model_dump(mode="json"))
        return receipt
