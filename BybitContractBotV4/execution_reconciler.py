from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from execution_state import ExecutionState
from ticket_store import ExecutionStore, parse_time


class ReconciliationGateway(Protocol):
    def find_order(self, symbol: str, order_link_id: str) -> dict[str, Any] | None: ...

    def fetch_executions(self, symbol: str, order_link_id: str) -> list[dict[str, Any]]: ...

    def cancel_order(self, symbol: str, bybit_order_id: str) -> dict[str, Any]: ...


class BybitReconciliationGateway:
    def __init__(self, client):
        self.client = client

    def find_order(self, symbol: str, order_link_id: str) -> dict[str, Any] | None:
        return self.client.find_order_by_link_id(symbol, order_link_id)

    def fetch_executions(self, symbol: str, order_link_id: str) -> list[dict[str, Any]]:
        # A transport/authentication failure is not equivalent to "no fills".
        # Let the service fail closed and try again without changing ticket state.
        trades = self.client.exchange.fetch_my_trades(
            symbol, params={"orderLinkId": order_link_id, "category": "linear"}
        )
        records = []
        for trade in trades or []:
            info = trade.get("info") or {}
            records.append(
                {
                    "execId": info.get("execId") or trade.get("id"),
                    "orderLinkId": order_link_id,
                    "orderId": info.get("orderId") or trade.get("order"),
                    "execQty": info.get("execQty") or trade.get("amount"),
                    "execPrice": info.get("execPrice") or trade.get("price"),
                    "execFee": info.get("execFee") or (trade.get("fee") or {}).get("cost") or 0,
                    "execTime": info.get("execTime") or trade.get("timestamp"),
                }
            )
        return records

    def cancel_order(self, symbol: str, bybit_order_id: str) -> dict[str, Any]:
        return self.client.cancel_order(bybit_order_id, symbol)


class ExecutionReconciler:
    def __init__(self, store: ExecutionStore, gateway: ReconciliationGateway, grace_seconds: int = 30):
        self.store = store
        self.gateway = gateway
        self.grace_seconds = max(5, int(grace_seconds))

    def reconcile_ticket(self, ticket_id: str, *, now: datetime | None = None) -> ExecutionState:
        current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        ticket = self.store.get_ticket(ticket_id)
        record = self.store.ticket_record(ticket_id)
        if not ticket or not record:
            raise KeyError(ticket_id)
        state = ExecutionState(record["state"])
        if state == ExecutionState.SUBMITTING and ticket.intent.action == "CANCEL":
            target_link_id = ticket.intent.target_order_link_id
            target = self.store.order(target_link_id) if target_link_id else None
            if not target_link_id or not target:
                self.store.transition(
                    ticket_id,
                    ExecutionState.FAILED,
                    "cancel_reconciliation_target_missing",
                    reason_code="CANCEL_TARGET_NOT_FOUND",
                    reason_detail="cancel target is absent from the local execution journal",
                )
                return self.store.state(ticket_id)
            remote = self.gateway.find_order(ticket.instrument.symbol, target_link_id)
            remote_status = str(
                (remote or {}).get("status")
                or ((remote or {}).get("info") or {}).get("orderStatus")
                or (remote or {}).get("orderStatus")
                or ""
            ).strip().upper()
            if remote_status in {"CANCELED", "CANCELLED", "DEACTIVATED"}:
                self.store.confirm_cancellation(ticket_id, target_link_id, remote)
                return self.store.state(ticket_id)
            age = (current_time - parse_time(record["updated_at"])).total_seconds()
            if age > self.grace_seconds:
                self.store.transition(
                    ticket_id,
                    ExecutionState.FAILED,
                    "cancel_reconciliation_unconfirmed",
                    reason_code="AMBIGUOUS_CANCELLATION_UNCONFIRMED",
                    reason_detail="exchange did not confirm target cancellation after the grace period",
                )
            return self.store.state(ticket_id)
        for order in self.store.orders_for_ticket(ticket_id):
            local_status = str(order.get("order_status") or "").upper().replace("CANCELED", "CANCELLED")
            if local_status in {"FILLED", "CANCELLED", "REJECTED", "DEACTIVATED"}:
                continue
            if order["role"] == "entry" and ticket.entry is not None:
                wait_deadline = min(
                    ticket.expires_at,
                    parse_time(order["created_at"])
                    + timedelta(seconds=ticket.entry.max_wait_sec),
                )
                if current_time >= wait_deadline and order.get("bybit_order_id"):
                    try:
                        response = self.gateway.cancel_order(
                            ticket.instrument.symbol, str(order["bybit_order_id"])
                        )
                    except Exception as exc:
                        response = {"cancel_error": f"{type(exc).__name__}: {exc}"}
                    self.store.record_cancel_requested(order["order_link_id"], response)
                    response_status = str(
                        (response or {}).get("status")
                        or ((response or {}).get("info") or {}).get("orderStatus")
                        or (response or {}).get("orderStatus")
                        or ""
                    ).upper()
                    if response_status in {"CANCELED", "CANCELLED", "DEACTIVATED"}:
                        self.store.acknowledge_order(
                            order["order_link_id"], response_status, response
                        )
                        continue
            remote = self.gateway.find_order(ticket.instrument.symbol, order["order_link_id"])
            if remote:
                bybit_id = str(
                    remote.get("id")
                    or (remote.get("info") or {}).get("orderId")
                    or remote.get("orderId")
                    or ""
                ) or None
                if order["role"] == "entry" and state == ExecutionState.SUBMITTING:
                    self.store.record_rest_submission(order["order_link_id"], bybit_id, remote)
                elif order["role"] != "entry" and not order.get("bybit_order_id"):
                    self.store.record_exit_submission(order["order_link_id"], bybit_id, remote)
                remote_status = str(
                    remote.get("status")
                    or (remote.get("info") or {}).get("orderStatus")
                    or remote.get("orderStatus")
                    or "RECONCILED"
                )
                self.store.acknowledge_order(order["order_link_id"], remote_status, remote)
            for fill in self.gateway.fetch_executions(ticket.instrument.symbol, order["order_link_id"]):
                exec_time = fill.get("execTime")
                try:
                    executed_at = datetime.fromtimestamp(float(exec_time) / 1000, timezone.utc)
                except (TypeError, ValueError, OSError):
                    executed_at = current_time
                self.store.record_fill(
                    exec_id=str(fill["execId"]),
                    order_link_id=order["order_link_id"],
                    bybit_order_id=str(fill.get("orderId") or "") or None,
                    quantity=float(fill["execQty"]),
                    price=float(fill["execPrice"]),
                    fee=abs(float(fill.get("execFee") or 0)),
                    executed_at=executed_at,
                    raw=fill,
                )
        refreshed = self.store.state(ticket_id)
        if refreshed == ExecutionState.SUBMITTING:
            age = (current_time - parse_time(record["updated_at"])).total_seconds()
            if age > self.grace_seconds:
                # No blind resubmit; retain a terminal audit reason after both lookups were empty.
                self.store.transition(
                    ticket_id,
                    ExecutionState.FAILED,
                    "reconciliation_not_found",
                    reason_code="AMBIGUOUS_SUBMISSION_NOT_FOUND",
                    reason_detail="order was not found by deterministic orderLinkId after grace period",
                )
        return self.store.state(ticket_id)

    def recover_all(self, *, now: datetime | None = None) -> dict[str, ExecutionState]:
        return {
            ticket_id: self.reconcile_ticket(ticket_id, now=now)
            for ticket_id in self.store.reconciliation_ticket_ids()
        }
