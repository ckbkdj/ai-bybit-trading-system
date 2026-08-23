from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional
import uuid

from contracts.common import order_link_id
from contracts.operation_ticket_v1 import OperationTicket
from execution_state import ExecutionState
from rate_limiter import EndpointRateLimiter
from sizing import ExecutionPlan, InstrumentRules, ceil_to_step, decimal, floor_to_step
from ticket_store import ExecutionStore
from ticket_client import deterministic_lease_token


class AmbiguousSubmission(RuntimeError):
    """The request outcome is unknown and must be reconciled by orderLinkId."""


@dataclass(frozen=True)
class SubmissionResult:
    order_link_id: str
    bybit_order_id: Optional[str]
    raw: dict[str, Any]
    newly_submitted: bool


class BybitExecutor:
    def __init__(
        self,
        client,
        store: ExecutionStore,
        *,
        uid: str = "default",
        rate_limiter: EndpointRateLimiter | None = None,
    ):
        self.client = client
        self.store = store
        self.uid = uid
        self.service_session_id = f"session_{uuid.uuid4().hex}"
        self.rate_limiter = rate_limiter or EndpointRateLimiter()

    def set_lease_session(self, uid: str, service_session_id: str) -> None:
        self.uid = uid
        self.service_session_id = service_session_id

    def operation_lease_token(self, ticket_id: str) -> str:
        return deterministic_lease_token(self.uid, ticket_id, self.service_session_id)

    def _operation_fence(self, ticket_id: str) -> tuple[str, int]:
        epoch = self.store.claim_operation(
            ticket_id,
            self.uid,
            self.operation_lease_token(ticket_id),
            lease_sec=60,
        )
        if epoch is None:
            raise RuntimeError("operation fencing lease is owned by another worker")
        return self.uid, epoch

    @staticmethod
    def _extract_order_id(response: dict[str, Any]) -> Optional[str]:
        if response.get("id"):
            return str(response["id"])
        info = response.get("info") or {}
        result = response.get("result") or info.get("result") or {}
        return str(
            info.get("orderId") or result.get("orderId") or response.get("orderId") or ""
        ) or None

    def _update_rate_headers(self, endpoint: str) -> None:
        getter = getattr(self.client, "response_headers", None)
        if getter:
            self.rate_limiter.update_from_headers(self.uid, endpoint, getter())

    def submit_entry(
        self,
        ticket: OperationTicket,
        plan: ExecutionPlan,
        *,
        claim_owner: str,
        claim_epoch: int,
    ) -> SubmissionResult:
        link_id = order_link_id(ticket.ticket_id, "entry")
        reserved = self.store.reserve_order(
            ticket.ticket_id,
            link_id,
            role="entry",
            side=plan.side,
            order_type=plan.order_type,
            quantity=float(plan.quantity),
            price=None if plan.price is None else float(plan.price),
            claim_owner=claim_owner,
            claim_epoch=claim_epoch,
        )
        if not reserved:
            existing = self.store.order(link_id) or {}
            return SubmissionResult(
                link_id,
                existing.get("bybit_order_id"),
                {"idempotent_replay": True},
                False,
            )
        command_roles = []
        if ticket.protection and ticket.protection.stop_loss:
            command_roles.append("stop_loss_1")
        if ticket.intent.action == "CLOSE":
            command_roles.append("close_1")
        if ticket.intent.action == "REPLACE":
            command_roles.append("revision_1")
        for role in command_roles:
            self.store.reserve_command(
                ticket.ticket_id,
                order_link_id(ticket.ticket_id, role),
                role=role,
                claim_owner=claim_owner,
                claim_epoch=claim_epoch,
                target_order_link_id=ticket.intent.target_order_link_id,
            )
        self.rate_limiter.acquire(self.uid, "POST:/v5/order/create")
        try:
            response = self.client.create_ticket_order(
                symbol=plan.symbol,
                side=plan.side,
                order_type=plan.order_type,
                amount=plan.quantity,
                price=plan.price,
                leverage=plan.leverage,
                order_link_id=link_id,
                reduce_only=ticket.intent.position_effect in {"REDUCE_ONLY", "CLOSE_ONLY"},
                stop_loss_price=plan.stop_loss_price,
                stop_trigger_by=(
                    {
                        "MARK_PRICE": "MarkPrice",
                        "LAST_PRICE": "LastPrice",
                        "INDEX_PRICE": "IndexPrice",
                    }[ticket.protection.stop_loss.type]
                    if ticket.protection and ticket.protection.stop_loss else "MarkPrice"
                ),
                time_in_force=ticket.entry.time_in_force if ticket.entry else "GTC",
                post_only=bool(ticket.entry and ticket.entry.post_only),
            )
        except Exception as exc:
            for role in command_roles:
                self.store.confirm_command(
                    order_link_id(ticket.ticket_id, role),
                    "UNKNOWN",
                    {"error_type": type(exc).__name__},
                )
            # Do not submit again. Reconciler must query using the deterministic link id.
            raise AmbiguousSubmission(f"submission outcome must be reconciled for {link_id}: {exc}") from exc
        self._update_rate_headers("POST:/v5/order/create")
        if not isinstance(response, dict):
            response = {"raw": repr(response)}
        bybit_order_id = self._extract_order_id(response)
        self.store.record_rest_submission(link_id, bybit_order_id, response)
        for role in command_roles:
            self.store.confirm_command(
                order_link_id(ticket.ticket_id, role), "REST_ACCEPTED", response
            )
        return SubmissionResult(link_id, bybit_order_id, response, True)

    def cancel_target(
        self,
        ticket: OperationTicket,
        *,
        claim_owner: str,
        claim_epoch: int,
    ) -> SubmissionResult:
        """Cancel once; an unknown outcome is left for reconciliation, never retried blindly."""
        target_link_id = ticket.intent.target_order_link_id
        if not target_link_id:
            raise ValueError("cancel ticket has no target_order_link_id")
        target = self.store.order(target_link_id)
        if not target:
            raise ValueError(f"target order is not known locally: {target_link_id}")
        command_id = order_link_id(ticket.ticket_id, "cancel_1")
        newly_reserved = self.store.reserve_command(
            ticket.ticket_id,
            command_id,
            role="cancel_1",
            target_order_link_id=target_link_id,
            claim_owner=claim_owner,
            claim_epoch=claim_epoch,
        )
        self.store.transition(
            ticket.ticket_id,
            ExecutionState.SUBMITTING,
            "cancel_submitting",
            {"target_order_link_id": target_link_id, "command_id": command_id},
            fence_owner=claim_owner,
            fence_epoch=claim_epoch,
        )
        if target.get("order_status") == "CANCELLED":
            self.store.confirm_command(command_id, "CONFIRMED", {"idempotent_replay": True})
            self.store.confirm_cancellation(ticket.ticket_id, target_link_id, {"idempotent_replay": True})
            return SubmissionResult(command_id, target.get("bybit_order_id"), {}, False)
        if not newly_reserved:
            return SubmissionResult(
                command_id,
                target.get("bybit_order_id"),
                {"ambiguous_command_replay": True},
                False,
            )
        bybit_order_id = target.get("bybit_order_id")
        if not bybit_order_id:
            raise ValueError("target order has no confirmed exchange order id")
        self.rate_limiter.acquire(self.uid, "POST:/v5/order/cancel")
        try:
            response = self.client.cancel_order(bybit_order_id, ticket.instrument.symbol)
        except Exception as exc:
            self.store.confirm_command(
                command_id, "UNKNOWN", {"error_type": type(exc).__name__}
            )
            raise AmbiguousSubmission(
                f"cancellation outcome must be reconciled for {target_link_id}: {exc}"
            ) from exc
        self._update_rate_headers("POST:/v5/order/cancel")
        if not isinstance(response, dict):
            response = {"raw": repr(response)}
        self.store.confirm_cancellation(ticket.ticket_id, target_link_id, response)
        self.store.confirm_command(command_id, "CONFIRMED", response)
        return SubmissionResult(command_id, bybit_order_id, response, True)

    def submit_take_profits(
        self,
        ticket: OperationTicket,
        rules: InstrumentRules,
        *,
        position_quantity: Decimal | None = None,
    ) -> tuple[SubmissionResult, ...]:
        """Install deterministic reduce-only TP children after the entry is fully filled."""
        if not ticket.protection or not ticket.protection.take_profit:
            return ()
        entry = self.store.order(order_link_id(ticket.ticket_id, "entry"))
        if not entry or decimal(entry.get("cum_exec_qty") or 0) <= 0:
            raise ValueError("take-profit orders require a filled entry quantity")
        total_quantity = (
            decimal(position_quantity)
            if position_quantity is not None
            else decimal(entry["cum_exec_qty"])
        )
        if total_quantity <= 0:
            raise ValueError("take-profit orders require a live position quantity")
        exit_side = "SELL" if ticket.intent.side == "BUY" else "BUY"
        results = []
        claim_owner, claim_epoch = self._operation_fence(ticket.ticket_id)
        for index, level in enumerate(ticket.protection.take_profit, start=1):
            role = f"take_profit_{index}"
            link_id = order_link_id(ticket.ticket_id, role)
            quantity = floor_to_step(total_quantity * decimal(level.close_fraction), rules.qty_step)
            if quantity < rules.min_qty:
                raise ValueError(f"{role} quantity is below exchange minimum")
            raw_price = decimal(level.price)
            price = ceil_to_step(raw_price, rules.tick_size) if exit_side == "SELL" else floor_to_step(raw_price, rules.tick_size)
            if quantity * price < rules.min_notional_usdt:
                raise ValueError(f"{role} notional is below exchange minimum")
            reserved = self.store.reserve_exit_order(
                ticket.ticket_id,
                link_id,
                role=role,
                side=exit_side,
                order_type="LIMIT",
                quantity=float(quantity),
                price=float(price),
                claim_owner=claim_owner,
                claim_epoch=claim_epoch,
            )
            if not reserved:
                existing = self.store.order(link_id) or {}
                results.append(
                    SubmissionResult(link_id, existing.get("bybit_order_id"), {"idempotent_replay": True}, False)
                )
                continue
            self.rate_limiter.acquire(self.uid, "POST:/v5/order/create")
            try:
                response = self.client.create_ticket_order(
                    symbol=ticket.instrument.symbol,
                    side=exit_side,
                    order_type="LIMIT",
                    amount=quantity,
                    price=price,
                    leverage=Decimal("1"),
                    order_link_id=link_id,
                    reduce_only=True,
                )
            except Exception as exc:
                raise AmbiguousSubmission(
                    f"take-profit outcome must be reconciled for {link_id}: {exc}"
                ) from exc
            self._update_rate_headers("POST:/v5/order/create")
            if not isinstance(response, dict):
                response = {"raw": repr(response)}
            bybit_order_id = self._extract_order_id(response)
            self.store.record_exit_submission(link_id, bybit_order_id, response)
            results.append(SubmissionResult(link_id, bybit_order_id, response, True))
        return tuple(results)

    def submit_time_exit(
        self,
        ticket: OperationTicket,
        *,
        position_quantity: Decimal,
    ) -> SubmissionResult:
        """Close an overdue position once with a deterministic reduce-only market order."""

        quantity = decimal(position_quantity)
        if quantity <= 0:
            raise ValueError("time exit requires a positive live position quantity")
        role = "time_exit_1"
        link_id = order_link_id(ticket.ticket_id, role)
        exit_side = "SELL" if ticket.intent.side == "BUY" else "BUY"
        claim_owner, claim_epoch = self._operation_fence(ticket.ticket_id)
        reserved = self.store.reserve_exit_order(
            ticket.ticket_id,
            link_id,
            role=role,
            side=exit_side,
            order_type="MARKET",
            quantity=float(quantity),
            price=None,
            claim_owner=claim_owner,
            claim_epoch=claim_epoch,
        )
        if not reserved:
            existing = self.store.order(link_id) or {}
            return SubmissionResult(
                link_id,
                existing.get("bybit_order_id"),
                {"idempotent_replay": True},
                False,
            )
        self.rate_limiter.acquire(self.uid, "POST:/v5/order/create")
        try:
            response = self.client.create_ticket_order(
                symbol=ticket.instrument.symbol,
                side=exit_side,
                order_type="MARKET",
                amount=quantity,
                price=None,
                leverage=Decimal("1"),
                order_link_id=link_id,
                reduce_only=True,
                time_in_force="IOC",
            )
        except Exception as exc:
            raise AmbiguousSubmission(
                f"time-exit outcome must be reconciled for {link_id}: {exc}"
            ) from exc
        self._update_rate_headers("POST:/v5/order/create")
        if not isinstance(response, dict):
            response = {"raw": repr(response)}
        bybit_order_id = self._extract_order_id(response)
        self.store.record_exit_submission(link_id, bybit_order_id, response)
        return SubmissionResult(link_id, bybit_order_id, response, True)
