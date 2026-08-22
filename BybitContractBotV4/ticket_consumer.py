from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Protocol

from bybit_executor import AmbiguousSubmission, BybitExecutor
from contracts.common import sortable_id
from contracts.operation_ticket_v1 import OperationTicket
from execution_state import ExecutionState, TERMINAL_STATES
from risk_guard import (
    AccountSnapshot,
    MarketSnapshot,
    PortfolioSnapshot,
    RiskGuard,
    SystemHealth,
)
from sizing import InstrumentRules, PositionSizer, SizingError
from ticket_store import ExecutionStore
from ticket_validator import TicketValidator


class ContextProvider(Protocol):
    def market(self, ticket: OperationTicket) -> MarketSnapshot: ...
    def account(self, ticket: OperationTicket) -> AccountSnapshot: ...
    def portfolio(self, ticket: OperationTicket) -> PortfolioSnapshot: ...
    def health(self, ticket: OperationTicket) -> SystemHealth: ...
    def instrument_rules(self, ticket: OperationTicket) -> InstrumentRules: ...


class TicketConsumer:
    def __init__(
        self,
        *,
        consumer_id: str,
        store: ExecutionStore,
        context: ContextProvider,
        executor: BybitExecutor,
        validator: TicketValidator | None = None,
        risk_guard: RiskGuard | None = None,
        sizer: PositionSizer | None = None,
    ):
        self.consumer_id = consumer_id
        self.store = store
        self.context = context
        self.executor = executor
        self.validator = validator or TicketValidator()
        self.risk_guard = risk_guard or RiskGuard()
        self.sizer = sizer or PositionSizer(self.risk_guard.limits)

    def process(self, ticket: OperationTicket, *, now: datetime | None = None) -> ExecutionState:
        current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self.store.receive(ticket)
        state = self.store.state(ticket.ticket_id)
        if state in TERMINAL_STATES or state in {
            ExecutionState.SUBMITTING,
            ExecutionState.SUBMITTED,
            ExecutionState.ACKNOWLEDGED,
            ExecutionState.PARTIALLY_FILLED,
        }:
            return state

        if state == ExecutionState.RECEIVED:
            validation = self.validator.validate(ticket, now=current_time)
            if not validation.accepted:
                target = ExecutionState.EXPIRED if validation.reason_code == "TICKET_EXPIRED" else ExecutionState.REJECTED
                self.store.transition(
                    ticket.ticket_id,
                    target,
                    "ticket_validation_failed",
                    reason_code=validation.reason_code,
                    reason_detail=validation.reason_detail,
                )
                return target
            self.store.transition(ticket.ticket_id, ExecutionState.VALIDATED, "ticket_validated")
            state = ExecutionState.VALIDATED

        if state in {ExecutionState.VALIDATED, ExecutionState.CLAIMED, ExecutionState.RISK_APPROVED}:
            claimed = self.store.claim(
                ticket.ticket_id,
                self.consumer_id,
                sortable_id("lease"),
                lease_sec=60,
            )
            if not claimed:
                return self.store.state(ticket.ticket_id)
            state = self.store.state(ticket.ticket_id)

        market = self.context.market(ticket)
        account = self.context.account(ticket)
        portfolio = self.context.portfolio(ticket)
        health = self.context.health(ticket)
        if self.store.kill_switch_enabled() and not health.kill_switch:
            health = replace(health, kill_switch=True)

        if state == ExecutionState.CLAIMED:
            decision = self.risk_guard.evaluate(
                ticket, market, account, portfolio, health, now=current_time
            )
            if not decision.approved:
                target = ExecutionState.EXPIRED if decision.reason_code == "TICKET_EXPIRED" else ExecutionState.RISK_BLOCKED
                self.store.transition(
                    ticket.ticket_id,
                    target,
                    "risk_rejected",
                    {"checks": list(decision.checks)},
                    reason_code=decision.reason_code,
                    reason_detail=decision.reason_detail,
                )
                return target
            self.store.transition(
                ticket.ticket_id,
                ExecutionState.RISK_APPROVED,
                "risk_approved",
                {"checks": list(decision.checks)},
            )
            state = ExecutionState.RISK_APPROVED

        if state == ExecutionState.RISK_APPROVED:
            if ticket.intent.action == "CANCEL":
                try:
                    self.executor.cancel_target(ticket)
                except AmbiguousSubmission:
                    return ExecutionState.SUBMITTING
                except ValueError as exc:
                    self.store.transition(
                        ticket.ticket_id,
                        ExecutionState.RISK_BLOCKED,
                        "cancellation_rejected",
                        reason_code="CANCELLATION_REJECTED",
                        reason_detail=str(exc),
                    )
                    return ExecutionState.RISK_BLOCKED
                return self.store.state(ticket.ticket_id)
            try:
                plan = self.sizer.calculate(
                    ticket, account, portfolio, self.context.instrument_rules(ticket)
                )
            except SizingError as exc:
                self.store.transition(
                    ticket.ticket_id,
                    ExecutionState.RISK_BLOCKED,
                    "sizing_rejected",
                    reason_code="SIZING_REJECTED",
                    reason_detail=str(exc),
                )
                return ExecutionState.RISK_BLOCKED
            try:
                self.executor.submit_entry(ticket, plan)
            except AmbiguousSubmission:
                # Leave SUBMITTING for deterministic reconciliation; never retry create blindly.
                return ExecutionState.SUBMITTING
        return self.store.state(ticket.ticket_id)
