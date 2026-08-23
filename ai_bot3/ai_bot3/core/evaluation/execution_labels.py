from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from contracts.common import deterministic_id
from contracts.execution_label_v1 import ExecutionAwareLabel
from contracts.execution_receipt_v1 import ExecutionReceipt
from contracts.operation_ticket_v1 import OperationTicket


@dataclass(frozen=True)
class PriceObservation:
    observed_at: datetime
    price: float


class ExecutionLabelBuilder:
    def build(
        self,
        ticket: OperationTicket,
        receipt: ExecutionReceipt,
        *,
        price_path: Iterable[PriceObservation] = (),
        funding_bps: float = 0.0,
    ) -> ExecutionAwareLabel:
        if receipt.ticket_id != ticket.ticket_id:
            raise ValueError("receipt and ticket ids do not match")
        order_roles = {order.order_link_id: order.role for order in receipt.orders}
        entry_orders = [order for order in receipt.orders if order.role == "entry"]
        if not entry_orders:
            raise ValueError("execution-aware label requires an entry order")
        requested = sum(order.quantity for order in entry_orders)
        filled = sum(order.cum_exec_qty for order in entry_orders)
        fraction = min(1.0, filled / requested) if requested > 0 else 0.0
        entry_fills = [
            fill for fill in receipt.fills if order_roles.get(fill.order_link_id, "entry") == "entry"
        ]
        entry_fills.sort(key=lambda item: item.executed_at)
        first_fill = entry_fills[0].executed_at if entry_fills else None
        full_fill = entry_fills[-1].executed_at if entry_fills and fraction >= 1 - 1e-9 else None
        average_entry = (
            sum(fill.quantity * fill.price for fill in entry_fills)
            / sum(fill.quantity for fill in entry_fills)
            if entry_fills
            else None
        )
        direction = 1.0 if ticket.intent.side == "BUY" else -1.0
        observations = sorted(price_path, key=lambda item: item.observed_at)
        if first_fill:
            observations = [item for item in observations if item.observed_at >= first_fill]
        returns = (
            [direction * (item.price / average_entry - 1.0) * 10_000 for item in observations]
            if average_entry
            else []
        )
        mfe = max(0.0, max(returns)) if returns else None
        mae = max(0.0, -min(returns)) if returns else None
        first_barrier = self._first_barrier(ticket, observations)
        entry_notional = sum(fill.quantity * fill.price for fill in entry_fills)
        fee_bps = receipt.total_exec_fee / entry_notional * 10_000 if entry_notional > 0 else 0.0
        slippage = 0.0
        if average_entry is not None and ticket.entry is not None:
            slippage = max(
                0.0,
                direction
                * (average_entry / ticket.entry.reference_price - 1.0)
                * 10_000,
            )
        exit_roles = [
            order_roles.get(fill.order_link_id, "")
            for fill in receipt.fills
            if order_roles.get(fill.order_link_id, "") != "entry"
        ]
        exit_reason = receipt.reason_code or (exit_roles[-1] if exit_roles else "OPEN_OR_UNFILLED")
        return ExecutionAwareLabel(
            label_id=deterministic_id(
                "xl", ticket.ticket_id, receipt.receipt_id, receipt.status
            ),
            source_receipt_id=receipt.receipt_id,
            ticket_id=ticket.ticket_id,
            portfolio_decision_id=ticket.portfolio_decision_id,
            strategy_release_id=ticket.strategy_release_id,
            symbol=ticket.instrument.symbol,
            side=ticket.intent.side,
            requested_quantity=requested,
            filled_quantity=filled,
            entry_fill_fraction=fraction,
            time_to_first_fill_sec=(
                max(0.0, (first_fill - ticket.created_at).total_seconds())
                if first_fill
                else None
            ),
            time_to_full_fill_sec=(
                max(0.0, (full_fill - ticket.created_at).total_seconds())
                if full_fill
                else None
            ),
            partial_fill=0 < fraction < 1,
            mfe_bps=mfe,
            mae_bps=mae,
            first_barrier=first_barrier,
            fee_bps=fee_bps,
            slippage_bps=slippage,
            funding_bps=float(funding_bps),
            realised_cost_bps=fee_bps + slippage + float(funding_bps),
            exit_reason=exit_reason,
            created_at=receipt.updated_at,
        )

    @staticmethod
    def _first_barrier(
        ticket: OperationTicket, observations: list[PriceObservation]
    ) -> str:
        if not observations:
            return "UNOBSERVED"
        if not ticket.protection:
            return "NONE"
        direction = 1 if ticket.intent.side == "BUY" else -1
        stop = ticket.protection.stop_loss.price if ticket.protection.stop_loss else None
        targets = [level.price for level in ticket.protection.take_profit]
        for observation in observations:
            if stop is not None and direction * (observation.price - stop) <= 0:
                return "STOP_LOSS"
            if any(direction * (observation.price - target) >= 0 for target in targets):
                return "TAKE_PROFIT"
        if observations[-1].observed_at >= ticket.created_at + timedelta(
            seconds=ticket.protection.max_holding_sec
        ):
            return "TIME"
        return "NONE"
