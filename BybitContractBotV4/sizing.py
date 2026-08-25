from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Optional

from contracts.operation_ticket_v1 import OperationTicket
from risk_guard import AccountSnapshot, PortfolioSnapshot, RiskLimits


def decimal(value: object) -> Decimal:
    return Decimal(str(value))


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


@dataclass(frozen=True)
class InstrumentRules:
    symbol: str
    min_qty: Decimal
    qty_step: Decimal
    tick_size: Decimal
    min_notional_usdt: Decimal
    max_qty: Optional[Decimal] = None


@dataclass(frozen=True)
class ExecutionPlan:
    symbol: str
    side: str
    order_type: str
    quantity: Decimal
    price: Optional[Decimal]
    stop_loss_price: Optional[Decimal]
    leverage: Decimal
    risk_amount_usdt: Decimal
    notional_usdt: Decimal


class SizingError(ValueError):
    pass


class PositionSizer:
    def __init__(self, risk_limits: RiskLimits | None = None):
        self.risk_limits = risk_limits or RiskLimits()

    def calculate(
        self,
        ticket: OperationTicket,
        account: AccountSnapshot,
        portfolio: PortfolioSnapshot,
        rules: InstrumentRules,
    ) -> ExecutionPlan:
        if rules.symbol.strip().upper() != ticket.instrument.symbol:
            raise SizingError("instrument rules symbol mismatch")
        if ticket.entry is None:
            raise SizingError("ticket has no entry sizing inputs")
        if ticket.intent.action in {"REDUCE", "CLOSE"}:
            reference = decimal(ticket.entry.limit_price or ticket.entry.reference_price)
            current_quantity = abs(decimal(portfolio.current_position_qty))
            if current_quantity <= 0:
                raise SizingError("risk-reducing ticket has no current position")
            fraction = Decimal("1") if ticket.intent.action == "CLOSE" else decimal(ticket.intent.reduce_fraction)
            quantity = floor_to_step(current_quantity * fraction, rules.qty_step)
            if quantity < rules.min_qty:
                raise SizingError("reduction quantity is below exchange minimum")
            if rules.max_qty is not None:
                quantity = min(quantity, rules.max_qty)
            price = None
            if ticket.entry.order_type == "LIMIT":
                price = (
                    floor_to_step(reference, rules.tick_size)
                    if ticket.intent.side == "BUY"
                    else ceil_to_step(reference, rules.tick_size)
                )
            notional = quantity * (price or reference)
            if notional < rules.min_notional_usdt:
                raise SizingError("reduction notional is below exchange minimum")
            return ExecutionPlan(
                symbol=ticket.instrument.symbol,
                side=ticket.intent.side,
                order_type=ticket.entry.order_type,
                quantity=quantity,
                price=price,
                stop_loss_price=None,
                leverage=Decimal("1"),
                risk_amount_usdt=Decimal("0"),
                notional_usdt=notional,
            )
        if ticket.protection is None or ticket.protection.stop_loss is None:
            raise SizingError("ticket has no entry/stop-loss sizing inputs")
        reference = decimal(ticket.entry.limit_price or ticket.entry.reference_price)
        stop = decimal(ticket.protection.stop_loss.price)
        stop_distance = abs(reference - stop)
        if stop_distance <= 0:
            raise SizingError("stop-loss distance must be positive")
        equity = decimal(account.equity_usdt)
        free_margin = decimal(account.free_margin_usdt)
        if equity <= 0 or free_margin < 0:
            raise SizingError("invalid account balance")

        leverage = min(decimal(ticket.intent.leverage_cap), decimal(self.risk_limits.max_gross_leverage))
        risk_amount = equity * min(
            decimal(ticket.intent.risk_budget_pct),
            decimal(self.risk_limits.max_risk_per_trade_pct),
        )
        risk_qty = risk_amount / stop_distance
        notional_cap_qty = decimal(ticket.intent.max_notional_usdt) / reference
        exposure_qty = equity * decimal(ticket.intent.target_exposure_pct) / reference
        margin_qty = free_margin * leverage / reference
        portfolio_capacity = max(
            Decimal("0"),
            equity * decimal(self.risk_limits.max_gross_leverage)
            - decimal(portfolio.gross_notional_usdt),
        )
        portfolio_qty = portfolio_capacity / reference
        correlated_capacity = max(
            Decimal("0"),
            equity * decimal(self.risk_limits.max_correlated_exposure_pct)
            - decimal(portfolio.same_direction_correlated_notional_usdt),
        )
        correlated_qty = correlated_capacity / reference
        candidates = [
            risk_qty,
            notional_cap_qty,
            exposure_qty,
            margin_qty,
            portfolio_qty,
            correlated_qty,
        ]
        if rules.max_qty is not None:
            candidates.append(rules.max_qty)
        raw_qty = min(candidates)
        quantity = floor_to_step(raw_qty, rules.qty_step)
        if quantity < rules.min_qty:
            raise SizingError("calculated quantity is below exchange minimum")

        side = ticket.intent.side
        price = None
        if ticket.entry.order_type == "LIMIT":
            price = floor_to_step(reference, rules.tick_size) if side == "BUY" else ceil_to_step(reference, rules.tick_size)
        normalized_stop = floor_to_step(stop, rules.tick_size) if side == "SELL" else ceil_to_step(stop, rules.tick_size)
        valuation_price = price or reference
        notional = quantity * valuation_price
        if notional < rules.min_notional_usdt:
            raise SizingError("calculated notional is below exchange minimum")
        return ExecutionPlan(
            symbol=ticket.instrument.symbol,
            side=side,
            order_type=ticket.entry.order_type,
            quantity=quantity,
            price=price,
            stop_loss_price=normalized_stop,
            leverage=leverage,
            risk_amount_usdt=risk_amount,
            notional_usdt=notional,
        )
