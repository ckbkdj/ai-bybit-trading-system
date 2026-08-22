from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Optional

from contracts.operation_ticket_v1 import OperationTicket
from private_stream import PrivateStreamHandler
from risk_guard import AccountSnapshot, MarketSnapshot, PortfolioSnapshot, SystemHealth
from sizing import InstrumentRules
from ticket_store import ExecutionStore, parse_time


class BybitRuntimeContext:
    """Build risk inputs only from live exchange/account state and explicit health providers."""

    def __init__(
        self,
        *,
        public_exchange,
        account_client,
        store: ExecutionStore,
        mode: str,
        private_stream: Optional[PrivateStreamHandler] = None,
        regime_provider: Optional[Callable[[str], str]] = None,
        data_health_provider: Optional[Callable[[], bool]] = None,
        correlated_symbols: Optional[set[str]] = None,
    ):
        self.public_exchange = public_exchange
        self.account_client = account_client
        self.store = store
        self.mode = mode
        self.private_stream = private_stream
        self.regime_provider = regime_provider
        self.data_health_provider = data_health_provider
        self.correlated_symbols = {item.upper() for item in (correlated_symbols or set())}

    def market(self, ticket: OperationTicket) -> MarketSnapshot:
        ticker = self.public_exchange.fetch_ticker(ticket.instrument.symbol)
        last = float(ticker.get("last") or ticker.get("close") or 0)
        bid = float(ticker.get("bid") or 0)
        ask = float(ticker.get("ask") or 0)
        if not bid or not ask:
            book = self.public_exchange.fetch_order_book(ticket.instrument.symbol, limit=5)
            bid = float(book["bids"][0][0]) if book.get("bids") else 0
            ask = float(book["asks"][0][0]) if book.get("asks") else 0
        regime = self.regime_provider(ticket.instrument.symbol) if self.regime_provider else "unknown"
        return MarketSnapshot(
            ticket.instrument.symbol, last, bid, ask, regime, datetime.now(timezone.utc)
        )

    def account(self, ticket: OperationTicket) -> AccountSnapshot:
        balance = self.account_client.get_balances()
        usdt = balance.get("USDT") or {}
        equity = float(usdt.get("total") or 0)
        free = float(usdt.get("free") if usdt.get("free") is not None else equity)
        used = float(usdt.get("used") or max(0, equity - free))
        runtime = self.store.risk_runtime()
        cooldown = parse_time(runtime["cooldown_until"]) if runtime.get("cooldown_until") else None
        return AccountSnapshot(
            equity_usdt=equity,
            free_margin_usdt=free,
            margin_used_usdt=used,
            realised_pnl_today=float(runtime.get("realised_pnl") or 0),
            unrealised_pnl=float(runtime.get("unrealised_pnl") or 0),
            consecutive_losses=int(runtime.get("consecutive_losses") or 0),
            cooldown_until=cooldown,
        )

    def portfolio(self, ticket: OperationTicket) -> PortfolioSnapshot:
        positions = self.account_client.get_all_open_positions()
        gross = 0.0
        correlated = 0.0
        target_side = None
        target_qty = 0.0
        target_avg = None
        target_notional = 0.0
        desired_side = "long" if ticket.intent.side == "BUY" else "short"
        for position in positions or []:
            symbol = str(position.get("symbol") or (position.get("info") or {}).get("symbol") or "").upper()
            quantity = abs(float(position.get("contracts") or (position.get("info") or {}).get("size") or 0))
            mark = float(position.get("markPrice") or (position.get("info") or {}).get("markPrice") or 0)
            notional = abs(float(position.get("notional") or quantity * mark))
            side = str(position.get("side") or "").lower()
            gross += notional
            if symbol in self.correlated_symbols and side == desired_side:
                correlated += notional
            if symbol == ticket.instrument.symbol:
                target_side = side.upper() if side else None
                target_qty = quantity if side != "short" else -quantity
                target_avg = float(position.get("entryPrice") or (position.get("info") or {}).get("avgPrice") or 0) or None
                target_notional = notional
        version = self.store.sync_position(
            ticket.instrument.symbol,
            side=target_side,
            quantity=target_qty,
            avg_price=target_avg,
            notional_usdt=target_notional,
            source="bybit-reconcile",
        )
        return PortfolioSnapshot(gross, correlated, version, target_qty)

    def health(self, ticket: OperationTicket) -> SystemHealth:
        try:
            server_ms = float(self.public_exchange.fetch_time())
            drift = server_ms / 1000 - time.time()
        except Exception:
            drift = float("inf")
        data_healthy = bool(self.data_health_provider and self.data_health_provider())
        websocket = bool(self.private_stream and self.private_stream.connected)
        return SystemHealth(
            self.mode,
            self.store.kill_switch_enabled(),
            websocket,
            data_healthy,
            drift,
        )

    @staticmethod
    def _step(value, fallback: str) -> Decimal:
        try:
            numeric = Decimal(str(value))
        except Exception:
            return Decimal(fallback)
        if numeric <= 0:
            return Decimal(fallback)
        # CCXT may expose precision as decimal places rather than a tick size.
        if numeric == numeric.to_integral_value() and numeric <= 18:
            return Decimal(1).scaleb(-int(numeric))
        return numeric

    @staticmethod
    def _decimal_value(value, fallback: str) -> Decimal:
        try:
            numeric = Decimal(str(value))
            return numeric if numeric > 0 else Decimal(fallback)
        except Exception:
            return Decimal(fallback)

    def instrument_rules(self, ticket: OperationTicket) -> InstrumentRules:
        market = self.public_exchange.market(ticket.instrument.symbol)
        limits = market.get("limits") or {}
        precision = market.get("precision") or {}
        amount_limits = limits.get("amount") or {}
        cost_limits = limits.get("cost") or {}
        return InstrumentRules(
            symbol=ticket.instrument.symbol,
            min_qty=self._decimal_value(amount_limits.get("min"), "0.001"),
            qty_step=self._step(precision.get("amount"), "0.001"),
            tick_size=self._step(precision.get("price"), "0.0001"),
            min_notional_usdt=self._decimal_value(cost_limits.get("min"), "5"),
            max_qty=(
                self._decimal_value(amount_limits.get("max"), "0.001")
                if amount_limits.get("max") is not None else None
            ),
        )
