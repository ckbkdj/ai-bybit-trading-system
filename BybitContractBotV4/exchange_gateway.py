"""Small production exchange boundary used by the versioned execution service.

The historical :mod:`bybit` module remains untouched as a compatibility source
for the v2-v6 bots.  New execution code imports this module so legacy helpers
that can close positions without a ticket are not reachable from production.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from itertools import count
from typing import Any, Callable

from logger import logger


class ShadowExchange:
    """In-memory CCXT-shaped exchange that can never send an external order."""

    def __init__(self, account_equity_usdt: float = 10_000.0):
        self.account_equity_usdt = float(account_equity_usdt)
        self.enableRateLimit = True
        self.operations: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self._ids = count(1)

    def load_markets(self) -> dict[str, Any]:
        self.operations.append({"operation": "load_markets"})
        return {}

    def set_sandbox_mode(self, enabled: bool) -> None:
        self.operations.append({"operation": "set_sandbox_mode", "enabled": bool(enabled)})

    def set_margin_mode(self, marginMode: str, symbol: str, params=None) -> dict[str, Any]:
        result = {
            "operation": "set_margin_mode",
            "marginMode": marginMode,
            "symbol": symbol,
            "params": dict(params or {}),
        }
        self.operations.append(result)
        return result

    def create_order(self, symbol, type, side, amount, price=None, params=None) -> dict[str, Any]:
        order_id = f"shadow-{next(self._ids)}"
        order = {
            "id": order_id,
            "symbol": symbol,
            "type": type,
            "side": side,
            "amount": float(amount),
            "price": None if price is None else float(price),
            "params": dict(params or {}),
            "status": "open",
            "timestamp": int(time.time() * 1000),
            "info": {
                "orderId": order_id,
                "orderLinkId": dict(params or {}).get("orderLinkId"),
                "stopOrderType": "",
            },
            "shadow": True,
        }
        self.orders.append(order)
        self.operations.append({"operation": "create_order", "order": order})
        logger.warning(
            "SHADOW order recorded only: %s %s %s amount=%s",
            symbol,
            side,
            type,
            amount,
        )
        return order

    def fetch_open_orders(self, symbol=None) -> list[dict[str, Any]]:
        return [
            order
            for order in self.orders
            if order["status"] == "open" and (symbol is None or order["symbol"] == symbol)
        ]

    def cancel_order(self, order_id, symbol=None) -> dict[str, Any]:
        for order in self.orders:
            if order["id"] == order_id and (symbol is None or order["symbol"] == symbol):
                order["status"] = "canceled"
                self.operations.append({"operation": "cancel_order", "order_id": order_id})
                return order
        raise ValueError(f"shadow order not found: {order_id}")

    def fetch_my_trades(self, symbol=None, params=None) -> list[dict[str, Any]]:
        return []

    def fetch_positions(self, symbols=None) -> list[dict[str, Any]]:
        if not symbols:
            return list(self.positions)
        allowed = set(symbols)
        return [position for position in self.positions if position.get("symbol") in allowed]

    def fetch_balance(self) -> dict[str, Any]:
        equity = self.account_equity_usdt
        coin = {
            "coin": "USDT",
            "equity": str(equity),
            "walletBalance": str(equity),
            "totalPositionIM": "0",
            "unrealisedPnl": "0",
            "curRealisedPnl": "0",
        }
        return {
            "USDT": {"total": equity, "free": equity, "used": 0.0},
            "total": {"USDT": equity},
            "free": {"USDT": equity},
            "used": {"USDT": 0.0},
            "info": {"result": {"list": [{"coin": [coin]}]}},
        }


class ExchangeGateway:
    """Only the exchange operations required by the ticket execution service."""

    _risk_metrics_cache_seconds = 15.0

    def __init__(
        self,
        api_key: str = "",
        secret_key: str = "",
        *,
        mode: str = "shadow",
        exchange=None,
        load_markets: bool = True,
        shadow_equity_usdt: float = 10_000.0,
        position_mode: str = "hedge",
    ):
        normalized_mode = str(getattr(mode, "value", mode)).strip().lower()
        normalized_position_mode = str(position_mode).strip().lower()
        if normalized_position_mode not in {"hedge", "one_way"}:
            raise ValueError("position_mode must be hedge or one_way")
        self.mode = normalized_mode
        self.position_mode = normalized_position_mode
        if exchange is not None:
            self.exchange = exchange
            return
        if normalized_mode == "shadow":
            self.exchange = ShadowExchange(shadow_equity_usdt)
            return
        if normalized_mode not in {"testnet", "live"}:
            raise ValueError(f"unsupported Bybit mode: {normalized_mode}")
        if not api_key or not secret_key:
            raise ValueError(f"{normalized_mode} mode requires Bybit credentials")

        import ccxt

        self.exchange = ccxt.bybit(
            {
                "apiKey": api_key,
                "secret": secret_key,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        self.exchange.enableRateLimit = True
        if normalized_mode == "testnet":
            self.exchange.set_sandbox_mode(True)
        if load_markets:
            self.exchange.load_markets()

    def response_headers(self) -> dict[str, Any]:
        return dict(getattr(self.exchange, "last_response_headers", None) or {})

    def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return self.exchange.cancel_order(order_id, symbol)

    def create_ticket_order(
        self,
        *,
        symbol,
        side,
        order_type,
        amount,
        price,
        leverage,
        order_link_id,
        reduce_only=False,
        stop_loss_price=None,
        stop_trigger_by="MarkPrice",
        time_in_force="GTC",
        post_only=False,
    ) -> dict[str, Any]:
        if not reduce_only:
            self.exchange.set_margin_mode(
                marginMode="cross", symbol=symbol, params={"leverage": float(leverage)}
            )
        normalized_side = str(side).lower()
        if self.position_mode == "one_way":
            position_idx = 0
        elif reduce_only:
            position_idx = 2 if normalized_side == "buy" else 1
        else:
            position_idx = 1 if normalized_side == "buy" else 2
        params = {
            "positionIdx": position_idx,
            "orderLinkId": order_link_id,
            "reduceOnly": bool(reduce_only),
            "timeInForce": "PostOnly" if post_only else str(time_in_force),
        }
        if stop_loss_price is not None and not reduce_only:
            params.update(
                {
                    "stopLoss": str(stop_loss_price),
                    "slTriggerBy": str(stop_trigger_by),
                    "tpslMode": "Full",
                }
            )
        return self.exchange.create_order(
            symbol=symbol,
            type=str(order_type).lower(),
            side=normalized_side,
            amount=float(amount),
            price=None if price is None else float(price),
            params=params,
        )

    def find_order_by_link_id(self, symbol: str, order_link_id: str) -> dict[str, Any] | None:
        for order in self.exchange.fetch_open_orders(symbol):
            info = order.get("info") or {}
            if info.get("orderLinkId") == order_link_id or order.get("clientOrderId") == order_link_id:
                return order
        realtime_error = None
        try:
            response = self.exchange.private_get_v5_order_realtime(
                {"category": "linear", "symbol": symbol, "orderLinkId": order_link_id}
            )
            records = (((response or {}).get("result") or {}).get("list") or [])
            if records:
                return records[0]
        except Exception as exc:
            realtime_error = exc

        history_endpoint = getattr(self.exchange, "private_get_v5_order_history", None)
        if not callable(history_endpoint):
            if self.mode == "shadow":
                return None
            if realtime_error is not None:
                raise RuntimeError("both realtime and order-history reconciliation failed") from realtime_error
            return None
        try:
            response = history_endpoint(
                {"category": "linear", "symbol": symbol, "orderLinkId": order_link_id, "limit": 1}
            )
            records = (((response or {}).get("result") or {}).get("list") or [])
            return records[0] if records else None
        except Exception as exc:
            raise RuntimeError("order reconciliation history is unavailable") from exc

    def get_balances(self) -> dict[str, Any]:
        return self.exchange.fetch_balance()

    def get_all_open_positions(self) -> list[dict[str, Any]]:
        return self.exchange.fetch_positions()

    def get_all_open_orders(self) -> list[dict[str, Any]]:
        return self.exchange.fetch_open_orders()

    def get_daily_risk_metrics(self, now=None) -> dict[str, Any]:
        """Replay today's official account ledger; partial evidence fails closed."""

        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if self.mode == "shadow":
            return {
                "healthy": True,
                "realised_pnl": 0.0,
                "consecutive_losses": 0,
                "last_loss_at": None,
                "record_count": 0,
            }
        cache = getattr(self, "_daily_risk_cache", None)
        monotonic_now = time.monotonic()
        if (
            cache
            and cache[0] == current.date().isoformat()
            and monotonic_now - cache[1] <= self._risk_metrics_cache_seconds
        ):
            return dict(cache[2])

        start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        params = {
            "accountType": "UNIFIED",
            "category": "linear",
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(current.timestamp() * 1000),
            "limit": 50,
        }
        endpoint = getattr(self.exchange, "private_get_v5_account_transaction_log", None)
        if not callable(endpoint):
            return self._unhealthy_risk("transaction_log_endpoint_unavailable")
        records: list[dict[str, Any]] = []
        seen_cursors: set[str] = set()
        pagination_exhausted = False
        try:
            for page_number in range(20):
                response = endpoint(dict(params))
                result = (response or {}).get("result") or {}
                records.extend(result.get("list") or [])
                cursor = str(result.get("nextPageCursor") or "")
                if not cursor or cursor in seen_cursors:
                    break
                seen_cursors.add(cursor)
                params["cursor"] = cursor
                if page_number == 19:
                    pagination_exhausted = True
        except Exception as exc:
            return self._unhealthy_risk(
                f"{type(exc).__name__}: transaction_log_query_failed", len(records)
            )
        if pagination_exhausted:
            return self._unhealthy_risk("transaction_log_pagination_limit_exceeded", len(records))

        realised = 0.0
        closed_orders: dict[str, dict[str, Any]] = {}
        for record in records:
            event_type = str(record.get("type") or "").upper()
            if event_type not in {"TRADE", "SETTLEMENT", "DELIVERY"}:
                continue
            try:
                change = float(record.get("change") or 0)
                cash_flow = float(record.get("cashFlow") or 0)
                event_ms = int(record.get("transactionTime") or 0)
            except (TypeError, ValueError):
                continue
            realised += change
            if event_type == "TRADE" and abs(cash_flow) > 1e-15:
                key = str(
                    record.get("orderId") or record.get("tradeId") or record.get("id") or ""
                )
                if not key:
                    continue
                aggregate = closed_orders.setdefault(key, {"pnl": 0.0, "time": 0})
                aggregate["pnl"] += change
                aggregate["time"] = max(aggregate["time"], event_ms)
        consecutive_losses = 0
        last_loss_at = None
        for event in sorted(closed_orders.values(), key=lambda item: item["time"]):
            if event["pnl"] < 0:
                consecutive_losses += 1
                if event["time"] > 0:
                    last_loss_at = datetime.fromtimestamp(event["time"] / 1000, timezone.utc)
            else:
                consecutive_losses = 0
                last_loss_at = None
        metrics = {
            "healthy": True,
            "realised_pnl": realised,
            "consecutive_losses": consecutive_losses,
            "last_loss_at": last_loss_at,
            "record_count": len(records),
        }
        self._daily_risk_cache = (current.date().isoformat(), monotonic_now, metrics)
        return dict(metrics)

    @staticmethod
    def _unhealthy_risk(reason: str, record_count: int = 0) -> dict[str, Any]:
        return {
            "healthy": False,
            "realised_pnl": 0.0,
            "consecutive_losses": 0,
            "last_loss_at": None,
            "record_count": record_count,
            "reason": reason,
        }


class LazyExchangeGateway:
    """Thread-safe deferred gateway construction for import-side-effect tests."""

    def __init__(self, factory: Callable[[], ExchangeGateway]):
        self._factory = factory
        self._client: ExchangeGateway | None = None
        self._lock = threading.Lock()

    @property
    def initialized(self) -> bool:
        return self._client is not None

    @property
    def client(self) -> ExchangeGateway:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._factory()
        return self._client

    def __getattr__(self, name):
        return getattr(self.client, name)


def build_exchange_gateway(settings) -> ExchangeGateway:
    return ExchangeGateway(
        settings.api_key,
        settings.secret_key,
        mode=settings.mode,
        shadow_equity_usdt=settings.shadow_account_equity_usdt,
        position_mode=settings.position_mode,
    )
