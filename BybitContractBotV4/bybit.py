import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from itertools import count
from typing import Callable

from logger import logger


class ShadowExchange:
    """In-memory exchange used to exercise strategy code without external orders."""

    def __init__(self, account_equity_usdt=10000.0):
        self.account_equity_usdt = float(account_equity_usdt)
        self.enableRateLimit = True
        self.operations = []
        self.orders = []
        self.positions = []
        self._ids = count(1)

    def load_markets(self):
        self.operations.append({"operation": "load_markets"})
        return {}

    def set_sandbox_mode(self, enabled):
        self.operations.append({"operation": "set_sandbox_mode", "enabled": bool(enabled)})

    def set_margin_mode(self, marginMode, symbol, params=None):
        result = {
            "operation": "set_margin_mode",
            "marginMode": marginMode,
            "symbol": symbol,
            "params": dict(params or {}),
        }
        self.operations.append(result)
        return result

    def create_order(self, symbol, type, side, amount, price=None, params=None):
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

    def create_market_order(self, symbol, side, amount, params=None):
        return self.create_order(symbol, "market", side, amount, params=params)

    def create_limit_sell_order(self, symbol, amount, price, params=None):
        return self.create_order(symbol, "limit", "sell", amount, price, params)

    def create_limit_buy_order(self, symbol, amount, price, params=None):
        return self.create_order(symbol, "limit", "buy", amount, price, params)

    def fetch_open_orders(self, symbol=None):
        return [
            order
            for order in self.orders
            if order["status"] == "open" and (symbol is None or order["symbol"] == symbol)
        ]

    def cancel_order(self, order_id, symbol=None):
        for order in self.orders:
            if order["id"] == order_id and (symbol is None or order["symbol"] == symbol):
                order["status"] = "canceled"
                self.operations.append({"operation": "cancel_order", "order_id": order_id})
                return order
        raise ValueError(f"shadow order not found: {order_id}")

    def fetch_order(self, order_id, symbol=None, params=None):
        order_link_id = (params or {}).get("orderLinkId")
        for order in self.orders:
            if order["id"] == order_id or order["params"].get("orderLinkId") == order_link_id:
                if symbol is None or order["symbol"] == symbol:
                    return order
        raise ValueError(f"shadow order not found: {order_id or order_link_id}")

    def fetch_positions(self, symbols=None):
        if not symbols:
            return list(self.positions)
        allowed = set(symbols)
        return [position for position in self.positions if position.get("symbol") in allowed]

    def fetch_balance(self):
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

    def private_post_v5_position_trading_stop(self, params):
        result = {"retCode": 0, "retMsg": "shadow-only", "result": {}, "params": dict(params)}
        self.operations.append({"operation": "trading_stop", "params": dict(params)})
        return result


class _BybitConnection:
    def __init__(
        self,
        api_key="",
        secret_key="",
        *,
        mode="live",
        exchange=None,
        load_markets=True,
        shadow_equity_usdt=10000.0,
        position_mode="hedge",
    ):
        normalized_mode = str(getattr(mode, "value", mode)).strip().lower()
        normalized_position_mode = str(position_mode).strip().lower()
        if normalized_position_mode not in {"hedge", "one_way"}:
            raise ValueError("position_mode must be hedge or one_way")
        self.position_mode = normalized_position_mode
        if exchange is not None:
            self.exchange = exchange
            self.mode = normalized_mode
            return
        if normalized_mode == "shadow":
            self.exchange = ShadowExchange(shadow_equity_usdt)
            self.mode = normalized_mode
            return
        if normalized_mode not in {"testnet", "live"}:
            raise ValueError(f"unsupported Bybit mode: {normalized_mode}")
        if not api_key or not secret_key:
            raise ValueError(f"{normalized_mode} mode requires Bybit credentials")

        # Kept lazy so importing strategy modules cannot initialize a real exchange.
        import ccxt

        self.exchange = ccxt.bybit({
            'apiKey': api_key,
            'secret': secret_key,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'},
        })
        self.mode = normalized_mode
        self.exchange.enableRateLimit = True
        if normalized_mode == "testnet":
            self.exchange.set_sandbox_mode(True)
        if load_markets:
            self.exchange.load_markets()


class LazyBybitClient:
    """Defers client construction until strategy code first needs the exchange."""

    def __init__(self, factory: Callable[[], "_BybitConnection"]):
        self._factory = factory
        self._client = None
        self._lock = threading.Lock()

    @property
    def initialized(self):
        return self._client is not None

    @property
    def client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._factory()
        return self._client

    def __getattr__(self, name):
        return getattr(self.client, name)


def build_bybit_client(settings):
    return BybitClient(
        settings.api_key,
        settings.secret_key,
        mode=settings.mode,
        shadow_equity_usdt=settings.shadow_account_equity_usdt,
        position_mode=settings.position_mode,
    )


class BybitClient(_BybitConnection):

    _risk_metrics_cache_seconds = 15.0

    def response_headers(self):
        return dict(getattr(self.exchange, "last_response_headers", None) or {})

    def get_open_orders(self, symbol):
        orders = self.exchange.fetch_open_orders(symbol)
        return orders

    def cancel_order(self,order_id,symbol):
        cancelled_order = self.exchange.cancel_order(order_id,symbol)
        return cancelled_order

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
    ):
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
            # Bybit attaches this protection to each filled portion of the entry.
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
            side=str(side).lower(),
            amount=float(amount),
            price=None if price is None else float(price),
            params=params,
        )

    def find_order_by_link_id(self, symbol, order_link_id):
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

        # Bybit documents that realtime only retains a bounded recent closed-order
        # window and that this cache is cleared after a server restart.  History is
        # therefore required before deciding that an idempotent submission is absent.
        history_endpoint = getattr(self.exchange, "private_get_v5_order_history", None)
        if not callable(history_endpoint):
            if realtime_error is not None:
                raise RuntimeError("both open-order and realtime reconciliation failed") from realtime_error
            return None
        try:
            response = history_endpoint(
                {"category": "linear", "symbol": symbol, "orderLinkId": order_link_id, "limit": 1}
            )
            records = (((response or {}).get("result") or {}).get("list") or [])
            return records[0] if records else None
        except Exception as exc:
            raise RuntimeError("order reconciliation history is unavailable") from exc
        
    def check_any_limit_order_exists(self, symbol, side):
      """
      检查某个方向上是否还有任何限价平仓单
      """
      open_orders = self.exchange.fetch_open_orders(symbol)
      
      # 逻辑：如果是做多(side='buy')，平仓单应该是 'sell'
      target_side = 'sell' if side == 'buy' else 'buy'
      
      for order in open_orders:
          # 排除掉止盈止损单（CCXT通常会标记 type 为 'limit' 或 'market'）
          if order['side'] == target_side and order['type'] == 'limit':
              return True
      return False

    def create_limit_liquidation_order(self,symbol,side,amount,price):
        try:
            if side == 'buy':
                positionIdx = 1
                params = {
                    "positionIdx": positionIdx,
                }
                order = self.exchange.create_limit_sell_order(symbol,float(amount),float(price),params)
            else:
                positionIdx = 2
                params = {
                    "positionIdx": positionIdx,
                }
                order = self.exchange.create_limit_buy_order(symbol,float(amount),float(price),params)
        except Exception as e:
            logger.error(f"create_limit_liquidation_order error: {e}")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error("Exception type: %s" % exc_type)
            logger.error("Exception value: %s" % exc_value)
            for line in traceback.format_exception(exc_type, exc_value, exc_traceback):
                logger.error(line)

    def create_order(self, symbol, side, price, stop_loss_percentage=None, take_profit_percentage=None, usdt_cost=1.00,
                     leverage=100, ordertype='limit'):
        """
        symbol 币对
        side 方向 buy/sell
        price 价格
        stop_loss_percentage 止损百分比 例：300
        take_profit_percentage 止盈百分比 例：100
        usdt_cost usdt下单成本
        leverage 杠杆倍数
        """
        self.exchange.set_margin_mode(
            marginMode="cross", symbol=symbol, params={"leverage": leverage}
        )
        if take_profit_percentage or stop_loss_percentage:

            quantity = (usdt_cost * leverage) / price
            if side == 'buy':
                stop_loss_price = Decimal(price) * \
                                  (1 - Decimal(stop_loss_percentage / (Decimal(100) * Decimal(leverage))))
                take_profit_price = Decimal(price) * \
                                    (1 + Decimal(take_profit_percentage) / (Decimal(100) * Decimal(leverage)))
                positionIdx = 1
            else:
                stop_loss_price = Decimal(price) * \
                                  (1 + Decimal(stop_loss_percentage) / (Decimal(100) * Decimal(leverage)))
                take_profit_price = Decimal(price * \
                                            (1 - Decimal(take_profit_percentage) / (Decimal(100) * Decimal(leverage))))
                positionIdx = 2
            if stop_loss_percentage == 0:
                params = {
                    "positionIdx": positionIdx,
                    "takeProfit": take_profit_price
                }
            else:
                params = {
                    "positionIdx": positionIdx,
                    "stopLoss": stop_loss_price,
                    "takeProfit": take_profit_price
                }

        else:
            if side == 'buy':
                positionIdx = 1
            else:
                positionIdx = 2
            quantity = (usdt_cost * leverage) / price
            params = {
                'positionIdx': positionIdx
            }
        order = self.exchange.create_order(
            symbol=symbol,
            type=ordertype,
            side=side,
            price=price,
            amount=quantity,
            params=params
        )
        return order

    def create_lock_order(self, symbol, side, price, stop_loss_percentage=None, take_profit_percentage=None, quantity=1,
                          leverage=100, ordertype='limit'):
        """
        symbol 币对
        side 方向 buy/sell
        price 价格
        stop_loss_percentage 止损百分比 例：300
        take_profit_percentage 止盈百分比 例：100
        usdt_cost usdt下单成本
        leverage 杠杆倍数
        """
        self.exchange.set_margin_mode(
            marginMode="cross", symbol=symbol, params={"leverage": leverage}
        )
        if take_profit_percentage or stop_loss_percentage:

            if side == 'buy':
                stop_loss_price = Decimal(price) * \
                                  (1 - Decimal(stop_loss_percentage / (Decimal(100) * Decimal(leverage))))
                take_profit_price = Decimal(price) * \
                                    (1 + Decimal(take_profit_percentage) / (Decimal(100) * Decimal(leverage)))
                positionIdx = 1
            else:
                stop_loss_price = Decimal(price) * \
                                  (1 + Decimal(stop_loss_percentage) / (Decimal(100) * Decimal(leverage)))
                take_profit_price = Decimal(price * \
                                            (1 - Decimal(take_profit_percentage) / (Decimal(100) * Decimal(leverage))))
                positionIdx = 2
            if stop_loss_percentage == 0:
                params = {
                    "positionIdx": positionIdx,
                    "takeProfit": take_profit_price
                }
            else:
                params = {
                    "positionIdx": positionIdx,
                    "stopLoss": stop_loss_price,
                    "takeProfit": take_profit_price
                }

        else:
            params = {
                'positionIdx': 1 if side == 'buy' else 2
            }
        order = self.exchange.create_order(
            symbol=symbol,
            type=ordertype,
            side=side,
            price=price,
            amount=quantity,
            params=params
        )
        return order

    def get_all_orders(self, symbol):
        orders = self.exchange.fetch_open_orders(symbol)
        return orders

    def get_all_open_positions(self):
        # 获取所有币对仓位
        positions = self.exchange.fetch_positions()
        return positions

    # 平掉仓位：平仓操作
    def close_position(self, symbol, side, amount):
        try:
            # 创建市场订单来平仓
            print(f"平仓操作：{side} {amount} {symbol}")
            order = self.exchange.create_market_order(symbol, side, amount)
            print(f"平仓成功：", order)
        except Exception as e:
            print("错误：", str(e))

    # def get_open_positions(self,symbol):
    #     # 获取指定币对仓位
    #     symbol_positions = []
    #     positions = self.get_all_open_positions()
    #     for i in positions:
    #         if i['info']['symbol'] == symbol:
    #             symbol_positions.append(i)
    #     return symbol_positions

    def get_open_positions(self, symbol):

        # 获取指定币对仓位
        positions = self.exchange.fetch_positions([symbol])
        positions = [i for i in positions if i['side']]
        return positions

    def cancel_all_orders(self, symbol):
        # 取消所有限价单
        orders = self.get_all_orders(symbol)
        for order in orders:
            if order['info']['stopOrderType'] == '':
                self.exchange.cancel_order(order['id'], symbol)
        return True

    def cancel_open_position(self, symbol):
        # 取消所有限价单
        positions = self.get_open_positions(symbol)
        for i in positions:
            if i['info']['symbol'] == symbol:
                order = i['info']
                s = order['side']
                if s.lower() == 'buy':
                    side: str = 'sell'
                else:
                    side: str = 'buy'
                amount = order['size']
                params = {
                    'positionIdx': 1 if s.lower() == 'buy' else 2
                }
                ordertype = 'market'
                self.exchange.create_order(
                    symbol=symbol,
                    type=ordertype,
                    side=side,
                    amount=amount,
                    params=params
                )
        return True

    def cancel_position(self, symbol, amount, side):
        # 取消所有限价单
        params = {
            'positionIdx': 1 if side.lower() == 'buy' else 2
        }
        if side.lower() == 'buy':
            side = 'sell'
        else:
            side = 'buy'
        ordertype = 'market'
        self.exchange.create_order(
            symbol=symbol,
            type=ordertype,
            side=side,
            amount=amount,
            params=params
        )
        return True

    def get_balances(self):
        """
        获取账户余额信息
        :return: 账户的余额信息
        """
        return self.exchange.fetch_balance()

    def get_daily_risk_metrics(self, now=None):
        """Replay today's Bybit linear ledger into deterministic risk metrics.

        Transfers are excluded.  Realised trading cash flow, trading fees and
        funding/settlement changes are included through Bybit's ``change`` field.
        """

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
            return {
                "healthy": False,
                "realised_pnl": 0.0,
                "consecutive_losses": 0,
                "last_loss_at": None,
                "record_count": 0,
                "reason": "transaction_log_endpoint_unavailable",
            }
        records = []
        seen_cursors = set()
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
            return {
                "healthy": False,
                "realised_pnl": 0.0,
                "consecutive_losses": 0,
                "last_loss_at": None,
                "record_count": len(records),
                "reason": f"{type(exc).__name__}: transaction_log_query_failed",
            }
        if pagination_exhausted:
            # A partial daily ledger can understate losses, so never use it to
            # approve a risk-increasing ticket.
            return {
                "healthy": False,
                "realised_pnl": 0.0,
                "consecutive_losses": 0,
                "last_loss_at": None,
                "record_count": len(records),
                "reason": "transaction_log_pagination_limit_exceeded",
            }

        included_types = {"TRADE", "SETTLEMENT", "DELIVERY"}
        realised = 0.0
        closed_orders = {}
        for record in records:
            event_type = str(record.get("type") or "").upper()
            if event_type not in included_types:
                continue
            try:
                change = float(record.get("change") or 0)
                cash_flow = float(record.get("cashFlow") or 0)
                event_ms = int(record.get("transactionTime") or 0)
            except (TypeError, ValueError):
                continue
            realised += change
            if event_type == "TRADE" and abs(cash_flow) > 1e-15:
                key = str(record.get("orderId") or record.get("tradeId") or record.get("id") or "")
                if not key:
                    continue
                aggregate = closed_orders.setdefault(key, {"pnl": 0.0, "time": 0})
                aggregate["pnl"] += change
                aggregate["time"] = max(aggregate["time"], event_ms)
        ordered_closes = sorted(closed_orders.values(), key=lambda item: item["time"])
        consecutive_losses = 0
        last_loss_at = None
        for event in ordered_closes:
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

    # todo 平仓收益 private_get_v5_position_closed_pnl

    def edit_allsymbol_positions(self, symbol, stop_loss_percentage, take_profit_percentage, leverage):
        orders = self.get_open_positions(symbol)
        edit_list = []
        for o in orders:
            price = float(o['info']['avgPrice'])
            side = o['info']['side'].lower()
            if side == 'buy':
                stop_loss_price = price * \
                                  (1 - stop_loss_percentage / (100 * leverage))
                take_profit_price = price * \
                                    (1 + take_profit_percentage / (100 * leverage))
                positionIdx = 1
            else:
                stop_loss_price = price * \
                                  (1 + stop_loss_percentage / (100 * leverage))
                take_profit_price = price * \
                                    (1 - take_profit_percentage / (100 * leverage))
                positionIdx = 2
            params = {
                'category': 'linear',
                'symbol': symbol,
                'takeProfit': str(take_profit_price),  # 将止盈价格设置为字符串类型的0
                'stopLoss': str(stop_loss_price),  # 将止损价格设置为字符串类型的0
                'tpslMode': 'Full',  # 根据文档说明，需要根据实际需求设置止盈止损模式
                'positionIdx': positionIdx
            }
            edit_list.append(
                self.exchange.private_post_v5_position_trading_stop(params))
        return edit_list

    def edit_positions(self, symbol, side, stop_loss_price=0, take_profit_price=0):
        params = {
            'category': 'linear',
            'symbol': symbol,
            'takeProfit': str(take_profit_price),  # 将止盈价格设置为字符串类型
            'stopLoss': str(stop_loss_price),  # 将止损价格设置为字符串类型
            'tpslMode': 'Full',  # 根据文档说明，需要根据实际需求设置止盈止损模式
            'positionIdx': 1 if side == 'buy' else 2
        }
        try:
            self.exchange.private_post_v5_position_trading_stop(params)
            return True
        except:
            return False

    def set_trailing_stop(self, symbol, side, trailing_percent, activation_price=0, trigger_by="LastPrice"):
        """
        设置追踪止损
        
        参数:
        symbol - 交易对，如 'BTCUSDT'
        side - 方向，'buy' 或 'sell'
        trailing_percent - 追踪价差
        activation_price - 激活价格，可选
        trigger_by - 触发价格类型，可选值: "LastPrice"(最新价格) 或 "MarkPrice"(标记价格)
        
        返回:
        API响应结果
        """
        params = {
            'category': 'linear',
            'symbol': symbol,
            'trailingStop': str(trailing_percent),  # 设置追踪价差
            'tpslMode': 'Full',  # 根据文档说明，需要根据实际需求设置止盈止损模式
            'positionIdx': 1 if side == 'buy' else 2,
            'triggerBy': trigger_by  # 添加触发价格类型参数
        }
        
        # 如果设置了激活价格，则添加到参数中
        if activation_price:
            params['activePrice'] = str(activation_price)  # 修改为activePrice
        
        try:
            return self.exchange.private_post_v5_position_trading_stop(params)
        except Exception as e:
            print(f"设置追踪止损失败: {e}")
            return False

if __name__ == "__main__":
    raise SystemExit("Direct execution is disabled; launch bot_threshold_super_v4_1.py with .env.local")
