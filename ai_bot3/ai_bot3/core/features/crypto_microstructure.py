from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class BookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class PublicTrade:
    price: float
    size: float
    taker_side: str


def order_book_features(bids: Sequence[BookLevel], asks: Sequence[BookLevel], depth: int = 5) -> dict[str, float]:
    if not bids or not asks or depth <= 0:
        raise ValueError("both book sides and positive depth are required")
    best_bid = max(bids, key=lambda level: level.price).price
    best_ask = min(asks, key=lambda level: level.price).price
    if best_bid <= 0 or best_ask <= best_bid:
        raise ValueError("crossed or invalid order book")
    midpoint = (best_bid + best_ask) / 2
    bid_depth = sum(max(0, level.size) for level in sorted(bids, key=lambda x: x.price, reverse=True)[:depth])
    ask_depth = sum(max(0, level.size) for level in sorted(asks, key=lambda x: x.price)[:depth])
    total_depth = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total_depth if total_depth else 0.0
    return {
        "orderbook_spread_bps": (best_ask - best_bid) / midpoint * 10_000,
        "orderbook_imbalance_l5": imbalance,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
    }


def trade_flow_features(trades: Iterable[PublicTrade]) -> dict[str, float]:
    buy_volume = 0.0
    sell_volume = 0.0
    count = 0
    notional = 0.0
    for trade in trades:
        if trade.price <= 0 or trade.size < 0:
            continue
        count += 1
        notional += trade.price * trade.size
        if trade.taker_side.strip().lower() == "buy":
            buy_volume += trade.size
        elif trade.taker_side.strip().lower() == "sell":
            sell_volume += trade.size
    total = buy_volume + sell_volume
    return {
        "aggressive_buy_volume": buy_volume,
        "aggressive_sell_volume": sell_volume,
        "aggressive_cvd_1m": buy_volume - sell_volume,
        "aggressive_buy_ratio": buy_volume / total if total else 0.5,
        "trade_count": float(count),
        "average_trade_notional": notional / count if count else 0.0,
    }
