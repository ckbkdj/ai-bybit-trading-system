from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TradeIntent:
    side: str
    notional_usdt: float
    entry_price: float
    exit_price: float
    fill_fraction: float = 1.0
    maker_entry: bool = False
    maker_exit: bool = False
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    funding_bps: float = 0.0


@dataclass(frozen=True)
class TradeResult:
    gross_pnl: float
    fee: float
    slippage_cost: float
    funding_cost: float
    net_pnl: float
    filled_notional: float


class CostAwareBacktest:
    def __init__(self, maker_fee_bps: float = 2.0, taker_fee_bps: float = 5.5):
        self.maker_fee_bps = maker_fee_bps
        self.taker_fee_bps = taker_fee_bps

    def simulate(self, intent: TradeIntent) -> TradeResult:
        if intent.notional_usdt < 0 or not 0 <= intent.fill_fraction <= 1:
            raise ValueError("invalid notional or fill fraction")
        if intent.entry_price <= 0 or intent.exit_price <= 0:
            raise ValueError("prices must be positive")
        filled = intent.notional_usdt * intent.fill_fraction
        direction = 1 if intent.side.upper() == "BUY" else -1
        gross_return = direction * (intent.exit_price - intent.entry_price) / intent.entry_price
        gross_pnl = filled * gross_return
        entry_fee = self.maker_fee_bps if intent.maker_entry else self.taker_fee_bps
        exit_fee = self.maker_fee_bps if intent.maker_exit else self.taker_fee_bps
        fee = filled * (entry_fee + exit_fee) / 10_000
        slippage = filled * (intent.entry_slippage_bps + intent.exit_slippage_bps) / 10_000
        funding = filled * intent.funding_bps / 10_000
        return TradeResult(gross_pnl, fee, slippage, funding, gross_pnl - fee - slippage - funding, filled)

    def run(self, intents: Iterable[TradeIntent]) -> list[TradeResult]:
        return [self.simulate(intent) for intent in intents]
