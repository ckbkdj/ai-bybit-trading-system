from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.backtest.event_driven import EventDrivenBacktest, SignalEvent
from core.labels.triple_barrier import MarketBar


def _bar(start: datetime, high: float, low: float, close: float) -> MarketBar:
    return MarketBar(
        symbol="BTCUSDT",
        open_time=start,
        close_time=start + timedelta(minutes=1),
        available_at=start + timedelta(minutes=1),
        open=100.0,
        high=high,
        low=low,
        close=close,
        volume=10_000,
        spread_bps=5,
        depth_usdt=20_000,
        volatility_bps=25,
        funding_bps=0.2,
    )


def test_event_driven_backtest_uses_fills_intrabar_path_and_all_costs():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="alpha-test-1",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
        requested_notional_usdt=10_000,
        feature_available_at=(start,),
    )
    report = EventDrivenBacktest().run(
        [signal],
        {"BTCUSDT": [_bar(start, 102.0, 99.5, 101.5)]},
    )
    assert report.intrabar_path_used is True
    assert len(report.trades) == 1
    trade = report.trades[0]
    assert trade.exit_reason == "TAKE_PROFIT"
    assert trade.fill_probability > 0
    assert trade.fee_cost > 0 and trade.slippage_cost > 0 and trade.funding_cost > 0
    assert trade.net_pnl < trade.gross_pnl
    assert trade.notional_usdt <= 10_000


def test_event_driven_backtest_fails_closed_on_nonpositive_edge():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="alpha-test-2",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.0,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    report = EventDrivenBacktest().run([signal], {"BTCUSDT": [_bar(start, 102, 99, 101)]})
    assert not report.trades
    assert report.rejected_signals["non_positive_lower_bound_edge"] == 1
