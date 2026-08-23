from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.backtest.event_driven import BacktestConfig, EventDrivenBacktest, SignalEvent
from core.labels.triple_barrier import MarketBar


def _bar(
    start: datetime,
    high: float,
    low: float,
    close: float,
    *,
    symbol: str = "BTCUSDT",
) -> MarketBar:
    return MarketBar(
        symbol=symbol,
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
        {
            "BTCUSDT": [
                _bar(start, 102.0, 99.5, 100.5),
                _bar(start + timedelta(minutes=1), 102.0, 99.5, 101.5),
            ]
        },
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


def test_research_backtest_can_measure_negative_edge_without_changing_production_default():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="ablation-negative-edge",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=-0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    report = EventDrivenBacktest(
        BacktestConfig(require_positive_lower_bound_edge=False)
    ).run([signal], {"BTCUSDT": [_bar(start, 102, 99, 101)]})
    assert len(report.trades) == 1
    assert report.configuration["require_positive_lower_bound_edge"] is False


def test_drawdown_uses_mark_to_market_path_not_only_realized_closes():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="alpha-mtm-1",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=5_000,
        stop_loss_bps=2_000,
        max_holding_sec=180,
        feature_available_at=(start,),
    )
    bars = [
        _bar(start, 101.0, 89.0, 90.0),
        _bar(start + timedelta(minutes=1), 101.0, 90.0, 100.0),
        _bar(start + timedelta(minutes=2), 103.0, 99.0, 102.0),
        # This bar proves that the complete latency-adjusted holding window is
        # covered; its post-expiry OHLC values must not enter the result.
        _bar(start + timedelta(minutes=3), 200.0, 1.0, 50.0),
    ]
    report = EventDrivenBacktest().run([signal], {"BTCUSDT": bars})
    assert len(report.trades) == 1
    assert report.trades[0].net_pnl > 0
    assert report.mark_to_market_used is True
    assert report.max_drawdown > 0
    assert len(report.equity_curve) >= 3
    assert min(point.equity_usdt for point in report.equity_curve) < report.initial_equity_usdt
    adverse = [point.equity_usdt for point in report.equity_curve if point.mark_kind == "intrabar_adverse"]
    closes = [point.equity_usdt for point in report.equity_curve if point.mark_kind == "bar_close"]
    assert adverse and min(adverse) < min(closes)


def test_unrealized_drawdown_blocks_new_cross_symbol_risk():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = SignalEvent(
        signal_id="btc-open-loss",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=9_000,
        stop_loss_bps=5_000,
        max_holding_sec=300,
        feature_available_at=(start,),
    )
    second = SignalEvent(
        signal_id="eth-must-be-blocked",
        symbol="ETHUSDT",
        side="BUY",
        decision_at=start + timedelta(minutes=1),
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=9_000,
        stop_loss_bps=5_000,
        max_holding_sec=300,
        feature_available_at=(start + timedelta(minutes=1),),
    )
    btc_bars = [
        _bar(start + timedelta(minutes=i), 101.0, 95.0, 96.0)
        for i in range(6)
    ]
    eth_bars = [
        _bar(start + timedelta(minutes=i), 101.0, 99.0, 100.0, symbol="ETHUSDT")
        for i in range(7)
    ]
    report = EventDrivenBacktest(
        config=BacktestConfig(equity_drawdown_limit=0.0001)
    ).run(
        [first, second],
        {"BTCUSDT": btc_bars, "ETHUSDT": eth_bars},
    )
    assert [trade.signal_id for trade in report.trades] == ["btc-open-loss"]
    assert report.rejected_signals["equity_drawdown_limit"] == 1
