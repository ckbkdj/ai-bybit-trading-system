from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import core.backtest.event_driven as event_driven
from core.backtest.event_driven import BacktestConfig, EventDrivenBacktest, SignalEvent
from core.labels.triple_barrier import MarketBar


def _bar(
    start: datetime,
    high: float,
    low: float,
    close: float,
    *,
    symbol: str = "BTCUSDT",
    observed_execution_costs: bool = False,
    open_price: float = 100.0,
) -> MarketBar:
    return MarketBar(
        symbol=symbol,
        open_time=start,
        close_time=start + timedelta(minutes=1),
        available_at=start + timedelta(minutes=1),
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=10_000,
        spread_bps=5,
        depth_usdt=20_000,
        volatility_bps=25,
        funding_bps=0.2,
        spread_source=(
            "bybit.public.orderbook" if observed_execution_costs else "ohlcv_proxy"
        ),
        depth_source=(
            "bybit.public.orderbook" if observed_execution_costs else "ohlcv_proxy"
        ),
        funding_source=(
            "bybit.public.funding_history"
            if observed_execution_costs
            else "zero_proxy"
        ),
        spread_observed=observed_execution_costs,
        depth_observed=observed_execution_costs,
        funding_observed=observed_execution_costs,
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
    assert report.execution_cost_evidence_complete is False
    assert report.proxy_execution_cost_trade_count == 1


def test_event_driven_backtest_propagates_direct_cost_provenance():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="alpha-direct-cost-1",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    bars = [
        _bar(start, 102, 99.5, 101.5, observed_execution_costs=True),
        _bar(
            start + timedelta(minutes=1),
            102,
            99.5,
            101.5,
            observed_execution_costs=True,
        ),
    ]
    report = EventDrivenBacktest().run([signal], {"BTCUSDT": bars})
    assert len(report.trades) == 1
    assert report.execution_cost_evidence_complete is True
    assert report.direct_execution_cost_trade_count == 1
    assert report.proxy_execution_cost_trade_count == 0
    trade = report.trades[0]
    assert trade.entry_spread_source == "bybit.public.orderbook"
    assert trade.funding_source == "bybit.public.funding_history"


def test_two_x_cost_stress_includes_observed_spread_slippage_and_funding():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="alpha-direct-cost-stress-1",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    bars = [
        _bar(start, 102, 99.5, 101.5, observed_execution_costs=True),
        _bar(
            start + timedelta(minutes=1),
            102,
            99.5,
            101.5,
            observed_execution_costs=True,
        ),
    ]

    baseline = EventDrivenBacktest().run([signal], {"BTCUSDT": bars})
    stressed = EventDrivenBacktest().run(
        [signal], {"BTCUSDT": bars}, cost_multiplier=2.0
    )

    base_trade = baseline.trades[0]
    stressed_trade = stressed.trades[0]
    assert stressed_trade.fee_cost / stressed_trade.notional_usdt == pytest.approx(
        base_trade.fee_cost / base_trade.notional_usdt * 2
    )
    assert stressed_trade.funding_cost / stressed_trade.notional_usdt == pytest.approx(
        base_trade.funding_cost / base_trade.notional_usdt * 2
    )
    assert (
        stressed_trade.slippage_cost / stressed_trade.notional_usdt
        > base_trade.slippage_cost / base_trade.notional_usdt * 1.9
    )
    assert stressed_trade.net_return < base_trade.net_return
    assert stressed_trade.notional_usdt == pytest.approx(base_trade.notional_usdt)


def test_pretrade_sizing_includes_costs_and_gap_loss_breaches_risk_gate():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="gap-risk-breach",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=1_000,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    bars = [
        _bar(
            start,
            100.5,
            100.0,
            100.2,
            observed_execution_costs=True,
        ),
        _bar(
            start + timedelta(minutes=1),
            97.0,
            94.0,
            96.0,
            observed_execution_costs=True,
            open_price=95.0,
        ),
    ]

    report = EventDrivenBacktest().run([signal], {"BTCUSDT": bars})

    assert len(report.trades) == 1
    trade = report.trades[0]
    assert trade.pretrade_risk_loss_bps > signal.stop_loss_bps
    stop_only_notional = (
        report.initial_equity_usdt
        * BacktestConfig().risk_per_trade
        / (signal.stop_loss_bps / 10_000.0)
    )
    assert trade.notional_usdt < stop_only_notional
    assert trade.net_pnl < -trade.risk_budget_usdt
    assert report.risk_budget_breach_count == 1
    assert report.risk_policy_compliant is False


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


def test_unfilled_limit_reserves_symbol_until_timeout_is_observable():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = SignalEvent(
        signal_id="limit-waiting",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=60,
        order_type="LIMIT",
        limit_price=50.0,
        max_wait_sec=120,
    )
    overlapping = SignalEvent(
        **{
            **first.__dict__,
            "signal_id": "must-not-use-future-timeout",
            "decision_at": start + timedelta(minutes=1),
            "order_type": "MARKET",
            "limit_price": None,
        }
    )
    after_timeout = SignalEvent(
        **{
            **first.__dict__,
            "signal_id": "allowed-after-timeout",
            "decision_at": start + timedelta(minutes=2),
            "order_type": "MARKET",
            "limit_price": None,
        }
    )
    bars = [
        _bar(start + timedelta(minutes=index), 102.0, 99.0, 101.0)
        for index in range(5)
    ]

    report = EventDrivenBacktest().run(
        [first, overlapping, after_timeout], {"BTCUSDT": bars}
    )

    assert [trade.signal_id for trade in report.trades] == ["allowed-after-timeout"]
    assert report.rejected_signals["entry_timeout"] == 1
    assert report.rejected_signals["averaging_down_or_overlapping_position"] == 1


def test_filled_position_without_exit_path_makes_simulation_incomplete():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal = SignalEvent(
        signal_id="filled-but-data-ended",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=start,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=1_000,
        stop_loss_bps=1_000,
        max_holding_sec=300,
    )
    later_signal = SignalEvent(
        **{
            **signal.__dict__,
            "signal_id": "must-not-trade-on-unknown-equity",
            "symbol": "ETHUSDT",
            "decision_at": start + timedelta(minutes=1),
            "feature_available_at": (start + timedelta(minutes=1),),
        }
    )

    report = EventDrivenBacktest().run(
        [signal, later_signal],
        {
            "BTCUSDT": [_bar(start, 101.0, 99.0, 100.5)],
            "ETHUSDT": [
                _bar(
                    start + timedelta(minutes=index),
                    102.0,
                    99.0,
                    101.0,
                    symbol="ETHUSDT",
                )
                for index in range(8)
            ],
        },
    )

    assert not report.trades
    assert report.rejected_signals["no_exit_observation"] == 1
    assert report.unresolved_position_count == 1
    assert report.simulation_complete is False
    assert report.execution_cost_evidence_complete is False


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


def test_event_backtest_passes_only_the_bounded_holding_path_to_labeler(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    signal_at = start + timedelta(days=2)
    signal = SignalEvent(
        signal_id="bounded-market-path",
        symbol="BTCUSDT",
        side="BUY",
        decision_at=signal_at,
        reference_price=100.0,
        lower_bound_net_edge=0.001,
        take_profit_bps=100,
        stop_loss_bps=100,
        max_holding_sec=180,
    )
    bars = [
        _bar(start + timedelta(minutes=index), 102.0, 99.0, 101.0)
        for index in range(5_000)
    ]
    observed_path_lengths: list[int] = []
    original = event_driven.build_triple_barrier_label

    def capture_path(spec, path, config):
        observed_path_lengths.append(len(path))
        return original(spec, path, config)

    monkeypatch.setattr(event_driven, "build_triple_barrier_label", capture_path)
    report = EventDrivenBacktest().run([signal], {"BTCUSDT": bars})

    assert len(report.trades) == 1
    assert observed_path_lengths and observed_path_lengths[0] <= 6
    assert observed_path_lengths[0] < len(bars)
