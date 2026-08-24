from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.labels.triple_barrier import EntrySpec, MarketBar, TripleBarrierConfig, build_triple_barrier_label


UTC = timezone.utc


def _bar(start: datetime, *, high: float, low: float, close: float, depth: float = 50_000.0) -> MarketBar:
    return MarketBar(
        symbol="BTCUSDT",
        open_time=start,
        close_time=start + timedelta(minutes=1),
        available_at=start + timedelta(minutes=1),
        open=100.0,
        high=high,
        low=low,
        close=close,
        volume=1_000.0,
        spread_bps=4.0,
        depth_usdt=depth,
        volatility_bps=20.0,
    )


def test_triple_barrier_is_pit_safe_costed_and_stop_first():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start,
        reference_price=100.0,
        quantity=1.0,
        take_profit_bps=100.0,
        stop_loss_bps=100.0,
        max_holding_sec=300,
        feature_available_at=(start,),
    )
    label = build_triple_barrier_label(spec, [_bar(start, high=102.0, low=98.0, close=101.0)])
    assert label.exit_reason == "STOP_LOSS"
    assert label.label_available_at > label.signal_at
    assert label.pit_valid is True
    assert label.fee_return > 0
    assert label.slippage_return > 0
    assert label.net_return < label.gross_return
    assert label.mae > 0
    assert label.mfe == 0  # same-bar favorable extreme predates an unknown fill time


def test_triple_barrier_records_partial_fill_and_rejects_future_feature():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="PIT violation"):
        EntrySpec(
            symbol="BTCUSDT",
            side="BUY",
            signal_at=start,
            reference_price=100.0,
            quantity=100.0,
            take_profit_bps=100.0,
            stop_loss_bps=100.0,
            max_holding_sec=300,
            feature_available_at=(start + timedelta(seconds=1),),
        )

    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start,
        reference_price=100.0,
        quantity=100.0,
        take_profit_bps=100.0,
        stop_loss_bps=100.0,
        max_holding_sec=300,
    )
    label = build_triple_barrier_label(
        spec,
        [_bar(start, high=101.5, low=99.5, close=101.0, depth=1_000.0)],
        TripleBarrierConfig(latency_ms=0),
    )
    assert 0 < label.fill_probability < 1
    assert 0 < label.fill_fraction < 1
    assert label.partial_fill is True
    assert label.filled_quantity < label.requested_quantity


def test_triple_barrier_evaluates_every_observation_through_max_holding():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start,
        reference_price=100.0,
        quantity=1.0,
        take_profit_bps=500.0,
        stop_loss_bps=500.0,
        max_holding_sec=300,
        max_wait_sec=30,
    )
    bars = [
        _bar(
            start + timedelta(minutes=index),
            high=100.2 + index * 0.05,
            low=99.8,
            close=100.0 + index * 0.02,
        )
        for index in range(6)
    ]
    label = build_triple_barrier_label(spec, bars, TripleBarrierConfig(latency_ms=0))
    assert label.exit_reason == "MAX_HOLDING"
    assert label.path_observations == 5
    assert label.exit_at == start + timedelta(minutes=5)


def test_incomplete_holding_path_preserves_fill_and_never_invents_zero_return():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start,
        reference_price=100.0,
        quantity=10.0,
        take_profit_bps=1_000.0,
        stop_loss_bps=1_000.0,
        max_holding_sec=300,
    )

    label = build_triple_barrier_label(
        spec,
        [_bar(start, high=101.0, low=99.0, close=100.5, depth=500.0)],
        TripleBarrierConfig(latency_ms=0),
    )

    assert label.exit_reason == "NO_EXIT_OBSERVATION"
    assert label.outcome_complete is False
    assert label.entry_fill_at == start
    assert label.entry_fill_price is not None
    assert 0 < label.filled_quantity < label.requested_quantity
    assert label.partial_fill is True
    assert label.exit_at is None
    assert label.net_return is None
    assert label.gross_return is None
    assert label.execution_cost_evidence_complete is False


def test_mid_bar_activation_never_credits_a_pre_fill_target():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start,
        reference_price=100.0,
        quantity=1.0,
        take_profit_bps=100.0,
        stop_loss_bps=100.0,
        max_holding_sec=180,
    )
    bars = [
        _bar(start, high=102.0, low=100.0, close=100.5),
        _bar(start + timedelta(minutes=1), high=102.0, low=100.0, close=101.5),
    ]
    label = build_triple_barrier_label(spec, bars)
    assert label.exit_reason == "TAKE_PROFIT"
    assert label.path_observations == 2


@pytest.mark.parametrize(
    ("side", "expected_mae", "expected_mfe"),
    [("BUY", 0.50, 0.50), ("SELL", 0.50, 0.50)],
)
def test_mae_mfe_use_linear_not_reciprocal_returns(side, expected_mae, expected_mfe):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    bar = MarketBar(
        symbol="BTCUSDT",
        open_time=start,
        close_time=start + timedelta(minutes=1),
        available_at=start + timedelta(minutes=1),
        open=100.0,
        high=150.0,
        low=50.0,
        close=100.0,
        volume=1_000.0,
        spread_bps=0.0,
        depth_usdt=1_000_000.0,
        volatility_bps=0.0,
    )
    spec = EntrySpec(
        symbol="BTCUSDT",
        side=side,
        signal_at=start,
        reference_price=100.0,
        quantity=1.0,
        take_profit_bps=10_000.0,
        stop_loss_bps=10_000.0,
        max_holding_sec=60,
    )
    label = build_triple_barrier_label(
        spec,
        [bar],
        TripleBarrierConfig(
            maker_fee_bps=0.0,
            taker_fee_bps=0.0,
            base_slippage_bps=0.0,
            volatility_slippage_multiplier=0.0,
            impact_bps_at_full_depth=0.0,
            default_spread_bps=0.0,
            latency_ms=0,
        ),
    )
    assert label.mae == pytest.approx(expected_mae)
    assert label.mfe == pytest.approx(expected_mfe)


def test_limit_touch_in_activation_timeout_straddling_bar_fails_closed():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    spec = EntrySpec(
        symbol="BTCUSDT",
        side="BUY",
        signal_at=start + timedelta(seconds=30),
        reference_price=100.0,
        quantity=1.0,
        take_profit_bps=100.0,
        stop_loss_bps=100.0,
        max_holding_sec=180,
        order_type="LIMIT",
        limit_price=99.0,
        max_wait_sec=20,
    )
    bars = [
        _bar(start, high=101.0, low=98.0, close=100.0),
        _bar(start + timedelta(days=10), high=101.0, low=98.0, close=100.0),
    ]
    label = build_triple_barrier_label(spec, bars, TripleBarrierConfig(latency_ms=0))
    assert label.exit_reason == "ENTRY_TIMEOUT"
    assert label.entry_fill_at is None
    assert label.label_available_at == start + timedelta(minutes=1)
