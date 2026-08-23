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
    assert label.mae > 0 and label.mfe > 0


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
    assert label.path_observations == 6
    assert label.exit_at == start + timedelta(minutes=5)
