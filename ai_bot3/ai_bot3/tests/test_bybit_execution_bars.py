from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.labels.triple_barrier import MarketBar
from core.training.bybit_execution_bars import (
    enrich_market_bars_with_bybit_execution_pit,
)


UTC = timezone.utc


class _DirectOrderbookSource:
    def join(self, decisions, *, names, history):
        output = decisions.copy()
        output["orderbook_spread_bps"] = 1.5
        output["orderbook_depth_usdt_l5"] = 2_000_000.0
        output["orderbook_spread_bps__available_at"] = output[
            "decision_at"
        ] - pd.Timedelta(seconds=1)
        output["orderbook_depth_usdt_l5__available_at"] = output[
            "decision_at"
        ] - pd.Timedelta(seconds=1)
        return output


def _bar(start: datetime) -> MarketBar:
    return MarketBar(
        symbol="BTCUSDT",
        open_time=start,
        close_time=start + timedelta(minutes=2),
        available_at=start + timedelta(minutes=2),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1_000.0,
        spread_bps=9.0,
        depth_usdt=10_000.0,
    )


def _funding_history(event_time: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "BTCUSDT",
                "name": "funding_rate",
                "value": 0.0001,
                "event_time": event_time,
                "available_at": event_time + timedelta(seconds=60),
                "source": "bybit.public.funding_history",
            }
        ]
    )


def _funding_evidence(trading_date: str) -> dict[str, object]:
    return {
        "historical_api_batches": [
            {
                "data_kind": "funding",
                "status": "completed",
                "symbol": "BTCUSDT",
                "trading_date": trading_date,
            }
        ]
    }


def test_execution_bar_enrichment_uses_direct_orderbook_and_settled_funding():
    start = datetime(2026, 1, 1, 7, 59, tzinfo=UTC)
    bars, evidence = enrich_market_bars_with_bybit_execution_pit(
        [_bar(start)],
        source=_DirectOrderbookSource(),
        history=_funding_history(start + timedelta(minutes=1)),
        source_evidence=_funding_evidence("2026-01-01"),
    )
    assert len(bars) == 1
    bar = bars[0]
    assert bar.spread_bps == 1.5
    assert bar.depth_usdt == 2_000_000.0
    assert bar.funding_bps == 1.0
    assert bar.spread_observed and bar.depth_observed and bar.funding_observed
    assert bar.spread_source == "bybit.public.orderbook"
    assert bar.funding_source == "bybit.public.funding_history"
    assert evidence["fully_direct_execution_bar_ratio"] == 1.0


def test_execution_bar_enrichment_does_not_infer_funding_coverage_from_a_row():
    start = datetime(2026, 1, 1, 7, 59, tzinfo=UTC)
    bars, evidence = enrich_market_bars_with_bybit_execution_pit(
        [_bar(start)],
        source=_DirectOrderbookSource(),
        history=_funding_history(start + timedelta(minutes=1)),
        source_evidence={"historical_api_batches": []},
    )
    bar = bars[0]
    assert bar.spread_observed and bar.depth_observed
    assert bar.funding_observed is False
    assert bar.funding_bps == 0.0
    assert bar.funding_source == "zero_proxy"
    assert evidence["fully_direct_execution_bar_count"] == 0
