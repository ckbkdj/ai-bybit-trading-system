from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from typing import Mapping, Sequence

import pandas as pd

from core.labels.triple_barrier import MarketBar
from core.training.bybit_pit_panel import BybitPITFeatureSource


ORDERBOOK_EXECUTION_FEATURES = (
    "orderbook_spread_bps",
    "orderbook_depth_usdt_l5",
)


def _covered_dates(bar: MarketBar) -> tuple[date, ...]:
    current = bar.open_time.date()
    # A close exactly at midnight belongs to the interval that just ended.
    last = (bar.close_time - timedelta(microseconds=1)).date()
    output: list[date] = []
    while current <= last:
        output.append(current)
        current += timedelta(days=1)
    return tuple(output)


def _completed_funding_days(
    source_evidence: Mapping[str, object],
) -> set[tuple[str, date]]:
    completed: set[tuple[str, date]] = set()
    for raw in source_evidence.get("historical_api_batches") or ():
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("data_kind")) != "funding" or str(raw.get("status")) != "completed":
            continue
        try:
            trading_date = date.fromisoformat(str(raw["trading_date"]))
        except (KeyError, ValueError):
            continue
        completed.add((str(raw.get("symbol", "")).upper(), trading_date))
    return completed


def enrich_market_bars_with_bybit_execution_pit(
    bars: Sequence[MarketBar],
    *,
    source: BybitPITFeatureSource,
    history: pd.DataFrame,
    source_evidence: Mapping[str, object],
) -> tuple[list[MarketBar], dict[str, object]]:
    """Attach direct PIT spread/depth and settled funding to OHLC price paths.

    OHLC remains the conservative barrier path.  Spread and depth are replaced
    only when an official order-book observation was already available at the
    bar open.  Funding is marked observed only for UTC days backed by completed
    official funding-history batches; ticker snapshots cannot establish the
    settled funding paid by a position.
    """

    if not bars:
        return [], {
            "bar_count": 0,
            "direct_open_spread_depth_bar_count": 0,
            "direct_close_spread_depth_bar_count": 0,
            "direct_spread_depth_bar_count": 0,
            "direct_funding_bar_count": 0,
            "fully_direct_execution_bar_count": 0,
            "fully_direct_execution_bar_ratio": 0.0,
        }
    ordered = list(bars)
    open_decisions = pd.DataFrame(
        {
            "symbol": [bar.symbol.upper() for bar in ordered],
            "decision_at": [bar.open_time for bar in ordered],
        }
    )
    close_decisions = pd.DataFrame(
        {
            "symbol": [bar.symbol.upper() for bar in ordered],
            "decision_at": [bar.close_time for bar in ordered],
        }
    )
    open_joined = source.join(
        open_decisions,
        names=ORDERBOOK_EXECUTION_FEATURES,
        history=history,
    )
    close_joined = source.join(
        close_decisions,
        names=ORDERBOOK_EXECUTION_FEATURES,
        history=history,
    )
    funding = history[
        (history["name"] == "funding_rate")
        & (history["source"] == "bybit.public.funding_history")
    ].copy()
    if not funding.empty:
        funding["symbol"] = funding["symbol"].astype(str).str.upper()
        funding["event_time"] = pd.to_datetime(
            funding["event_time"], utc=True, errors="raise"
        )
        funding["available_at"] = pd.to_datetime(
            funding["available_at"], utc=True, errors="raise"
        )
    funding_by_symbol_day: dict[tuple[str, date], list[tuple[pd.Timestamp, pd.Timestamp, float]]] = {}
    for raw in funding.itertuples(index=False):
        event_time = pd.Timestamp(raw.event_time)
        funding_by_symbol_day.setdefault(
            (str(raw.symbol).upper(), event_time.date()), []
        ).append(
            (event_time, pd.Timestamp(raw.available_at), float(raw.value))
        )
    completed_funding_days = _completed_funding_days(source_evidence)
    output: list[MarketBar] = []
    direct_open_spread_depth = 0
    direct_close_spread_depth = 0
    direct_spread_depth = 0
    direct_funding = 0
    fully_direct = 0
    for position, bar in enumerate(ordered):
        open_row = open_joined.iloc[position]
        close_row = close_joined.iloc[position]
        spread = open_row["orderbook_spread_bps"]
        depth = open_row["orderbook_depth_usdt_l5"]
        close_spread = close_row["orderbook_spread_bps"]
        close_depth = close_row["orderbook_depth_usdt_l5"]
        spread_observed = pd.notna(spread)
        depth_observed = pd.notna(depth)
        close_spread_observed = pd.notna(close_spread)
        close_depth_observed = pd.notna(close_depth)
        required_funding_days = _covered_dates(bar)
        funding_observed = bool(required_funding_days) and all(
            (bar.symbol.upper(), item) in completed_funding_days
            for item in required_funding_days
        )
        bar_funding = [
            item
            for trading_day in required_funding_days
            for item in funding_by_symbol_day.get(
                (bar.symbol.upper(), trading_day), ()
            )
            if item[0] > pd.Timestamp(bar.open_time)
            and item[0] <= pd.Timestamp(bar.close_time)
        ]
        funding_bps = (
            sum(item[2] for item in bar_funding) * 10_000.0
            if funding_observed
            else bar.funding_bps
        )
        available_at = bar.available_at
        if funding_observed and bar_funding:
            latest_funding_available = max(item[1] for item in bar_funding).to_pydatetime()
            available_at = max(available_at, latest_funding_available)
        enriched = replace(
            bar,
            available_at=available_at,
            spread_bps=float(spread) if spread_observed else bar.spread_bps,
            depth_usdt=float(depth) if depth_observed else bar.depth_usdt,
            funding_bps=funding_bps,
            spread_source=(
                "bybit.public.orderbook" if spread_observed else bar.spread_source
            ),
            depth_source=(
                "bybit.public.orderbook" if depth_observed else bar.depth_source
            ),
            funding_source=(
                "bybit.public.funding_history"
                if funding_observed
                else bar.funding_source
            ),
            spread_observed=spread_observed,
            depth_observed=depth_observed,
            funding_observed=funding_observed,
            close_spread_bps=(
                float(close_spread) if close_spread_observed else None
            ),
            close_depth_usdt=(
                float(close_depth) if close_depth_observed else None
            ),
            close_spread_source=(
                "bybit.public.orderbook"
                if close_spread_observed
                else None
            ),
            close_depth_source=(
                "bybit.public.orderbook"
                if close_depth_observed
                else None
            ),
            close_spread_observed=close_spread_observed,
            close_depth_observed=close_depth_observed,
        )
        output.append(enriched)
        spread_depth_complete = bool(
            spread_observed
            and depth_observed
            and close_spread_observed
            and close_depth_observed
        )
        direct_open_spread_depth += int(spread_observed and depth_observed)
        direct_close_spread_depth += int(
            close_spread_observed and close_depth_observed
        )
        direct_spread_depth += int(spread_depth_complete)
        direct_funding += int(funding_observed)
        fully_direct += int(spread_depth_complete and funding_observed)
    return output, {
        "bar_count": len(output),
        "direct_open_spread_depth_bar_count": direct_open_spread_depth,
        "direct_close_spread_depth_bar_count": direct_close_spread_depth,
        "direct_spread_depth_bar_count": direct_spread_depth,
        "direct_funding_bar_count": direct_funding,
        "fully_direct_execution_bar_count": fully_direct,
        "fully_direct_execution_bar_ratio": fully_direct / len(output),
        "spread_depth_source": "bybit.public.orderbook",
        "funding_source": "bybit.public.funding_history",
        "orderbook_join_policy": "separate latest available_at snapshots at or before bar open and bar close, each with registry staleness cutoff",
        "funding_policy": "settled funding events only on completed official funding-history UTC days",
        "ohlc_role": "barrier path only; never execution spread/depth evidence",
    }


__all__ = (
    "ORDERBOOK_EXECUTION_FEATURES",
    "enrich_market_bars_with_bybit_execution_pit",
)
