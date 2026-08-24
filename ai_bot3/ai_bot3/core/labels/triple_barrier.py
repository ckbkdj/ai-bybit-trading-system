from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Literal, Sequence


Side = Literal["BUY", "SELL"]
ExitReason = Literal[
    "TAKE_PROFIT",
    "STOP_LOSS",
    "MAX_HOLDING",
    "ENTRY_TIMEOUT",
    "UNFILLED",
    "NO_EXIT_OBSERVATION",
]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class MarketBar:
    """A bar whose full OHLC path is usable only at ``available_at``.

    ``open_time`` is the earliest instant at which the open price can be used
    for a simulated order.  High/low/close are not considered observed until
    ``available_at``.  This split prevents the label builder from leaking a
    completed candle into the entry decision.
    """

    symbol: str
    open_time: datetime
    close_time: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    spread_bps: float | None = None
    depth_usdt: float | None = None
    volatility_bps: float | None = None
    funding_bps: float = 0.0
    spread_source: str = "ohlcv_proxy"
    depth_source: str = "ohlcv_proxy"
    funding_source: str = "zero_proxy"
    spread_observed: bool = False
    depth_observed: bool = False
    funding_observed: bool = False

    def __post_init__(self) -> None:
        open_time = _utc(self.open_time)
        close_time = _utc(self.close_time)
        available_at = _utc(self.available_at)
        if not (open_time < close_time <= available_at):
            raise ValueError("bar timestamps must satisfy open < close <= available_at")
        prices = (self.open, self.high, self.low, self.close)
        if any(not math.isfinite(value) or value <= 0 for value in prices):
            raise ValueError("bar prices must be finite and positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC envelope")
        if self.volume < 0:
            raise ValueError("volume cannot be negative")
        if not all(
            str(value).strip()
            for value in (self.spread_source, self.depth_source, self.funding_source)
        ):
            raise ValueError("execution cost provenance sources cannot be empty")


@dataclass(frozen=True)
class EntrySpec:
    symbol: str
    side: Side
    signal_at: datetime
    reference_price: float
    quantity: float
    take_profit_bps: float
    stop_loss_bps: float
    max_holding_sec: int
    feature_available_at: tuple[datetime, ...] = ()
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price: float | None = None
    max_wait_sec: int = 90
    maker_entry: bool = False
    maker_exit: bool = False

    def __post_init__(self) -> None:
        signal_at = _utc(self.signal_at)
        if any(_utc(value) > signal_at for value in self.feature_available_at):
            raise ValueError("PIT violation: a decision feature was not available at signal_at")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if self.reference_price <= 0 or self.quantity <= 0:
            raise ValueError("reference price and quantity must be positive")
        if min(self.take_profit_bps, self.stop_loss_bps) <= 0:
            raise ValueError("take-profit and stop-loss barriers must be positive")
        if self.max_holding_sec <= 0 or self.max_wait_sec <= 0:
            raise ValueError("holding and entry timeout must be positive")
        if self.order_type == "LIMIT" and (self.limit_price is None or self.limit_price <= 0):
            raise ValueError("LIMIT entry requires a positive limit_price")


@dataclass(frozen=True)
class TripleBarrierConfig:
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 5.5
    base_slippage_bps: float = 1.0
    volatility_slippage_multiplier: float = 0.05
    impact_bps_at_full_depth: float = 4.0
    default_spread_bps: float = 4.0
    default_depth_usdt: float = 25_000.0
    missing_depth_fill_fraction: float = 0.50
    latency_ms: int = 250
    stop_first_when_same_bar: bool = True
    slippage_stress_multiplier: float = 1.0
    funding_stress_multiplier: float = 1.0

    def __post_init__(self) -> None:
        if min(self.maker_fee_bps, self.taker_fee_bps, self.base_slippage_bps) < 0:
            raise ValueError("cost parameters cannot be negative")
        if self.default_depth_usdt <= 0 or not 0 < self.missing_depth_fill_fraction <= 1:
            raise ValueError("depth and missing-depth fill fraction are invalid")
        if self.latency_ms < 0:
            raise ValueError("latency cannot be negative")
        if self.slippage_stress_multiplier < 1:
            raise ValueError("slippage stress cannot make costs more optimistic")
        if self.funding_stress_multiplier < 1:
            raise ValueError("funding stress cannot make costs more optimistic")


@dataclass(frozen=True)
class TripleBarrierLabel:
    label_id: str
    symbol: str
    side: Side
    signal_at: datetime
    entry_fill_at: datetime | None
    exit_at: datetime | None
    label_available_at: datetime
    entry_reference_price: float
    entry_fill_price: float | None
    exit_reference_price: float | None
    exit_fill_price: float | None
    requested_quantity: float
    filled_quantity: float
    fill_probability: float
    fill_fraction: float
    partial_fill: bool
    gross_return: float | None
    net_return: float | None
    fee_return: float | None
    slippage_return: float | None
    funding_return: float | None
    mae: float
    mfe: float
    exit_reason: ExitReason
    max_holding_sec: int
    path_observations: int
    pit_valid: bool
    outcome_complete: bool
    execution_cost_evidence_complete: bool
    entry_spread_source: str
    entry_depth_source: str
    exit_spread_source: str
    exit_depth_source: str
    funding_source: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key in ("signal_at", "entry_fill_at", "exit_at", "label_available_at"):
            value = payload[key]
            payload[key] = value.isoformat().replace("+00:00", "Z") if value else None
        return payload


def _slippage_bps(bar: MarketBar, notional: float, config: TripleBarrierConfig) -> float:
    spread = bar.spread_bps if bar.spread_bps is not None else config.default_spread_bps
    depth = bar.depth_usdt if bar.depth_usdt is not None else config.default_depth_usdt
    volatility = bar.volatility_bps if bar.volatility_bps is not None else 0.0
    impact = config.impact_bps_at_full_depth * min(3.0, notional / max(depth, 1.0))
    base = (
        config.base_slippage_bps
        + spread / 2.0
        + impact
        + volatility * config.volatility_slippage_multiplier
    )
    return max(0.0, base * config.slippage_stress_multiplier)


def _fill_probability(spec: EntrySpec, bar: MarketBar) -> float:
    notional = spec.reference_price * spec.quantity
    depth = bar.depth_usdt
    if depth is None:
        depth_component = 0.5
    else:
        depth_component = math.exp(-notional / max(depth, 1.0))
    spread = bar.spread_bps if bar.spread_bps is not None else 8.0
    spread_component = math.exp(-max(0.0, spread - 2.0) / 30.0)
    if spec.order_type == "MARKET":
        return max(0.05, min(1.0, 0.7 + 0.3 * depth_component))
    assert spec.limit_price is not None
    touched = bar.low <= spec.limit_price if spec.side == "BUY" else bar.high >= spec.limit_price
    if not touched:
        return 0.0
    return max(0.01, min(1.0, depth_component * spread_component))


def _entry_bar(spec: EntrySpec, bars: Sequence[MarketBar], config: TripleBarrierConfig) -> tuple[MarketBar | None, float]:
    activation = _utc(spec.signal_at) + timedelta(milliseconds=config.latency_ms)
    timeout = _utc(spec.signal_at) + timedelta(seconds=spec.max_wait_sec)
    for bar in bars:
        if bar.symbol.upper() != spec.symbol.upper() or bar.close_time <= activation:
            continue
        if bar.open_time > timeout:
            break
        if spec.order_type == "LIMIT" and (
            bar.open_time < activation or bar.close_time > timeout
        ):
            # A completed OHLC bar cannot establish whether a limit touch in a
            # straddling bar happened after activation and before cancellation.
            # Treat that ambiguity as no fill instead of leaking the full bar.
            continue
        probability = _fill_probability(spec, bar)
        if probability > 0:
            return bar, probability
    return None, 0.0


def _timeout_available_at(
    spec: EntrySpec,
    bars: Sequence[MarketBar],
) -> datetime:
    """Earliest observation that can establish an entry timeout."""

    timeout = _utc(spec.signal_at) + timedelta(seconds=spec.max_wait_sec)
    for bar in bars:
        if bar.close_time >= timeout:
            return max(timeout, bar.available_at)
    return max(timeout, bars[-1].available_at if bars else timeout)


def build_triple_barrier_label(
    spec: EntrySpec,
    bars: Iterable[MarketBar],
    config: TripleBarrierConfig | None = None,
) -> TripleBarrierLabel:
    """Build an execution-aware label from future market events.

    The same-bar TP/SL ambiguity is resolved against the strategy by default
    (stop first).  That convention is intentionally conservative and is
    recorded by deterministic code rather than silently choosing the target.
    """

    cfg = config or TripleBarrierConfig()
    ordered = sorted(bars, key=lambda item: (item.open_time, item.available_at))
    if not ordered:
        available = _utc(spec.signal_at)
        return _empty_label(spec, available, "UNFILLED")
    if any(bar.symbol.upper() != spec.symbol.upper() for bar in ordered):
        raise ValueError("all bars must match EntrySpec.symbol")
    entry_bar, probability = _entry_bar(spec, ordered, cfg)
    if entry_bar is None:
        available = _timeout_available_at(spec, ordered)
        return _empty_label(spec, available, "ENTRY_TIMEOUT")

    notional = spec.reference_price * spec.quantity
    if entry_bar.depth_usdt is None:
        liquidity_fraction = cfg.missing_depth_fill_fraction
    else:
        liquidity_fraction = min(1.0, entry_bar.depth_usdt / max(notional, 1.0))
    fill_fraction = min(1.0, probability, liquidity_fraction)
    filled_quantity = spec.quantity * fill_fraction
    if filled_quantity <= 0:
        return _empty_label(spec, entry_bar.available_at, "UNFILLED")

    direction = 1.0 if spec.side == "BUY" else -1.0
    activation = _utc(spec.signal_at) + timedelta(milliseconds=cfg.latency_ms)
    # A market decision already carries a PIT-safe reference price.  Reusing a
    # candle open from before a mid-bar activation would be lookahead/mispricing.
    entry_reference = spec.limit_price if spec.order_type == "LIMIT" else spec.reference_price
    # A limit touch has no event timestamp in OHLC data.  It is only established
    # when the completed bar becomes known, so its fill time is conservative.
    entry_fill_at = entry_bar.close_time if spec.order_type == "LIMIT" else activation
    entry_slippage_bps = _slippage_bps(entry_bar, notional * fill_fraction, cfg)
    entry_fill = entry_reference * (1.0 + direction * entry_slippage_bps / 10_000.0)
    take_profit = entry_fill * (1.0 + direction * spec.take_profit_bps / 10_000.0)
    stop_loss = entry_fill * (1.0 - direction * spec.stop_loss_bps / 10_000.0)
    expiry = entry_fill_at + timedelta(seconds=spec.max_holding_sec)

    # Only completed bars at or before expiry may trigger a barrier.  Using the
    # high/low/close of a bar that straddles max holding would incorporate price
    # action after the position should already be closed.
    path = [
        bar
        for bar in ordered
        if bar.close_time > entry_fill_at and bar.close_time <= expiry
    ]
    exit_bar: MarketBar | None = None
    exit_reference: float | None = None
    exit_at: datetime | None = None
    exit_reason: ExitReason = "NO_EXIT_OBSERVATION"
    mfe = 0.0
    mae = 0.0
    for bar in path:
        entry_bar_ambiguous = (
            bar is entry_bar
            and bar.open_time < entry_fill_at < bar.close_time
        )
        favorable = (
            bar.high / entry_fill - 1.0
            if direction > 0
            else 1.0 - bar.low / entry_fill
        )
        adverse = (
            1.0 - bar.low / entry_fill
            if direction > 0
            else bar.high / entry_fill - 1.0
        )
        # The adverse extreme is retained as a conservative same-bar bound. A
        # favorable extreme cannot be credited because it may predate the fill.
        if entry_bar_ambiguous:
            favorable = 0.0
        mfe = max(mfe, favorable)
        mae = max(mae, adverse)
        stop_hit = bar.low <= stop_loss if direction > 0 else bar.high >= stop_loss
        target_hit = (
            False
            if entry_bar_ambiguous
            else (bar.high >= take_profit if direction > 0 else bar.low <= take_profit)
        )
        if stop_hit and target_hit:
            exit_reason = "STOP_LOSS" if cfg.stop_first_when_same_bar else "TAKE_PROFIT"
            if cfg.stop_first_when_same_bar and bar.open_time >= entry_fill_at and (
                (direction > 0 and bar.open < stop_loss)
                or (direction < 0 and bar.open > stop_loss)
            ):
                exit_reference = bar.open
            else:
                exit_reference = stop_loss if cfg.stop_first_when_same_bar else take_profit
        elif stop_hit:
            exit_reason = "STOP_LOSS"
            # A stop-market order cannot fill at the trigger when a later bar
            # opens through it. The entry bar open may predate a mid-bar fill,
            # so it is intentionally excluded from gap logic.
            if bar.open_time >= entry_fill_at and (
                (direction > 0 and bar.open < stop_loss)
                or (direction < 0 and bar.open > stop_loss)
            ):
                exit_reference = bar.open
            else:
                exit_reference = stop_loss
        elif target_hit:
            exit_reason, exit_reference = "TAKE_PROFIT", take_profit
        if exit_reference is not None:
            exit_bar = bar
            exit_at = bar.close_time
            break

    source_covers_expiry = bool(ordered and ordered[-1].close_time >= expiry)
    if exit_bar is None and path and source_covers_expiry:
        exit_bar = path[-1]
        exit_reference = exit_bar.close
        exit_reason = "MAX_HOLDING"
        # The exact expiry can fall inside the next OHLC bar (for example
        # after a 250 ms entry latency).  The last completed close is a real
        # observable price; pairing it with the later expiry timestamp would
        # invent an execution at a time for which no price was observed.
        exit_at = exit_bar.close_time
    if exit_bar is None or exit_reference is None or exit_at is None:
        return _incomplete_filled_label(
            spec,
            entry_bar=entry_bar,
            entry_fill_at=entry_fill_at,
            entry_reference=float(entry_reference),
            entry_fill=float(entry_fill),
            filled_quantity=float(filled_quantity),
            fill_probability=float(probability),
            fill_fraction=float(fill_fraction),
            path=path,
            label_available_at=max(bar.available_at for bar in ordered),
            mae=float(max(0.0, mae)),
            mfe=float(max(0.0, mfe)),
        )

    exit_slippage_bps = _slippage_bps(exit_bar, notional * fill_fraction, cfg)
    exit_fill = exit_reference * (1.0 - direction * exit_slippage_bps / 10_000.0)
    gross_return = direction * (exit_reference / entry_reference - 1.0)
    realised_return = direction * (exit_fill / entry_fill - 1.0)
    entry_fee = cfg.maker_fee_bps if spec.maker_entry else cfg.taker_fee_bps
    exit_fee = cfg.maker_fee_bps if spec.maker_exit else cfg.taker_fee_bps
    fee_return = (entry_fee + exit_fee) / 10_000.0
    slippage_return = max(0.0, gross_return - realised_return)
    funding_bps = sum(bar.funding_bps for bar in path if bar.close_time <= exit_bar.close_time)
    funding_return = direction * funding_bps / 10_000.0
    # A cost stress must never turn known funding income into larger income.
    # Positive values are payments and are multiplied; negative values are
    # receipts and are reduced toward zero at 2x before becoming a cost under
    # more extreme stresses.
    funding_return += (
        cfg.funding_stress_multiplier - 1.0
    ) * abs(funding_return)
    net_return = gross_return - fee_return - slippage_return - funding_return
    funding_path = [bar for bar in path if bar.close_time <= exit_bar.close_time]
    execution_cost_evidence_complete = bool(
        entry_bar.spread_observed
        and entry_bar.depth_observed
        and exit_bar.spread_observed
        and exit_bar.depth_observed
        and funding_path
        and all(bar.funding_observed for bar in funding_path)
    )
    funding_sources = sorted({bar.funding_source for bar in funding_path})
    label_id = hashlib.sha256(
        f"{spec.symbol}|{spec.side}|{_utc(spec.signal_at).isoformat()}|{entry_fill_at.isoformat()}|{exit_at.isoformat()}".encode()
    ).hexdigest()[:32]
    return TripleBarrierLabel(
        label_id=label_id,
        symbol=spec.symbol.upper(),
        side=spec.side,
        signal_at=_utc(spec.signal_at),
        entry_fill_at=entry_fill_at,
        exit_at=exit_at,
        label_available_at=max(exit_bar.available_at, exit_at),
        entry_reference_price=float(entry_reference),
        entry_fill_price=float(entry_fill),
        exit_reference_price=float(exit_reference),
        exit_fill_price=float(exit_fill),
        requested_quantity=float(spec.quantity),
        filled_quantity=float(filled_quantity),
        fill_probability=float(probability),
        fill_fraction=float(fill_fraction),
        partial_fill=fill_fraction < 1.0 - 1e-12,
        gross_return=float(gross_return),
        net_return=float(net_return),
        fee_return=float(fee_return),
        slippage_return=float(slippage_return),
        funding_return=float(funding_return),
        mae=float(max(0.0, mae)),
        mfe=float(max(0.0, mfe)),
        exit_reason=exit_reason,
        max_holding_sec=spec.max_holding_sec,
        path_observations=len(path),
        pit_valid=True,
        outcome_complete=True,
        execution_cost_evidence_complete=execution_cost_evidence_complete,
        entry_spread_source=entry_bar.spread_source,
        entry_depth_source=entry_bar.depth_source,
        exit_spread_source=exit_bar.spread_source,
        exit_depth_source=exit_bar.depth_source,
        funding_source="+".join(funding_sources) if funding_sources else "unobserved",
    )


def _empty_label(
    spec: EntrySpec,
    available_at: datetime,
    reason: ExitReason,
    *,
    probability: float = 0.0,
) -> TripleBarrierLabel:
    label_id = hashlib.sha256(
        f"{spec.symbol}|{spec.side}|{_utc(spec.signal_at).isoformat()}|{reason}".encode()
    ).hexdigest()[:32]
    return TripleBarrierLabel(
        label_id=label_id,
        symbol=spec.symbol.upper(),
        side=spec.side,
        signal_at=_utc(spec.signal_at),
        entry_fill_at=None,
        exit_at=None,
        label_available_at=_utc(available_at),
        entry_reference_price=float(spec.reference_price),
        entry_fill_price=None,
        exit_reference_price=None,
        exit_fill_price=None,
        requested_quantity=float(spec.quantity),
        filled_quantity=0.0,
        fill_probability=float(probability),
        fill_fraction=0.0,
        partial_fill=False,
        gross_return=0.0,
        net_return=0.0,
        fee_return=0.0,
        slippage_return=0.0,
        funding_return=0.0,
        mae=0.0,
        mfe=0.0,
        exit_reason=reason,
        max_holding_sec=spec.max_holding_sec,
        path_observations=0,
        pit_valid=True,
        outcome_complete=True,
        execution_cost_evidence_complete=False,
        entry_spread_source="unfilled",
        entry_depth_source="unfilled",
        exit_spread_source="unfilled",
        exit_depth_source="unfilled",
        funding_source="unfilled",
    )


def _incomplete_filled_label(
    spec: EntrySpec,
    *,
    entry_bar: MarketBar,
    entry_fill_at: datetime,
    entry_reference: float,
    entry_fill: float,
    filled_quantity: float,
    fill_probability: float,
    fill_fraction: float,
    path: Sequence[MarketBar],
    label_available_at: datetime,
    mae: float,
    mfe: float,
) -> TripleBarrierLabel:
    """Preserve a known fill without inventing a zero-return exit."""

    funding_sources = sorted({bar.funding_source for bar in path})
    label_id = hashlib.sha256(
        f"{spec.symbol}|{spec.side}|{_utc(spec.signal_at).isoformat()}|"
        f"{entry_fill_at.isoformat()}|NO_EXIT_OBSERVATION".encode()
    ).hexdigest()[:32]
    return TripleBarrierLabel(
        label_id=label_id,
        symbol=spec.symbol.upper(),
        side=spec.side,
        signal_at=_utc(spec.signal_at),
        entry_fill_at=entry_fill_at,
        exit_at=None,
        label_available_at=_utc(label_available_at),
        entry_reference_price=entry_reference,
        entry_fill_price=entry_fill,
        exit_reference_price=None,
        exit_fill_price=None,
        requested_quantity=float(spec.quantity),
        filled_quantity=filled_quantity,
        fill_probability=fill_probability,
        fill_fraction=fill_fraction,
        partial_fill=fill_fraction < 1.0 - 1e-12,
        gross_return=None,
        net_return=None,
        fee_return=None,
        slippage_return=None,
        funding_return=None,
        mae=mae,
        mfe=mfe,
        exit_reason="NO_EXIT_OBSERVATION",
        max_holding_sec=spec.max_holding_sec,
        path_observations=len(path),
        pit_valid=True,
        outcome_complete=False,
        execution_cost_evidence_complete=False,
        entry_spread_source=entry_bar.spread_source,
        entry_depth_source=entry_bar.depth_source,
        exit_spread_source="unobserved",
        exit_depth_source="unobserved",
        funding_source=(
            "+".join(funding_sources) if funding_sources else "unobserved"
        ),
    )


__all__: Sequence[str] = (
    "EntrySpec",
    "MarketBar",
    "TripleBarrierConfig",
    "TripleBarrierLabel",
    "build_triple_barrier_label",
)
