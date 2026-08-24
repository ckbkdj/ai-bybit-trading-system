from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

from core.labels.triple_barrier import (
    EntrySpec,
    MarketBar,
    TripleBarrierConfig,
    build_triple_barrier_label,
)


@dataclass(frozen=True)
class SignalEvent:
    signal_id: str
    symbol: str
    side: str
    decision_at: datetime
    reference_price: float
    lower_bound_net_edge: float
    take_profit_bps: float
    stop_loss_bps: float
    max_holding_sec: int
    feature_available_at: tuple[datetime, ...] = ()
    requested_notional_usdt: float | None = None
    order_type: str = "MARKET"
    limit_price: float | None = None
    max_wait_sec: int = 90
    maker_entry: bool = False
    maker_exit: bool = False
    regime: str = "unknown"
    market_key: str | None = None


@dataclass(frozen=True)
class BacktestConfig:
    initial_equity_usdt: float = 100_000.0
    risk_per_trade: float = 0.0025
    daily_loss_limit: float = 0.0050
    weekly_loss_limit: float = 0.0150
    equity_drawdown_limit: float = 0.03
    leverage_cap: float = 2.0
    max_gross_exposure: float = 1.0
    funding_risk_buffer_bps_per_8h: float = 10.0
    no_averaging_down: bool = True
    no_martingale: bool = True
    require_stop: bool = True
    require_positive_lower_bound_edge: bool = True

    def __post_init__(self) -> None:
        if self.initial_equity_usdt <= 0:
            raise ValueError("initial equity must be positive")
        if not 0 < self.risk_per_trade <= 0.0025:
            raise ValueError("risk_per_trade cannot exceed 0.25%")
        if not 0 < self.daily_loss_limit <= 0.005:
            raise ValueError("daily loss limit cannot exceed 0.50%")
        if not 0 < self.weekly_loss_limit <= 0.015:
            raise ValueError("weekly loss limit cannot exceed 1.50%")
        if not 0 < self.equity_drawdown_limit <= 0.03:
            raise ValueError("equity drawdown limit cannot exceed 3%")
        if not 0 < self.leverage_cap <= 2:
            raise ValueError("leverage cannot exceed 2x")
        if not 0 < self.max_gross_exposure <= self.leverage_cap:
            raise ValueError("gross exposure must fit inside the leverage cap")
        if self.funding_risk_buffer_bps_per_8h < 10.0:
            raise ValueError("funding risk buffer cannot be below 10 bps per 8h")


@dataclass(frozen=True)
class TradeRecord:
    signal_id: str
    symbol: str
    side: str
    decision_at: datetime
    entry_at: datetime
    exit_at: datetime
    month: str
    regime: str
    notional_usdt: float
    gross_pnl: float
    net_pnl: float
    gross_return: float
    net_return: float
    fee_cost: float
    entry_fee_cost: float
    slippage_cost: float
    funding_cost: float
    mae: float
    mfe: float
    fill_probability: float
    fill_fraction: float
    partial_fill: bool
    exit_reason: str
    cancel_fill_race: bool
    entry_fill_price: float
    exit_fill_price: float
    filled_quantity: float
    risk_budget_usdt: float
    pretrade_risk_loss_bps: float
    market_key: str
    execution_cost_evidence_complete: bool
    entry_spread_source: str
    entry_depth_source: str
    exit_spread_source: str
    exit_depth_source: str
    funding_source: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key in ("decision_at", "entry_at", "exit_at"):
            payload[key] = payload[key].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return payload


@dataclass(frozen=True)
class EquityPoint:
    observed_at: datetime
    equity_usdt: float
    realized_pnl_usdt: float
    unrealized_pnl_usdt: float
    gross_exposure_usdt: float
    active_positions: int
    mark_kind: str = "bar_close"

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        return payload


@dataclass(frozen=True)
class EventDrivenReport:
    configuration: Mapping[str, object]
    trades: tuple[TradeRecord, ...]
    rejected_signals: Mapping[str, int]
    initial_equity_usdt: float
    final_equity_usdt: float
    net_return: float
    max_drawdown: float
    profit_factor: float | None
    total_fee_cost: float
    total_slippage_cost: float
    total_funding_cost: float
    cancel_fill_race_count: int
    execution_cost_evidence_complete: bool
    direct_execution_cost_trade_count: int
    proxy_execution_cost_trade_count: int
    simulation_complete: bool
    unresolved_position_count: int
    risk_policy_compliant: bool
    risk_budget_breach_count: int
    maximum_realized_loss_to_risk_budget: float
    intrabar_path_used: bool = True
    mark_to_market_used: bool = True
    equity_curve: tuple[EquityPoint, ...] = ()
    maximum_gross_exposure_usdt: float = 0.0

    def to_dict(self, *, include_trades: bool = True) -> dict[str, object]:
        payload = asdict(self)
        payload["trades"] = [trade.to_dict() for trade in self.trades] if include_trades else []
        payload["equity_curve"] = [point.to_dict() for point in self.equity_curve]
        return payload


def _drawdown(curve: Sequence[float]) -> float:
    peak = curve[0] if curve else 0.0
    maximum = 0.0
    for value in curve:
        peak = max(peak, value)
        if peak > 0:
            maximum = max(maximum, (peak - value) / peak)
    return maximum


def _pretrade_risk_loss_bps(
    signal: SignalEvent,
    execution: TripleBarrierConfig,
    config: BacktestConfig,
) -> float:
    """Conservative, decision-time loss budget without future bar information."""

    entry_fee = execution.maker_fee_bps if signal.maker_entry else execution.taker_fee_bps
    exit_fee = execution.maker_fee_bps if signal.maker_exit else execution.taker_fee_bps
    # Impact is capped at 3x full-depth impact by the execution model. Use that
    # cap here because actual future depth/spread must not influence sizing.
    adverse_slippage_per_leg = (
        execution.base_slippage_bps
        + execution.default_spread_bps / 2.0
        + execution.impact_bps_at_full_depth * 3.0
    ) * execution.slippage_stress_multiplier
    funding_intervals = max(1, math.ceil(signal.max_holding_sec / (8 * 60 * 60)))
    funding_buffer = (
        funding_intervals
        * config.funding_risk_buffer_bps_per_8h
        * execution.funding_stress_multiplier
    )
    return (
        signal.stop_loss_bps
        + entry_fee
        + exit_fee
        + 2.0 * adverse_slippage_per_leg
        + funding_buffer
    )


@dataclass(frozen=True, slots=True)
class _MarketPathIndex:
    bars: tuple[MarketBar, ...]
    open_times: tuple[datetime, ...]
    close_times: tuple[datetime, ...]

    @classmethod
    def build(cls, values: Sequence[MarketBar]) -> "_MarketPathIndex":
        bars = tuple(
            sorted(values, key=lambda bar: (bar.open_time, bar.available_at))
        )
        if any(
            later.close_time < earlier.close_time
            for earlier, later in zip(bars, bars[1:])
        ):
            raise ValueError("market path close times must be chronological")
        return cls(
            bars=bars,
            open_times=tuple(bar.open_time for bar in bars),
            close_times=tuple(bar.close_time for bar in bars),
        )

    def signal_window(
        self,
        *,
        signal_at: datetime,
        max_wait_sec: int,
        max_holding_sec: int,
        latency_ms: int,
    ) -> tuple[MarketBar, ...]:
        """Return only bars that can affect entry, barriers, or timeout proof."""

        activation = signal_at + timedelta(milliseconds=latency_ms)
        latest_open = signal_at + timedelta(
            seconds=max_wait_sec + max_holding_sec
        )
        start = bisect_right(self.close_times, activation)
        stop = bisect_right(self.open_times, latest_open)
        # Preserve the first post-activation observation even when a data gap
        # puts its open beyond the timeout; it is needed to prove ENTRY_TIMEOUT.
        if start < len(self.bars) and stop <= start:
            stop = start + 1
        return self.bars[start:stop]

    def trade_window(self, trade: TradeRecord) -> tuple[MarketBar, ...]:
        start = bisect_right(self.close_times, trade.entry_at)
        stop = bisect_left(self.open_times, trade.exit_at)
        return self.bars[start:stop]


def _prepare_market_index(
    market_bars: Mapping[str, Sequence[MarketBar]],
) -> dict[str, _MarketPathIndex]:
    return {
        str(key): _MarketPathIndex.build(values)
        for key, values in market_bars.items()
    }


def _mark_to_market_curve(
    trades: Sequence[TradeRecord],
    market_bars: Mapping[str, Sequence[MarketBar]],
    initial_equity: float,
    *,
    until: datetime | None = None,
    _market_index: Mapping[str, _MarketPathIndex] | None = None,
) -> tuple[EquityPoint, ...]:
    if not trades:
        return ()
    market_index = dict(_market_index or _prepare_market_index(market_bars))
    cutoff = until.astimezone(timezone.utc) if until is not None else None
    trade_bars = {
        id(trade): (
            market_index[trade.market_key].trade_window(trade)
            if trade.market_key in market_index
            else ()
        )
        for trade in trades
    }
    observations = {trade.decision_at for trade in trades}
    observations.update(trade.entry_at for trade in trades)
    observations.update(trade.exit_at for trade in trades)
    bar_observations: set[datetime] = set()
    for trade in trades:
        for bar in trade_bars[id(trade)]:
            if cutoff is not None and bar.available_at > cutoff:
                continue
            if bar.close_time <= trade.entry_at or bar.open_time >= trade.exit_at:
                continue
            observed_at = min(bar.close_time, trade.exit_at)
            observations.add(observed_at)
            bar_observations.add(observed_at)
    if cutoff is not None:
        observations = {value for value in observations if value <= cutoff}
        bar_observations = {value for value in bar_observations if value <= cutoff}

    def point_at(observed_at: datetime, *, adverse: bool) -> EquityPoint:
        # At an exit timestamp the conservative intrabar point is evaluated
        # immediately before settlement; the close point is evaluated after it.
        realized = sum(
            trade.net_pnl
            for trade in trades
            if trade.exit_at < observed_at or (not adverse and trade.exit_at == observed_at)
        )
        unrealized = 0.0
        gross_exposure = 0.0
        active_positions = 0
        for trade in trades:
            active = trade.entry_at <= observed_at and (
                trade.exit_at > observed_at or (adverse and trade.exit_at == observed_at)
            )
            if not active:
                continue
            active_positions += 1
            gross_exposure += trade.notional_usdt
            mark = trade.entry_fill_price
            current_bar: MarketBar | None = None
            accrued_funding_bps = 0.0
            for bar in trade_bars[id(trade)]:
                if cutoff is not None and bar.available_at > cutoff:
                    continue
                if bar.close_time > observed_at:
                    break
                if bar.close_time <= trade.entry_at:
                    continue
                if bar.open_time >= trade.exit_at:
                    break
                mark = bar.close
                accrued_funding_bps += bar.funding_bps
                if bar.open_time < trade.exit_at and bar.close_time >= observed_at:
                    current_bar = bar
            if adverse and current_bar is not None:
                mark = current_bar.low if trade.side == "BUY" else current_bar.high
            direction = 1.0 if trade.side == "BUY" else -1.0
            gross_open_pnl = trade.notional_usdt * direction * (
                mark / trade.entry_fill_price - 1.0
            )
            accrued_funding = (
                trade.notional_usdt * direction * accrued_funding_bps / 10_000.0
            )
            # Fill-to-mark PnL already embeds entry slippage.  Only the entry
            # fee is a separate cash debit while the position remains open.
            unrealized += gross_open_pnl - trade.entry_fee_cost - accrued_funding
        return EquityPoint(
            observed_at=observed_at,
            equity_usdt=initial_equity + realized + unrealized,
            realized_pnl_usdt=realized,
            unrealized_pnl_usdt=unrealized,
            gross_exposure_usdt=gross_exposure,
            active_positions=active_positions,
            mark_kind="intrabar_adverse" if adverse else "bar_close",
        )

    points: list[EquityPoint] = []
    for observed_at in sorted(observations):
        if observed_at in bar_observations:
            points.append(point_at(observed_at, adverse=True))
        points.append(point_at(observed_at, adverse=False))
    return tuple(points)


class EventDrivenBacktest:
    """Portfolio-aware simulation driven by order and OHLC path events.

    Entry fill, stop/target path, fees, dynamic slippage, funding, partial
    fills, latency, timeout, and cancel/fill races are all represented.  A
    close-to-close return is never accepted as a trade record.
    """

    def __init__(
        self,
        config: BacktestConfig | None = None,
        execution: TripleBarrierConfig | None = None,
    ) -> None:
        self.config = config or BacktestConfig()
        self.execution = execution or TripleBarrierConfig()
        self._cached_market_signature: tuple[object, ...] | None = None
        self._cached_market_index: dict[str, _MarketPathIndex] | None = None

    def _market_index(
        self, market_bars: Mapping[str, Sequence[MarketBar]]
    ) -> dict[str, _MarketPathIndex]:
        signature = (
            id(market_bars),
            tuple(
                (
                    str(key),
                    id(values),
                    len(values),
                    values[0].open_time if values else None,
                    values[-1].close_time if values else None,
                )
                for key, values in sorted(
                    market_bars.items(), key=lambda item: str(item[0])
                )
            ),
        )
        if signature != self._cached_market_signature:
            self._cached_market_index = _prepare_market_index(market_bars)
            self._cached_market_signature = signature
        assert self._cached_market_index is not None
        return self._cached_market_index

    def run(
        self,
        signals: Iterable[SignalEvent],
        market_bars: Mapping[str, Sequence[MarketBar]],
        *,
        cost_multiplier: float = 1.0,
    ) -> EventDrivenReport:
        if cost_multiplier < 1:
            raise ValueError("cost_multiplier cannot make costs more optimistic")
        cfg = self.config
        execution = TripleBarrierConfig(
            maker_fee_bps=self.execution.maker_fee_bps * cost_multiplier,
            taker_fee_bps=self.execution.taker_fee_bps * cost_multiplier,
            base_slippage_bps=self.execution.base_slippage_bps,
            volatility_slippage_multiplier=self.execution.volatility_slippage_multiplier,
            impact_bps_at_full_depth=self.execution.impact_bps_at_full_depth,
            default_spread_bps=self.execution.default_spread_bps,
            default_depth_usdt=self.execution.default_depth_usdt,
            missing_depth_fill_fraction=self.execution.missing_depth_fill_fraction,
            latency_ms=self.execution.latency_ms,
            stop_first_when_same_bar=self.execution.stop_first_when_same_bar,
            slippage_stress_multiplier=(
                self.execution.slippage_stress_multiplier * cost_multiplier
            ),
            funding_stress_multiplier=(
                self.execution.funding_stress_multiplier * cost_multiplier
            ),
        )
        market_index = self._market_index(market_bars)
        ordered = sorted(signals, key=lambda item: (item.decision_at, item.signal_id))
        equity = cfg.initial_equity_usdt
        curve = [equity]
        trades: list[TradeRecord] = []
        pending: list[TradeRecord] = []
        order_reservations: list[tuple[datetime, float]] = []
        unresolved_position_count = 0
        active_until: dict[str, datetime] = {}
        rejected: dict[str, int] = defaultdict(int)
        gross_profit = 0.0
        gross_loss = 0.0

        def settle_until(timestamp: datetime) -> None:
            nonlocal equity, gross_profit, gross_loss
            due = sorted(
                (trade for trade in pending if trade.exit_at <= timestamp),
                key=lambda trade: (trade.exit_at, trade.signal_id),
            )
            for trade in due:
                equity += trade.net_pnl
                curve.append(equity)
                if trade.net_pnl >= 0:
                    gross_profit += trade.net_pnl
                else:
                    gross_loss += abs(trade.net_pnl)
                pending.remove(trade)
                if active_until.get(trade.symbol) == trade.exit_at:
                    active_until.pop(trade.symbol, None)

        for signal in ordered:
            now = signal.decision_at.astimezone(timezone.utc)
            settle_until(now)
            order_reservations = [
                item for item in order_reservations if item[0] > now
            ]
            if cfg.require_positive_lower_bound_edge and signal.lower_bound_net_edge <= 0:
                rejected["non_positive_lower_bound_edge"] += 1
                continue
            if cfg.require_stop and signal.stop_loss_bps <= 0:
                rejected["missing_stop"] += 1
                continue
            symbol = signal.symbol.upper()
            if active_until.get(symbol, datetime.min.replace(tzinfo=timezone.utc)) > now:
                rejected["averaging_down_or_overlapping_position"] += 1
                continue
            state_curve = _mark_to_market_curve(
                trades,
                market_bars,
                cfg.initial_equity_usdt,
                until=now,
                _market_index=market_index,
            )
            current_equity = state_curve[-1].equity_usdt if state_curve else equity
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = day_start - timedelta(days=day_start.weekday())

            def equity_before(cutoff: datetime) -> float:
                prior = [point.equity_usdt for point in state_curve if point.observed_at < cutoff]
                return prior[-1] if prior else cfg.initial_equity_usdt

            if current_equity <= equity_before(day_start) * (1.0 - cfg.daily_loss_limit):
                rejected["daily_loss_limit"] += 1
                continue
            if current_equity <= equity_before(week_start) * (1.0 - cfg.weekly_loss_limit):
                rejected["weekly_loss_limit"] += 1
                continue
            state_values = [cfg.initial_equity_usdt] + [
                point.equity_usdt for point in state_curve
            ]
            if _drawdown(state_values) >= cfg.equity_drawdown_limit:
                rejected["equity_drawdown_limit"] += 1
                continue
            if current_equity <= 0:
                rejected["non_positive_equity"] += 1
                continue
            risk_budget = current_equity * cfg.risk_per_trade
            pretrade_risk_loss_bps = _pretrade_risk_loss_bps(
                signal, self.execution, cfg
            )
            risk_notional = risk_budget / max(
                pretrade_risk_loss_bps / 10_000.0, 1e-12
            )
            leverage_notional = current_equity * cfg.leverage_cap
            portfolio_available = max(
                0.0,
                current_equity * cfg.max_gross_exposure
                - sum(trade.notional_usdt for trade in pending)
                - sum(value for _, value in order_reservations),
            )
            notional = min(risk_notional, leverage_notional, portfolio_available)
            if signal.requested_notional_usdt is not None:
                notional = min(notional, signal.requested_notional_usdt)
            if notional <= 0:
                rejected["portfolio_exposure_limit"] += 1
                continue
            quantity = notional / signal.reference_price
            market_key = signal.market_key or signal.symbol.upper()
            path_index = market_index.get(
                market_key, market_index.get(signal.symbol.upper())
            )
            if path_index is None:
                rejected["missing_market_path"] += 1
                continue
            bars = path_index.signal_window(
                signal_at=now,
                max_wait_sec=signal.max_wait_sec,
                max_holding_sec=signal.max_holding_sec,
                latency_ms=execution.latency_ms,
            )
            if not bars:
                rejected["missing_market_path"] += 1
                continue
            label = build_triple_barrier_label(
                EntrySpec(
                    symbol=signal.symbol,
                    side=signal.side.upper(),
                    signal_at=now,
                    reference_price=signal.reference_price,
                    quantity=quantity,
                    take_profit_bps=signal.take_profit_bps,
                    stop_loss_bps=signal.stop_loss_bps,
                    max_holding_sec=signal.max_holding_sec,
                    feature_available_at=signal.feature_available_at,
                    order_type=signal.order_type.upper(),
                    limit_price=signal.limit_price,
                    max_wait_sec=signal.max_wait_sec,
                    maker_entry=signal.maker_entry,
                    maker_exit=signal.maker_exit,
                ),
                bars,
                execution,
            )
            if label.entry_fill_at is None or label.exit_at is None:
                incomplete_position = label.entry_fill_at is not None
                if not incomplete_position:
                    # The offline result cannot be used before the timeout/cancel
                    # observation that established no fill. Reserve the order in
                    # the same way the live system must while its outcome is open.
                    reservation_until = label.label_available_at
                    reserved_notional = notional
                else:
                    # A known fill without a complete exit path is an unresolved
                    # position, never a zero-return non-trade.
                    unresolved_position_count += 1
                    reservation_until = label.entry_fill_at + timedelta(
                        seconds=signal.max_holding_sec
                    )
                    reserved_notional = notional * label.fill_fraction
                active_until[symbol] = max(
                    active_until.get(
                        symbol, datetime.min.replace(tzinfo=timezone.utc)
                    ),
                    reservation_until,
                )
                order_reservations.append(
                    (reservation_until, reserved_notional)
                )
                rejected[label.exit_reason.lower()] += 1
                if incomplete_position:
                    # Equity and portfolio exposure are unknowable beyond this
                    # point. Continuing would let later signals trade on an
                    # invented balance even though the final gate would fail.
                    break
                continue
            if any(
                value is None
                for value in (
                    label.gross_return,
                    label.net_return,
                    label.fee_return,
                    label.slippage_return,
                    label.funding_return,
                    label.entry_fill_price,
                    label.exit_fill_price,
                )
            ):
                raise RuntimeError("completed execution label has missing economics")
            filled_notional = notional * label.fill_fraction
            gross_pnl = filled_notional * float(label.gross_return)
            net_pnl = filled_notional * float(label.net_return)
            fee_cost = filled_notional * float(label.fee_return)
            entry_fee_bps = execution.maker_fee_bps if signal.maker_entry else execution.taker_fee_bps
            entry_fee_cost = filled_notional * entry_fee_bps / 10_000.0
            slippage_cost = filled_notional * float(label.slippage_return)
            funding_cost = filled_notional * float(label.funding_return)
            cancel_at = now.timestamp() + signal.max_wait_sec
            race = abs(label.entry_fill_at.timestamp() - cancel_at) <= max(1.0, execution.latency_ms / 1000.0)
            trade = TradeRecord(
                signal_id=signal.signal_id,
                symbol=signal.symbol.upper(),
                side=signal.side.upper(),
                decision_at=now,
                entry_at=label.entry_fill_at,
                exit_at=label.exit_at,
                month=label.exit_at.strftime("%Y-%m"),
                regime=signal.regime,
                notional_usdt=filled_notional,
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
                gross_return=float(label.gross_return),
                net_return=float(label.net_return),
                fee_cost=fee_cost,
                entry_fee_cost=entry_fee_cost,
                slippage_cost=slippage_cost,
                funding_cost=funding_cost,
                mae=label.mae,
                mfe=label.mfe,
                fill_probability=label.fill_probability,
                fill_fraction=label.fill_fraction,
                partial_fill=label.partial_fill,
                exit_reason=label.exit_reason,
                cancel_fill_race=race,
                entry_fill_price=float(label.entry_fill_price),
                exit_fill_price=float(label.exit_fill_price),
                filled_quantity=float(label.filled_quantity),
                risk_budget_usdt=risk_budget,
                pretrade_risk_loss_bps=pretrade_risk_loss_bps,
                market_key=signal.market_key or signal.symbol.upper(),
                execution_cost_evidence_complete=label.execution_cost_evidence_complete,
                entry_spread_source=label.entry_spread_source,
                entry_depth_source=label.entry_depth_source,
                exit_spread_source=label.exit_spread_source,
                exit_depth_source=label.exit_depth_source,
                funding_source=label.funding_source,
            )
            trades.append(trade)
            pending.append(trade)
            active_until[symbol] = label.exit_at

        settle_until(datetime.max.replace(tzinfo=timezone.utc))
        equity_curve = _mark_to_market_curve(
            trades,
            market_bars,
            cfg.initial_equity_usdt,
            _market_index=market_index,
        )
        mtm_values = [cfg.initial_equity_usdt] + [point.equity_usdt for point in equity_curve]
        realized_loss_to_budget = [
            max(0.0, -trade.net_pnl) / max(trade.risk_budget_usdt, 1e-12)
            for trade in trades
        ]
        risk_budget_breach_count = sum(
            ratio > 1.0 + 1e-12 for ratio in realized_loss_to_budget
        )

        return EventDrivenReport(
            configuration={**asdict(cfg), "execution": asdict(execution), "cost_multiplier": cost_multiplier},
            trades=tuple(trades),
            rejected_signals=dict(sorted(rejected.items())),
            initial_equity_usdt=cfg.initial_equity_usdt,
            final_equity_usdt=equity,
            net_return=equity / cfg.initial_equity_usdt - 1.0,
            max_drawdown=_drawdown(mtm_values),
            profit_factor=(gross_profit / gross_loss if gross_loss > 0 else None),
            total_fee_cost=sum(trade.fee_cost for trade in trades),
            total_slippage_cost=sum(trade.slippage_cost for trade in trades),
            total_funding_cost=sum(trade.funding_cost for trade in trades),
            cancel_fill_race_count=sum(trade.cancel_fill_race for trade in trades),
            execution_cost_evidence_complete=bool(trades)
            and unresolved_position_count == 0
            and all(trade.execution_cost_evidence_complete for trade in trades),
            direct_execution_cost_trade_count=sum(
                trade.execution_cost_evidence_complete for trade in trades
            ),
            proxy_execution_cost_trade_count=sum(
                not trade.execution_cost_evidence_complete for trade in trades
            ),
            simulation_complete=unresolved_position_count == 0,
            unresolved_position_count=unresolved_position_count,
            risk_policy_compliant=risk_budget_breach_count == 0,
            risk_budget_breach_count=risk_budget_breach_count,
            maximum_realized_loss_to_risk_budget=max(
                realized_loss_to_budget, default=0.0
            ),
            equity_curve=equity_curve,
            maximum_gross_exposure_usdt=max(
                (point.gross_exposure_usdt for point in equity_curve), default=0.0
            ),
        )


__all__: Sequence[str] = (
    "BacktestConfig",
    "EventDrivenBacktest",
    "EventDrivenReport",
    "EquityPoint",
    "SignalEvent",
    "TradeRecord",
)
