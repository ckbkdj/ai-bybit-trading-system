from __future__ import annotations

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
    market_key: str

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


def _mark_to_market_curve(
    trades: Sequence[TradeRecord],
    market_bars: Mapping[str, Sequence[MarketBar]],
    initial_equity: float,
    *,
    until: datetime | None = None,
) -> tuple[EquityPoint, ...]:
    if not trades:
        return ()
    bars_by_key = {
        str(key): sorted(values, key=lambda bar: (bar.close_time, bar.available_at))
        for key, values in market_bars.items()
    }
    cutoff = until.astimezone(timezone.utc) if until is not None else None
    observations = {trade.decision_at for trade in trades}
    observations.update(trade.entry_at for trade in trades)
    observations.update(trade.exit_at for trade in trades)
    bar_observations: set[datetime] = set()
    for trade in trades:
        for bar in bars_by_key.get(trade.market_key, ()):
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
            for bar in bars_by_key.get(trade.market_key, ()):
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
            base_slippage_bps=self.execution.base_slippage_bps * cost_multiplier,
            volatility_slippage_multiplier=self.execution.volatility_slippage_multiplier * cost_multiplier,
            impact_bps_at_full_depth=self.execution.impact_bps_at_full_depth * cost_multiplier,
            default_spread_bps=self.execution.default_spread_bps * cost_multiplier,
            default_depth_usdt=self.execution.default_depth_usdt,
            missing_depth_fill_fraction=self.execution.missing_depth_fill_fraction,
            latency_ms=self.execution.latency_ms,
            stop_first_when_same_bar=self.execution.stop_first_when_same_bar,
        )
        ordered = sorted(signals, key=lambda item: (item.decision_at, item.signal_id))
        equity = cfg.initial_equity_usdt
        curve = [equity]
        trades: list[TradeRecord] = []
        pending: list[TradeRecord] = []
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
            risk_notional = current_equity * cfg.risk_per_trade / max(signal.stop_loss_bps / 10_000.0, 1e-12)
            leverage_notional = current_equity * cfg.leverage_cap
            portfolio_available = max(
                0.0,
                current_equity * cfg.max_gross_exposure - sum(trade.notional_usdt for trade in pending),
            )
            notional = min(risk_notional, leverage_notional, portfolio_available)
            if signal.requested_notional_usdt is not None:
                notional = min(notional, signal.requested_notional_usdt)
            if notional <= 0:
                rejected["portfolio_exposure_limit"] += 1
                continue
            quantity = notional / signal.reference_price
            bars = list(
                market_bars.get(
                    signal.market_key or signal.symbol.upper(),
                    market_bars.get(signal.symbol.upper(), ()),
                )
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
                rejected[label.exit_reason.lower()] += 1
                continue
            filled_notional = notional * label.fill_fraction
            gross_pnl = filled_notional * label.gross_return
            net_pnl = filled_notional * label.net_return
            fee_cost = filled_notional * label.fee_return
            entry_fee_bps = execution.maker_fee_bps if signal.maker_entry else execution.taker_fee_bps
            entry_fee_cost = filled_notional * entry_fee_bps / 10_000.0
            slippage_cost = filled_notional * label.slippage_return
            funding_cost = filled_notional * label.funding_return
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
                gross_return=label.gross_return,
                net_return=label.net_return,
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
                market_key=signal.market_key or signal.symbol.upper(),
            )
            trades.append(trade)
            pending.append(trade)
            active_until[symbol] = label.exit_at

        settle_until(datetime.max.replace(tzinfo=timezone.utc))
        equity_curve = _mark_to_market_curve(trades, market_bars, cfg.initial_equity_usdt)
        mtm_values = [cfg.initial_equity_usdt] + [point.equity_usdt for point in equity_curve]

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
