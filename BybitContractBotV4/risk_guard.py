from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Sequence

from contracts.operation_ticket_v1 import OperationTicket
from incident_modes import action_allowed


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    last_price: float
    bid_price: float
    ask_price: float
    market_regime: str
    captured_at: datetime
    cross_exchange_basis_bps: Optional[float] = None

    @property
    def spread_bps(self) -> float:
        midpoint = (self.ask_price + self.bid_price) / 2
        return ((self.ask_price - self.bid_price) / midpoint * 10_000) if midpoint > 0 else float("inf")


@dataclass(frozen=True)
class AccountSnapshot:
    equity_usdt: float
    free_margin_usdt: float
    margin_used_usdt: float
    realised_pnl_today: float = 0.0
    unrealised_pnl: float = 0.0
    realised_pnl_week: float = 0.0
    consecutive_losses: int = 0
    cooldown_until: Optional[datetime] = None
    equity_high_water_usdt: Optional[float] = None
    risk_metrics_healthy: bool = True

    @property
    def margin_utilization(self) -> float:
        return self.margin_used_usdt / self.equity_usdt if self.equity_usdt > 0 else float("inf")


@dataclass(frozen=True)
class PortfolioSnapshot:
    gross_notional_usdt: float
    same_direction_correlated_notional_usdt: float
    position_version: int
    current_position_qty: float
    position_owner_id: Optional[str] = None


@dataclass(frozen=True)
class SystemHealth:
    mode: str
    kill_switch: bool
    websocket_confirmed: bool
    data_source_healthy: bool
    exchange_clock_drift_sec: float
    incident_mode: str = "NORMAL"
    reconciliation_complete: bool = True


@dataclass(frozen=True)
class RiskLimits:
    max_risk_per_trade_pct: float = 0.0025
    max_daily_loss_pct: float = 0.005
    max_weekly_loss_pct: float = 0.015
    max_equity_drawdown_pct: float = 0.03
    max_gross_leverage: float = 2.0
    max_correlated_exposure_pct: float = 0.35
    max_margin_utilization: float = 0.70
    max_consecutive_losses: int = 4
    max_exchange_clock_drift_sec: float = 2.0
    require_websocket_confirmation: bool = True

    def __post_init__(self) -> None:
        if not 0 < self.max_risk_per_trade_pct <= 0.0025:
            raise ValueError("max_risk_per_trade_pct cannot exceed 0.25%")
        if not 0 < self.max_daily_loss_pct <= 0.005:
            raise ValueError("max_daily_loss_pct cannot exceed 0.50%")
        if not 0 < self.max_weekly_loss_pct <= 0.015:
            raise ValueError("max_weekly_loss_pct cannot exceed 1.50%")
        if not 0 < self.max_equity_drawdown_pct <= 0.03:
            raise ValueError("max_equity_drawdown_pct cannot exceed 3.00%")
        if not 0 < self.max_gross_leverage <= 2.0:
            raise ValueError("max_gross_leverage cannot exceed 2x")


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason_code: str
    reason_detail: str
    checks: Sequence[str] = field(default_factory=tuple)


class RiskGuard:
    def __init__(self, limits: RiskLimits | None = None):
        self.limits = limits or RiskLimits()

    def evaluate(
        self,
        ticket: OperationTicket,
        market: MarketSnapshot,
        account: AccountSnapshot,
        portfolio: PortfolioSnapshot,
        health: SystemHealth,
        *,
        now: Optional[datetime] = None,
    ) -> RiskDecision:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        checks: list[str] = []
        risk_increasing = ticket.intent.action in {"OPEN", "INCREASE", "REPLACE"}

        def reject(code: str, detail: str) -> RiskDecision:
            return RiskDecision(False, code, detail, tuple(checks))

        if not action_allowed(health.incident_mode, ticket.intent.action):
            return reject(
                "INCIDENT_MODE_BLOCK",
                f"ticket action is blocked in incident mode {health.incident_mode}",
            )
        if risk_increasing and not health.reconciliation_complete:
            return reject(
                "RECONCILIATION_INCOMPLETE",
                "startup/account reconciliation has not completed",
            )
        checks.append("incident_and_reconciliation")
        if risk_increasing and health.kill_switch:
            return reject("KILL_SWITCH", "system kill switch is enabled")
        checks.append("kill_switch_allows_risk_reduction" if health.kill_switch else "kill_switch")
        if current < ticket.valid_from:
            return reject("NOT_YET_VALID", "ticket valid_from is in the future")
        if current >= ticket.expires_at:
            return reject("TICKET_EXPIRED", "ticket has expired")
        checks.append("ticket_time")
        if risk_increasing and ticket.guards.event_blackout:
            return reject("EVENT_BLACKOUT", "ticket carries an active event blackout")
        if risk_increasing and ticket.guards.provisional_reduce_only:
            return reject(
                "PROVISIONAL_REDUCE_ONLY",
                "Tier B event evidence may reduce risk but cannot authorize new risk",
            )
        checks.append("event_blackout")
        if risk_increasing and ticket.guards.observed_data_quality < ticket.guards.min_data_quality:
            return reject("BAD_DATA_QUALITY", "forecast data quality is below the ticket threshold")
        if risk_increasing and ticket.guards.observed_feature_age_sec > ticket.guards.max_feature_age_sec:
            return reject("STALE_FEATURES", "forecast features are too old")
        if risk_increasing and not health.data_source_healthy:
            return reject("DATA_SOURCE_OUTAGE", "live data source health is not confirmed")
        checks.append("data_quality")
        if market.symbol.strip().upper() != ticket.instrument.symbol:
            return reject("SYMBOL_MISMATCH", "live market symbol does not match ticket")
        if ticket.intent.action == "CANCEL":
            if portfolio.position_version != ticket.guards.required_position_version:
                return reject("POSITION_VERSION_CONFLICT", "position version no longer matches the ticket")
            if abs(health.exchange_clock_drift_sec) > self.limits.max_exchange_clock_drift_sec:
                return reject("CLOCK_DRIFT", "exchange clock drift exceeds the limit")
            if (
                risk_increasing
                and health.mode != "shadow"
                and self.limits.require_websocket_confirmation
                and not health.websocket_confirmed
            ):
                return reject("WEBSOCKET_UNCONFIRMED", "private WebSocket health is not confirmed")
            return RiskDecision(True, "APPROVED", "cancel risk checks passed", tuple(checks))
        if market.last_price <= 0 or market.bid_price <= 0 or market.ask_price <= 0:
            return reject("INVALID_MARKET_PRICE", "live market prices are invalid")
        if ticket.entry is None:
            return reject("ENTRY_MISSING", "ticket has no live reference entry")
        deviation = abs(market.last_price - ticket.entry.reference_price) / ticket.entry.reference_price * 10_000
        if risk_increasing and deviation > ticket.guards.max_live_price_deviation_bps:
            return reject("PRICE_DEVIATION", f"live price deviation {deviation:.3f} bps exceeds limit")
        if risk_increasing and market.spread_bps > ticket.guards.max_live_spread_bps:
            return reject("SPREAD_TOO_WIDE", f"live spread {market.spread_bps:.3f} bps exceeds limit")
        if risk_increasing and ticket.guards.execution_market != "bybit":
            return reject("EXECUTION_MARKET_INVALID", "execution-related market data must be Bybit")
        if risk_increasing and ticket.guards.require_cross_exchange_basis_check:
            observed_basis = market.cross_exchange_basis_bps
            if observed_basis is None:
                observed_basis = ticket.guards.observed_cross_exchange_basis_bps
            if observed_basis is None:
                return reject(
                    "BASIS_UNAVAILABLE",
                    "required Bybit/forecast-market basis evidence is unavailable",
                )
            if observed_basis > ticket.guards.max_cross_exchange_basis_bps:
                return reject(
                    "CROSS_EXCHANGE_BASIS",
                    f"cross-exchange basis {observed_basis:.3f} bps exceeds limit",
                )
        checks.append("live_market")
        if risk_increasing and ticket.guards.required_market_regime and market.market_regime not in ticket.guards.required_market_regime:
            return reject("REGIME_MISMATCH", "live market regime is not allowed by ticket")
        if risk_increasing and market.market_regime != ticket.guards.observed_market_regime:
            return reject("REGIME_CHANGED", "market regime changed after ticket creation")
        checks.append("regime")
        if risk_increasing and (account.equity_usdt <= 0 or account.free_margin_usdt < 0):
            return reject("ACCOUNT_EQUITY_INVALID", "account equity or free margin is invalid")
        if risk_increasing and not account.risk_metrics_healthy:
            return reject(
                "ACCOUNT_RISK_METRICS_UNAVAILABLE",
                "daily account PnL and loss-streak evidence is unavailable",
            )
        if (
            risk_increasing
            and ticket.intent.risk_budget_pct > self.limits.max_risk_per_trade_pct
        ):
            return reject(
                "TRADE_RISK_LIMIT",
                "ticket risk budget exceeds the 0.25% per-trade hard limit",
            )
        daily_pnl = account.realised_pnl_today + account.unrealised_pnl
        if risk_increasing and daily_pnl <= -(account.equity_usdt * self.limits.max_daily_loss_pct):
            return reject("DAILY_LOSS_LIMIT", "daily realised plus unrealised loss reached the limit")
        weekly_pnl = account.realised_pnl_week + account.unrealised_pnl
        if risk_increasing and weekly_pnl <= -(
            account.equity_usdt * self.limits.max_weekly_loss_pct
        ):
            return reject(
                "WEEKLY_LOSS_LIMIT",
                "weekly realised plus unrealised loss reached the limit",
            )
        high_water = float(account.equity_high_water_usdt or account.equity_usdt)
        equity_drawdown = (high_water - account.equity_usdt) / high_water if high_water > 0 else float("inf")
        if risk_increasing and equity_drawdown >= self.limits.max_equity_drawdown_pct:
            return reject(
                "EQUITY_DRAWDOWN_LIMIT",
                f"account equity drawdown {equity_drawdown:.3%} reached the high-water limit",
            )
        if risk_increasing and account.margin_utilization >= self.limits.max_margin_utilization:
            return reject("MARGIN_UTILIZATION", "account margin utilization reached the limit")
        if risk_increasing and account.consecutive_losses >= self.limits.max_consecutive_losses:
            return reject("CONSECUTIVE_LOSS_COOLDOWN", "consecutive loss limit reached")
        if risk_increasing and account.cooldown_until and account.cooldown_until.astimezone(timezone.utc) > current:
            return reject("COOLDOWN_ACTIVE", "trading cooldown is still active")
        checks.append("account")
        if portfolio.position_version != ticket.guards.required_position_version:
            return reject("POSITION_VERSION_CONFLICT", "position version no longer matches the ticket")
        if risk_increasing and ticket.guards.require_flat_position and abs(portfolio.current_position_qty) > 1e-12:
            return reject("POSITION_NOT_FLAT", "ticket requires a flat position")
        if risk_increasing:
            gross_leverage = portfolio.gross_notional_usdt / account.equity_usdt
            if gross_leverage >= self.limits.max_gross_leverage:
                return reject("GROSS_LEVERAGE_LIMIT", "portfolio gross leverage reached the limit")
            correlated_pct = (
                portfolio.same_direction_correlated_notional_usdt / account.equity_usdt
            )
            if correlated_pct >= self.limits.max_correlated_exposure_pct:
                return reject("CORRELATED_EXPOSURE", "same-direction correlated exposure reached the limit")
        checks.append("portfolio")
        if risk_increasing and abs(health.exchange_clock_drift_sec) > self.limits.max_exchange_clock_drift_sec:
            return reject("CLOCK_DRIFT", "exchange clock drift exceeds the limit")
        if (
            risk_increasing
            and
            health.mode != "shadow"
            and self.limits.require_websocket_confirmation
            and not health.websocket_confirmed
        ):
            return reject("WEBSOCKET_UNCONFIRMED", "private WebSocket health is not confirmed")
        checks.append("system_health")
        return RiskDecision(True, "APPROVED", "all risk checks passed", tuple(checks))
