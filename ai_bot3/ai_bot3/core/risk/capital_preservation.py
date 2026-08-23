from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Sequence


@dataclass(frozen=True)
class CapitalPreservationConfig:
    risk_per_trade: float = 0.0025
    daily_loss_limit: float = 0.0050
    weekly_loss_limit: float = 0.0150
    equity_drawdown_limit: float = 0.03
    leverage_cap: float = 2.0
    no_averaging_down: bool = True
    no_martingale: bool = True
    require_stop: bool = True
    require_positive_lower_bound_edge: bool = True

    def __post_init__(self) -> None:
        limits = (
            (self.risk_per_trade, 0.0025, "risk_per_trade"),
            (self.daily_loss_limit, 0.0050, "daily_loss_limit"),
            (self.weekly_loss_limit, 0.0150, "weekly_loss_limit"),
            (self.equity_drawdown_limit, 0.03, "equity_drawdown_limit"),
            (self.leverage_cap, 2.0, "leverage_cap"),
        )
        for value, maximum, name in limits:
            if value <= 0 or value > maximum:
                raise ValueError(f"{name} exceeds the capital-preservation maximum")


@dataclass(frozen=True)
class CapitalState:
    equity: float
    peak_equity: float
    daily_pnl: float
    weekly_pnl: float
    gross_exposure: float
    existing_symbol_exposure: float = 0.0
    previous_trade_risk: float | None = None
    previous_trade_was_loss: bool = False


@dataclass(frozen=True)
class TradeProposal:
    symbol: str
    risk_fraction: float
    leverage: float
    target_exposure: float
    stop_price: float | None
    lower_bound_net_edge: float | None
    increases_existing_position: bool = False


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reasons: tuple[str, ...]
    effective_risk_fraction: float
    effective_leverage_cap: float


def evaluate_trade_proposal(
    state: CapitalState,
    proposal: TradeProposal,
    config: CapitalPreservationConfig | None = None,
) -> RiskDecision:
    cfg = config or CapitalPreservationConfig()
    reasons: list[str] = []
    if state.equity <= 0 or state.peak_equity <= 0:
        reasons.append("invalid_equity_state")
    drawdown = (state.peak_equity - state.equity) / state.peak_equity if state.peak_equity > 0 else 1.0
    if drawdown >= cfg.equity_drawdown_limit:
        reasons.append("equity_drawdown_limit")
    if state.daily_pnl <= -state.peak_equity * cfg.daily_loss_limit:
        reasons.append("daily_loss_limit")
    if state.weekly_pnl <= -state.peak_equity * cfg.weekly_loss_limit:
        reasons.append("weekly_loss_limit")
    if proposal.risk_fraction <= 0 or proposal.risk_fraction > cfg.risk_per_trade:
        reasons.append("risk_per_trade_limit")
    if proposal.leverage <= 0 or proposal.leverage > cfg.leverage_cap:
        reasons.append("leverage_cap")
    if proposal.target_exposure <= 0 or state.gross_exposure + proposal.target_exposure > cfg.leverage_cap:
        reasons.append("portfolio_exposure_limit")
    if cfg.require_stop and (proposal.stop_price is None or proposal.stop_price <= 0):
        reasons.append("missing_stop")
    if cfg.require_positive_lower_bound_edge and (
        proposal.lower_bound_net_edge is None or proposal.lower_bound_net_edge <= 0
    ):
        reasons.append("non_positive_lower_bound_net_edge")
    if cfg.no_averaging_down and proposal.increases_existing_position and state.existing_symbol_exposure > 0:
        reasons.append("averaging_down_forbidden")
    if (
        cfg.no_martingale
        and state.previous_trade_was_loss
        and state.previous_trade_risk is not None
        and proposal.risk_fraction > state.previous_trade_risk + 1e-12
    ):
        reasons.append("martingale_forbidden")
    return RiskDecision(
        allowed=not reasons,
        reasons=tuple(reasons),
        effective_risk_fraction=min(max(0.0, proposal.risk_fraction), cfg.risk_per_trade),
        effective_leverage_cap=cfg.leverage_cap,
    )


def policy_report(config: CapitalPreservationConfig | None = None) -> dict[str, object]:
    cfg = config or CapitalPreservationConfig()
    return {
        "policy": asdict(cfg),
        "fail_closed": True,
        "no_averaging_down": cfg.no_averaging_down,
        "no_martingale": cfg.no_martingale,
        "no_trade_without_stop": cfg.require_stop,
        "no_trade_when_lower_bound_net_edge_lte_zero": cfg.require_positive_lower_bound_edge,
    }


__all__: Sequence[str] = (
    "CapitalPreservationConfig",
    "CapitalState",
    "RiskDecision",
    "TradeProposal",
    "evaluate_trade_proposal",
    "policy_report",
)
