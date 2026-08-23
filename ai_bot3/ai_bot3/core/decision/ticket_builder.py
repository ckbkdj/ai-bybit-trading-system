from __future__ import annotations

from datetime import timedelta
from typing import Iterable, Optional

from contracts.common import deterministic_id
from contracts.forecast_v1 import ForecastEnvelope
from contracts.operation_ticket_v1 import OperationTicket
from contracts.portfolio_intent_v1 import PortfolioIntent

from .ticket_policy import TicketPolicy, TicketPolicyConfig


class TicketBuilder:
    def __init__(self, policy: TicketPolicy | None = None):
        self.policy = policy or TicketPolicy()

    def build_open_ticket(
        self,
        forecast: ForecastEnvelope,
        *,
        reference_price: float,
        required_position_version: int,
        supersedes_ticket_id: str | None = None,
        portfolio_intent: PortfolioIntent | None = None,
    ) -> Optional[OperationTicket]:
        decision = self.policy.decide(forecast)
        if decision is None:
            return None
        cfg = self.policy.config
        stop_bps = max(float(forecast.distribution.expected_mae_bps or 0), cfg.default_stop_bps)
        direction = 1 if decision.side == "BUY" else -1
        stop_price = reference_price * (1 - direction * stop_bps / 10_000)
        take_profit_bps = max(decision.gross_edge_bps, stop_bps * cfg.default_take_profit_multiple)
        take_profit_price = reference_price * (1 + direction * take_profit_bps / 10_000)
        created_at = (
            portfolio_intent.created_at
            if portfolio_intent is not None
            else forecast.time.created_at
        )
        expires_at = created_at + timedelta(seconds=cfg.ticket_ttl_sec)
        if portfolio_intent is not None:
            # The executable ticket cannot outlive the multi-horizon decision
            # that authorized it.  The shortest contributing horizon therefore
            # remains a hard upper bound even when the normal ticket TTL is longer.
            expires_at = min(expires_at, portfolio_intent.valid_until)
        ticket_id = deterministic_id(
            "tk",
            portfolio_intent.portfolio_decision_id if portfolio_intent else forecast.forecast_id,
            forecast.revision,
            decision.side,
            reference_price,
            required_position_version,
            supersedes_ticket_id,
        )
        top_factors = dict(
            sorted(forecast.factor_scores.items(), key=lambda item: abs(item[1]), reverse=True)[:5]
        )
        limit_price = reference_price * (1 - direction * 1 / 10_000)
        return OperationTicket.model_validate(
            {
                "ticket_id": ticket_id,
                "forecast_id": forecast.forecast_id,
                "forecast_revision": forecast.revision,
                "portfolio_decision_id": (
                    portfolio_intent.portfolio_decision_id
                    if portfolio_intent
                    else deterministic_id("pd", "single-forecast-fixture", forecast.forecast_id)
                ),
                "strategy_release_id": (
                    portfolio_intent.strategy_release_id
                    if portfolio_intent
                    else forecast.lineage.strategy_release_id
                ),
                "supersedes_ticket_id": supersedes_ticket_id,
                "created_at": created_at,
                "valid_from": created_at,
                "expires_at": expires_at,
                "instrument": {"symbol": forecast.instrument.symbol},
                "intent": {
                    "action": "OPEN" if supersedes_ticket_id is None else "REPLACE",
                    "side": decision.side,
                    "position_effect": "OPEN_OR_INCREASE" if supersedes_ticket_id is None else "REPLACE_ONLY",
                    "target_exposure_pct": (
                        abs(portfolio_intent.target_net_exposure_pct)
                        if portfolio_intent
                        else cfg.target_exposure_pct
                    ),
                    "risk_budget_pct": (
                        portfolio_intent.risk_budget_pct if portfolio_intent else cfg.risk_budget_pct
                    ),
                    "max_notional_usdt": cfg.max_notional_usdt,
                    "leverage_cap": cfg.leverage_cap,
                },
                "entry": {
                    "order_type": cfg.order_type,
                    "reference_price": reference_price,
                    "limit_price": limit_price if cfg.order_type == "LIMIT" else None,
                    "price_band_bps": cfg.max_live_price_deviation_bps,
                    "max_slippage_bps": decision.cost.slippage_bps,
                    "max_wait_sec": 90,
                },
                "protection": {
                    "stop_loss": {
                        "type": "MARK_PRICE",
                        "price": stop_price,
                        "max_loss_bps": stop_bps,
                    },
                    "take_profit": [{"price": take_profit_price, "close_fraction": 0.8}],
                    "trailing_stop": {"enabled": False},
                    "max_holding_sec": forecast.time.horizon_sec,
                },
                "economics": {
                    "expected_return_bps": decision.cost.expected_return_bps,
                    "estimated_fee_bps": decision.cost.fee_bps,
                    "estimated_slippage_bps": decision.cost.slippage_bps,
                    "estimated_funding_bps": decision.cost.funding_bps,
                    "model_error_buffer_bps": decision.cost.model_error_buffer_bps,
                    "expected_return_after_cost_bps": decision.cost.after_cost_bps,
                },
                "guards": {
                    "min_data_quality": cfg.min_data_quality,
                    "observed_data_quality": forecast.quality.data_quality,
                    "max_feature_age_sec": cfg.max_feature_age_sec,
                    "observed_feature_age_sec": forecast.quality.max_feature_age_sec,
                    "max_live_spread_bps": cfg.max_live_spread_bps,
                    "max_live_price_deviation_bps": cfg.max_live_price_deviation_bps,
                    "required_market_regime": [forecast.regime.market_regime],
                    "observed_market_regime": forecast.regime.market_regime,
                    "event_blackout": forecast.regime.event_regime == "blackout",
                    "provisional_reduce_only": forecast.regime.event_regime == "reduce_only",
                    "require_flat_position": supersedes_ticket_id is None,
                    "required_position_version": required_position_version,
                    "forecast_market": forecast.instrument.exchange,
                    "execution_market": "bybit",
                    "require_cross_exchange_basis_check": forecast.time.horizon_sec <= 900,
                },
                "reason": {
                    "regime": forecast.regime.market_regime,
                    "top_factor_scores": top_factors,
                    "event_ids": forecast.evidence.event_ids,
                    "warnings": forecast.evidence.warnings,
                },
            }
        )

    def build_portfolio_ticket(
        self,
        portfolio_intent: PortfolioIntent,
        forecasts: Iterable[ForecastEnvelope],
        *,
        reference_price: float,
        required_position_version: int,
        supersedes_ticket_id: str | None = None,
    ) -> Optional[OperationTicket]:
        if abs(portfolio_intent.target_net_exposure_pct) <= 1e-12:
            return None
        expected_side = "BUY" if portfolio_intent.target_net_exposure_pct > 0 else "SELL"
        by_key = {(item.forecast_id, item.revision): item for item in forecasts}
        candidates = []
        for contribution in portfolio_intent.contributions:
            forecast = by_key.get((contribution.forecast_id, contribution.forecast_revision))
            if not forecast or forecast.instrument.symbol != portfolio_intent.symbol:
                continue
            decision = self.policy.decide(forecast)
            if decision and decision.side == expected_side:
                candidates.append((abs(contribution.weighted_score), forecast))
        if not candidates:
            return None
        representative = max(candidates, key=lambda item: item[0])[1]
        return self.build_open_ticket(
            representative,
            reference_price=reference_price,
            required_position_version=required_position_version,
            supersedes_ticket_id=supersedes_ticket_id,
            portfolio_intent=portfolio_intent,
        )


__all__ = ["TicketBuilder", "TicketPolicyConfig"]
