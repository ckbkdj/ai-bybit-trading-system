from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from contracts.forecast_v1 import ForecastEnvelope

from .cost_model import CostEstimate, CostModel


@dataclass(frozen=True)
class TicketPolicyConfig:
    min_direction_probability: float = 0.58
    min_probability_margin: float = 0.12
    min_data_quality: float = 0.90
    max_feature_age_sec: int = 120
    max_ood_score: float = 0.35
    min_after_cost_bps: float = 0.0
    ticket_ttl_sec: int = 300
    risk_budget_pct: float = 0.0025
    target_exposure_pct: float = 0.05
    max_notional_usdt: float = 5000.0
    leverage_cap: float = 2.0
    max_live_spread_bps: float = 10.0
    max_live_price_deviation_bps: float = 18.0
    default_stop_bps: float = 100.0
    default_take_profit_multiple: float = 1.8
    order_type: str = "LIMIT"


@dataclass(frozen=True)
class TicketDecision:
    side: str
    gross_edge_bps: float
    cost: CostEstimate
    lower_bound_net_edge_bps: float


class TicketPolicy:
    def __init__(self, config: TicketPolicyConfig | None = None, cost_model: CostModel | None = None):
        self.config = config or TicketPolicyConfig()
        self.cost_model = cost_model or CostModel()

    def decide(self, forecast: ForecastEnvelope) -> Optional[TicketDecision]:
        quality = forecast.quality
        if quality.source_status != "ok" or quality.calibration_status != "valid":
            return None
        if quality.data_quality < self.config.min_data_quality:
            return None
        if quality.max_feature_age_sec > self.config.max_feature_age_sec:
            return None
        if quality.range_guard_score > self.config.max_ood_score:
            return None

        distribution = forecast.distribution
        up_margin = distribution.p_up - max(distribution.p_flat, distribution.p_down)
        down_margin = distribution.p_down - max(distribution.p_flat, distribution.p_up)
        expected = distribution.expected_return_bps
        if expected is None:
            return None
        if distribution.p_up >= self.config.min_direction_probability and up_margin >= self.config.min_probability_margin and expected > 0:
            side, gross_edge = "BUY", expected
        elif distribution.p_down >= self.config.min_direction_probability and down_margin >= self.config.min_probability_margin and expected < 0:
            side, gross_edge = "SELL", abs(expected)
        else:
            return None

        cost = self.cost_model.estimate(
            gross_edge,
            order_type=self.config.order_type,
            expected_mae_bps=distribution.expected_mae_bps,
        )
        if cost.after_cost_bps < self.config.min_after_cost_bps:
            return None
        quantiles = distribution.return_quantiles_bps
        if quantiles is None:
            return None
        lower_gross_edge = float(quantiles.p10 if side == "BUY" else -quantiles.p90)
        lower_bound_net_edge = (
            lower_gross_edge
            - cost.fee_bps
            - cost.slippage_bps
            - cost.funding_bps
            - cost.model_error_buffer_bps
        )
        if lower_bound_net_edge <= 0:
            return None
        return TicketDecision(
            side=side,
            gross_edge_bps=gross_edge,
            cost=cost,
            lower_bound_net_edge_bps=lower_bound_net_edge,
        )
