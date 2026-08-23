from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping

from contracts.common import deterministic_id
from contracts.forecast_v1 import ForecastEnvelope
from contracts.portfolio_intent_v1 import HorizonContribution, PortfolioIntent


@dataclass(frozen=True)
class PortfolioIntentPolicy:
    horizon_weights: Mapping[int, float] | None = None
    min_horizons: int = 2
    min_data_quality: float = 0.90
    max_range_guard_score: float = 0.35
    max_target_exposure_pct: float = 0.08
    risk_budget_pct: float = 0.003
    max_turnover_pct: float = 0.08
    decision_deadband: float = 0.12
    ttl_sec: int = 300

    def weight_for(self, horizon_sec: int) -> float:
        configured = dict(self.horizon_weights or {})
        if horizon_sec in configured:
            return max(1e-9, float(configured[horizon_sec]))
        # Longer forecasts provide regime context but cannot overwhelm the near-term signal.
        return 1.0 / max(1.0, (horizon_sec / 900.0) ** 0.5)


class SignalBook:
    """Select the newest, eligible forecast for every horizon of one symbol/release."""

    def __init__(self, forecasts: Iterable[ForecastEnvelope], *, now: datetime | None = None):
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        selected: dict[int, ForecastEnvelope] = {}
        for forecast in forecasts:
            if forecast.time.created_at > current + timedelta(seconds=5):
                continue
            previous = selected.get(forecast.time.horizon_sec)
            if previous is None or (forecast.time.created_at, forecast.revision) > (
                previous.time.created_at,
                previous.revision,
            ):
                selected[forecast.time.horizon_sec] = forecast
        self.forecasts = tuple(selected[key] for key in sorted(selected))


class PortfolioIntentBuilder:
    def __init__(self, policy: PortfolioIntentPolicy | None = None):
        self.policy = policy or PortfolioIntentPolicy()

    def build(
        self,
        forecasts: Iterable[ForecastEnvelope],
        *,
        strategy_release_id: str,
        decision_version: int,
        now: datetime | None = None,
    ) -> PortfolioIntent | None:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        book = SignalBook(forecasts, now=current).forecasts
        eligible = [item for item in book if self._eligible(item, strategy_release_id)]
        if len(eligible) < self.policy.min_horizons:
            return None
        symbols = {item.instrument.symbol for item in eligible}
        if len(symbols) != 1:
            raise ValueError("one SignalBook cannot mix symbols")

        raw: list[tuple[ForecastEnvelope, float, float, float]] = []
        for forecast in eligible:
            direction = forecast.distribution.p_up - forecast.distribution.p_down
            quality = forecast.quality.data_quality
            horizon_weight = self.policy.weight_for(forecast.time.horizon_sec)
            raw.append((forecast, direction, quality, horizon_weight))
        denominator = sum(quality * weight for _, _, quality, weight in raw)
        if denominator <= 0:
            return None
        net_score = sum(direction * quality * weight for _, direction, quality, weight in raw) / denominator
        if abs(net_score) < self.policy.decision_deadband:
            target_net = 0.0
        else:
            target_net = self.policy.max_target_exposure_pct * min(1.0, abs(net_score))
            if net_score < 0:
                target_net = -target_net
        target_long = max(0.0, target_net)
        target_short = max(0.0, -target_net)

        contributions = []
        for forecast, direction, quality, horizon_weight in raw:
            normalized_weight = quality * horizon_weight / denominator
            contributions.append(
                HorizonContribution(
                    forecast_id=forecast.forecast_id,
                    forecast_revision=forecast.revision,
                    horizon_sec=forecast.time.horizon_sec,
                    direction_score=direction,
                    expected_return_bps=float(forecast.distribution.expected_return_bps or 0.0),
                    quality_weight=quality,
                    horizon_weight=min(1.0, horizon_weight),
                    weighted_score=direction * normalized_weight,
                )
            )
        created_at = max(item.time.created_at for item in eligible)
        valid_until = min(created_at + timedelta(seconds=self.policy.ttl_sec), min(
            item.time.forecast_target_at for item in eligible
        ))
        decision_id = deterministic_id(
            "pd",
            strategy_release_id,
            next(iter(symbols)),
            decision_version,
            *(f"{item.forecast_id}:{item.revision}" for item in eligible),
        )
        return PortfolioIntent(
            portfolio_decision_id=decision_id,
            strategy_release_id=strategy_release_id,
            symbol=next(iter(symbols)),
            created_at=created_at,
            valid_until=valid_until,
            decision_version=decision_version,
            target_net_exposure_pct=target_net,
            target_long_exposure_pct=target_long,
            target_short_exposure_pct=target_short,
            risk_budget_pct=self.policy.risk_budget_pct if target_net else 0.0,
            max_turnover_pct=self.policy.max_turnover_pct,
            contributions=contributions,
        )

    def _eligible(self, forecast: ForecastEnvelope, strategy_release_id: str) -> bool:
        return (
            forecast.lineage.strategy_release_id == strategy_release_id
            and forecast.quality.source_status == "ok"
            and forecast.quality.calibration_status == "valid"
            and forecast.quality.data_quality >= self.policy.min_data_quality
            and forecast.quality.range_guard_score <= self.policy.max_range_guard_score
            and forecast.distribution.expected_return_bps is not None
            and forecast.regime.event_regime not in {"blackout", "reduce_only"}
        )
