from __future__ import annotations

from datetime import datetime
from typing import List, Literal

from pydantic import Field, field_validator, model_validator

from .common import ContractModel, ensure_utc


class HorizonContribution(ContractModel):
    forecast_id: str = Field(min_length=8, max_length=80)
    forecast_revision: int = Field(ge=1)
    horizon_sec: int = Field(gt=0, le=31_536_000)
    direction_score: float = Field(ge=-1, le=1)
    expected_return_bps: float
    quality_weight: float = Field(gt=0, le=1)
    horizon_weight: float = Field(gt=0, le=1)
    weighted_score: float = Field(ge=-1, le=1)


class PortfolioIntent(ContractModel):
    """One netted portfolio decision built from several forecast horizons.

    This is deliberately an exposure target, not an order request.  The execution
    planner may create one or more tickets from it, but individual horizons must
    never bypass this boundary.
    """

    schema_version: Literal["portfolio-intent.v1"] = "portfolio-intent.v1"
    portfolio_decision_id: str = Field(min_length=8, max_length=80)
    strategy_release_id: str = Field(min_length=8, max_length=120)
    symbol: str = Field(min_length=3, max_length=40)
    created_at: datetime
    valid_until: datetime
    decision_version: int = Field(ge=1)
    target_net_exposure_pct: float = Field(ge=-1, le=1)
    target_long_exposure_pct: float = Field(ge=0, le=1)
    target_short_exposure_pct: float = Field(ge=0, le=1)
    risk_budget_pct: float = Field(ge=0, le=0.1)
    max_turnover_pct: float = Field(ge=0, le=1)
    hedge_mode: Literal["DISABLED", "EXPLICIT"] = "DISABLED"
    hedge_owner_id: str | None = Field(default=None, min_length=8, max_length=120)
    contributions: List[HorizonContribution] = Field(min_length=2, max_length=16)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("created_at", "valid_until")
    @classmethod
    def datetimes_are_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @model_validator(mode="after")
    def portfolio_is_consistent(self):
        if self.valid_until <= self.created_at:
            raise ValueError("valid_until must be after created_at")
        expected_net = self.target_long_exposure_pct - self.target_short_exposure_pct
        if abs(expected_net - self.target_net_exposure_pct) > 1e-9:
            raise ValueError("target net exposure must equal long exposure minus short exposure")
        if self.hedge_mode == "DISABLED":
            if self.hedge_owner_id is not None:
                raise ValueError("disabled hedge mode cannot declare a hedge owner")
            if self.target_long_exposure_pct > 0 and self.target_short_exposure_pct > 0:
                raise ValueError("simultaneous long/short exposure requires explicit hedge ownership")
        elif not self.hedge_owner_id:
            raise ValueError("explicit hedge mode requires hedge_owner_id")
        horizons = [item.horizon_sec for item in self.contributions]
        if len(horizons) != len(set(horizons)):
            raise ValueError("a SignalBook may contribute at most one forecast per horizon")
        return self
