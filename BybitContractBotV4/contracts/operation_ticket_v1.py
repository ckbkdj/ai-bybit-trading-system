from __future__ import annotations

from datetime import datetime
from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import Field, field_validator, model_validator

from .common import ContractModel, ensure_utc


class TicketInstrument(ContractModel):
    exchange: Literal["bybit"] = "bybit"
    category: Literal["linear"] = "linear"
    symbol: str = Field(min_length=3, max_length=40)
    settle_coin: str = "USDT"

    @field_validator("symbol", "settle_coin")
    @classmethod
    def uppercase(cls, value: str) -> str:
        return value.strip().upper()


class IntentCore(ContractModel):
    side: Literal["BUY", "SELL"]
    target_exposure_pct: float = Field(ge=0, le=1)
    risk_budget_pct: float = Field(ge=0, le=0.1)
    max_notional_usdt: float = Field(gt=0)
    leverage_cap: float = Field(gt=0, le=100)
    reduce_fraction: Optional[float] = Field(default=None, gt=0, le=1)
    target_order_link_id: Optional[str] = Field(default=None, min_length=8, max_length=36)


class OpenIntent(IntentCore):
    action: Literal["OPEN"]
    position_effect: Literal["OPEN_OR_INCREASE"]

    @model_validator(mode="after")
    def positive_risk(self):
        if self.target_exposure_pct <= 0 or self.risk_budget_pct <= 0:
            raise ValueError("OPEN requires positive target exposure and risk budget")
        return self


class IncreaseIntent(OpenIntent):
    action: Literal["INCREASE"]


class ReduceIntent(IntentCore):
    action: Literal["REDUCE"]
    position_effect: Literal["REDUCE_ONLY"]
    reduce_fraction: float = Field(gt=0, le=1)

    @model_validator(mode="after")
    def no_new_risk(self):
        if self.risk_budget_pct != 0:
            raise ValueError("REDUCE cannot allocate new risk")
        return self


class CloseIntent(IntentCore):
    action: Literal["CLOSE"]
    position_effect: Literal["CLOSE_ONLY"]

    @model_validator(mode="after")
    def closes_to_flat(self):
        if self.target_exposure_pct != 0 or self.risk_budget_pct != 0:
            raise ValueError("CLOSE must target zero exposure with zero new risk")
        return self


class CancelIntent(IntentCore):
    action: Literal["CANCEL"]
    position_effect: Literal["CANCEL_ONLY"]
    target_order_link_id: str = Field(min_length=8, max_length=36)

    @model_validator(mode="after")
    def no_new_risk(self):
        if self.target_exposure_pct != 0 or self.risk_budget_pct != 0:
            raise ValueError("CANCEL cannot allocate exposure or risk")
        return self


class ReplaceIntent(OpenIntent):
    action: Literal["REPLACE"]
    position_effect: Literal["REPLACE_ONLY"]


TicketIntent = Annotated[
    Union[OpenIntent, IncreaseIntent, ReduceIntent, CloseIntent, CancelIntent, ReplaceIntent],
    Field(discriminator="action"),
]


class TicketEntry(ContractModel):
    order_type: Literal["MARKET", "LIMIT"]
    reference_price: float = Field(gt=0)
    limit_price: Optional[float] = Field(default=None, gt=0)
    price_band_bps: float = Field(ge=0, le=10_000)
    max_slippage_bps: float = Field(ge=0, le=10_000)
    time_in_force: Literal["GTC", "IOC", "FOK", "POST_ONLY"] = "GTC"
    post_only: bool = False
    max_wait_sec: int = Field(gt=0, le=86_400)

    @model_validator(mode="after")
    def limit_requirements(self):
        if self.order_type == "LIMIT" and self.limit_price is None:
            raise ValueError("LIMIT order requires limit_price")
        if self.post_only and self.order_type != "LIMIT":
            raise ValueError("post_only is valid only for LIMIT orders")
        return self


class StopLoss(ContractModel):
    type: Literal["MARK_PRICE", "LAST_PRICE", "INDEX_PRICE"]
    price: float = Field(gt=0)
    max_loss_bps: float = Field(gt=0, le=10_000)


class TakeProfitLevel(ContractModel):
    price: float = Field(gt=0)
    close_fraction: float = Field(gt=0, le=1)


class TrailingStop(ContractModel):
    enabled: bool = False
    activation_price: Optional[float] = Field(default=None, gt=0)
    distance_bps: Optional[float] = Field(default=None, gt=0, le=10_000)

    @model_validator(mode="after")
    def enabled_fields(self):
        if self.enabled and (self.activation_price is None or self.distance_bps is None):
            raise ValueError("enabled trailing stop requires activation_price and distance_bps")
        return self


class TicketProtection(ContractModel):
    stop_loss: Optional[StopLoss] = None
    take_profit: List[TakeProfitLevel] = Field(default_factory=list, max_length=8)
    trailing_stop: TrailingStop = Field(default_factory=TrailingStop)
    max_holding_sec: int = Field(gt=0, le=31_536_000)

    @model_validator(mode="after")
    def fractions(self):
        if sum(level.close_fraction for level in self.take_profit) > 1.0 + 1e-9:
            raise ValueError("take-profit close fractions cannot exceed one")
        return self


class TicketEconomics(ContractModel):
    expected_return_bps: float
    estimated_fee_bps: float = Field(ge=0)
    estimated_slippage_bps: float = Field(ge=0)
    estimated_funding_bps: float = Field(ge=0)
    model_error_buffer_bps: float = Field(ge=0)
    expected_return_after_cost_bps: float

    @model_validator(mode="after")
    def after_cost(self):
        expected = self.expected_return_bps - self.estimated_fee_bps - self.estimated_slippage_bps
        expected -= self.estimated_funding_bps + self.model_error_buffer_bps
        if abs(expected - self.expected_return_after_cost_bps) > 0.01:
            raise ValueError("expected_return_after_cost_bps is inconsistent")
        return self


class TicketGuards(ContractModel):
    min_data_quality: float = Field(ge=0, le=1)
    observed_data_quality: float = Field(ge=0, le=1)
    max_feature_age_sec: int = Field(gt=0)
    observed_feature_age_sec: int = Field(ge=0)
    max_live_spread_bps: float = Field(ge=0)
    max_live_price_deviation_bps: float = Field(ge=0)
    required_market_regime: List[str] = Field(default_factory=list)
    observed_market_regime: str
    event_blackout: bool = False
    provisional_reduce_only: bool = False
    require_flat_position: bool = False
    required_position_version: int = Field(ge=0)
    execution_market: Literal["bybit"] = "bybit"
    forecast_market: str = "binance"
    require_cross_exchange_basis_check: bool = False
    max_cross_exchange_basis_bps: float = Field(default=25.0, ge=0)
    observed_cross_exchange_basis_bps: Optional[float] = Field(default=None, ge=0)


class TicketReason(ContractModel):
    regime: str
    top_factor_scores: Dict[str, float] = Field(default_factory=dict)
    event_ids: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class OperationTicket(ContractModel):
    schema_version: Literal["operation-ticket.v1"] = "operation-ticket.v1"
    ticket_id: str = Field(min_length=8, max_length=80)
    forecast_id: str = Field(min_length=8, max_length=80)
    forecast_revision: int = Field(ge=1)
    portfolio_decision_id: str = Field(min_length=8, max_length=80)
    strategy_release_id: str = Field(min_length=8, max_length=120)
    supersedes_ticket_id: Optional[str] = Field(default=None, min_length=8, max_length=80)
    created_at: datetime
    valid_from: datetime
    expires_at: datetime
    instrument: TicketInstrument
    intent: TicketIntent
    entry: Optional[TicketEntry]
    protection: Optional[TicketProtection]
    economics: TicketEconomics
    guards: TicketGuards
    reason: TicketReason

    @field_validator("created_at", "valid_from", "expires_at")
    @classmethod
    def utc_datetimes(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @model_validator(mode="after")
    def risk_requirements(self):
        if not (self.created_at <= self.valid_from < self.expires_at):
            raise ValueError("ticket validity interval is invalid")
        if self.supersedes_ticket_id == self.ticket_id:
            raise ValueError("a ticket cannot supersede itself")
        if self.intent.action == "REPLACE" and not self.supersedes_ticket_id:
            raise ValueError("REPLACE requires supersedes_ticket_id")
        if self.intent.action in {"OPEN", "INCREASE", "REPLACE"}:
            if self.entry is None or self.protection is None or self.protection.stop_loss is None:
                raise ValueError("risk-increasing tickets require entry and stop_loss")
            if self.intent.risk_budget_pct <= 0 or self.intent.target_exposure_pct <= 0:
                raise ValueError("risk-increasing tickets require positive risk fields")
            if self.economics.expected_return_after_cost_bps <= 0:
                raise ValueError("risk-increasing tickets require positive after-cost return")
            if self.guards.observed_data_quality < self.guards.min_data_quality:
                raise ValueError("observed data quality is below the ticket minimum")
            if self.guards.observed_feature_age_sec > self.guards.max_feature_age_sec:
                raise ValueError("observed feature age exceeds the ticket maximum")
        if self.intent.action in {"REDUCE", "CLOSE"} and self.entry is None:
            raise ValueError("risk-reducing tickets require a live reference entry")
        if self.intent.action == "CANCEL" and (self.entry is not None or self.protection is not None):
            raise ValueError("CANCEL cannot carry entry or protection instructions")
        return self
