from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import Field, field_validator

from .common import ContractModel, ensure_utc


class ExecutionAwareLabel(ContractModel):
    schema_version: Literal["execution-aware-label.v1"] = "execution-aware-label.v1"
    label_id: str = Field(min_length=8, max_length=80)
    source_receipt_id: str = Field(min_length=8, max_length=80)
    ticket_id: str = Field(min_length=8, max_length=80)
    portfolio_decision_id: str = Field(min_length=8, max_length=80)
    strategy_release_id: str = Field(min_length=8, max_length=120)
    symbol: str
    side: Literal["BUY", "SELL"]
    requested_quantity: float = Field(gt=0)
    filled_quantity: float = Field(ge=0)
    entry_fill_fraction: float = Field(ge=0, le=1)
    time_to_first_fill_sec: Optional[float] = Field(default=None, ge=0)
    time_to_full_fill_sec: Optional[float] = Field(default=None, ge=0)
    partial_fill: bool
    mfe_bps: Optional[float] = Field(default=None, ge=0)
    mae_bps: Optional[float] = Field(default=None, ge=0)
    first_barrier: Literal["TAKE_PROFIT", "STOP_LOSS", "TIME", "NONE", "UNOBSERVED"]
    fee_bps: float = Field(ge=0)
    slippage_bps: float = Field(ge=0)
    funding_bps: float
    realised_cost_bps: float
    exit_reason: str
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def datetime_is_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)
