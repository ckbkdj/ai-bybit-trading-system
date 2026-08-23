from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import Field, field_validator

from .common import ContractModel, ensure_utc


ExecutionStatus = Literal[
    "RECEIVED", "VALIDATED", "CLAIMED", "RISK_APPROVED", "SUBMITTING", "SUBMITTED",
    "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "EXPIRED", "CANCELLED",
    "FAILED", "SUPERSEDED", "RISK_BLOCKED",
]


class ReceiptOrder(ContractModel):
    order_link_id: str
    role: str = "entry"
    bybit_order_id: Optional[str] = None
    order_status: str
    side: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT"]
    quantity: float = Field(ge=0)
    price: Optional[float] = Field(default=None, ge=0)
    cum_exec_qty: float = Field(ge=0)
    avg_exec_price: Optional[float] = Field(default=None, ge=0)


class ReceiptFill(ContractModel):
    exec_id: str
    order_link_id: str
    quantity: float = Field(gt=0)
    price: float = Field(gt=0)
    exec_fee: float = Field(ge=0)
    executed_at: datetime

    @field_validator("executed_at")
    @classmethod
    def utc_datetime(cls, value: datetime) -> datetime:
        return ensure_utc(value)


class ExecutionReceipt(ContractModel):
    schema_version: Literal["execution-receipt.v1"] = "execution-receipt.v1"
    receipt_id: str = Field(min_length=8, max_length=80)
    ticket_id: str = Field(min_length=8, max_length=80)
    consumer_id: str = Field(min_length=1, max_length=80)
    mode: Literal["shadow", "testnet", "live"]
    status: ExecutionStatus
    reason_code: Optional[str] = None
    reason_detail: Optional[str] = None
    orders: List[ReceiptOrder] = Field(default_factory=list)
    fills: List[ReceiptFill] = Field(default_factory=list)
    position_version_before: Optional[int] = Field(default=None, ge=0)
    position_version_after: Optional[int] = Field(default=None, ge=0)
    position_qty_after: Optional[float] = None
    account_equity_usdt: Optional[float] = Field(default=None, ge=0)
    total_exec_fee: float = Field(default=0, ge=0)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def utc_datetimes(cls, value: datetime) -> datetime:
        return ensure_utc(value)
