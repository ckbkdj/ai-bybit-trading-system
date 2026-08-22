from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from .common import ContractModel, ensure_utc


class ForecastInstrument(ContractModel):
    asset_class: str = "crypto"
    exchange: str = "bybit"
    category: str = "linear"
    symbol: str = Field(min_length=3, max_length=40)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized.isalnum():
            raise ValueError("symbol must be alphanumeric")
        return normalized


class ForecastTime(ContractModel):
    created_at: datetime
    data_cutoff: datetime
    horizon_sec: int = Field(gt=0, le=31_536_000)
    forecast_target_at: datetime

    @field_validator("created_at", "data_cutoff", "forecast_target_at")
    @classmethod
    def datetimes_are_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @model_validator(mode="after")
    def chronological(self):
        if self.data_cutoff > self.created_at + timedelta(seconds=5):
            raise ValueError("data_cutoff cannot be after forecast creation")
        expected = self.data_cutoff + timedelta(seconds=self.horizon_sec)
        if abs((self.forecast_target_at - expected).total_seconds()) > 5:
            raise ValueError("forecast_target_at must equal data_cutoff + horizon_sec")
        return self


class ReturnQuantiles(ContractModel):
    p10: float
    p25: float
    p50: float
    p75: float
    p90: float

    @model_validator(mode="after")
    def ordered(self):
        values = [self.p10, self.p25, self.p50, self.p75, self.p90]
        if values != sorted(values):
            raise ValueError("return quantiles must be monotonic")
        return self


class ForecastDistribution(ContractModel):
    p_up: float = Field(ge=0, le=1)
    p_flat: float = Field(ge=0, le=1)
    p_down: float = Field(ge=0, le=1)
    return_quantiles_bps: Optional[ReturnQuantiles] = None
    expected_return_bps: Optional[float] = None
    expected_volatility_bps: Optional[float] = Field(default=None, ge=0)
    expected_mae_bps: Optional[float] = Field(default=None, ge=0)
    expected_mfe_bps: Optional[float] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def probabilities_sum_to_one(self):
        total = self.p_up + self.p_flat + self.p_down
        if abs(total - 1.0) > 1e-6:
            raise ValueError("direction probabilities must sum to one")
        return self


class ForecastRegime(ContractModel):
    market_regime: str = "unknown"
    liquidity_regime: str = "unknown"
    event_regime: str = "normal"


class ForecastQuality(ContractModel):
    data_coverage: float = Field(ge=0, le=1)
    data_quality: float = Field(ge=0, le=1)
    calibration_status: Literal["valid", "degraded", "invalid", "unknown"]
    out_of_distribution_score: float = Field(ge=0, le=1)
    max_feature_age_sec: int = Field(ge=0)
    prediction_interval_coverage_target: Optional[float] = Field(default=None, gt=0, le=1)
    source_status: Literal["ok", "degraded", "missing", "error"] = "ok"


class ForecastEvidence(ContractModel):
    top_positive_drivers: List[str] = Field(default_factory=list)
    top_negative_drivers: List[str] = Field(default_factory=list)
    event_ids: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class ForecastLineage(ContractModel):
    model_bundle_id: str = Field(min_length=1)
    feature_set_id: str = Field(min_length=1)
    calibration_model_id: str = Field(min_length=1)
    code_commit: str = Field(min_length=1)


class ForecastEnvelope(ContractModel):
    schema_version: Literal["forecast-envelope.v1"] = "forecast-envelope.v1"
    forecast_id: str = Field(min_length=8, max_length=80)
    revision: int = Field(ge=1)
    instrument: ForecastInstrument
    time: ForecastTime
    distribution: ForecastDistribution
    regime: ForecastRegime
    quality: ForecastQuality
    factor_scores: Dict[str, float] = Field(default_factory=dict)
    evidence: ForecastEvidence = Field(default_factory=ForecastEvidence)
    lineage: ForecastLineage

    @field_validator("factor_scores")
    @classmethod
    def factor_scores_are_bounded(cls, value: Dict[str, float]) -> Dict[str, float]:
        if any(score < -1 or score > 1 for score in value.values()):
            raise ValueError("factor scores must be in [-1, 1]")
        return value
