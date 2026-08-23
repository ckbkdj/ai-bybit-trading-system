from __future__ import annotations

from datetime import datetime
from typing import Dict, Literal

from pydantic import Field, field_validator, model_validator

from .common import ContractModel, ensure_utc


Sha256 = str


class StrategyArtifactHashes(ContractModel):
    brain_model_sha256: Sha256
    lstm_model_sha256: Sha256
    scaler_sha256: Sha256
    calibration_sha256: Sha256
    feature_schema_sha256: Sha256
    factor_weights_sha256: Sha256
    cost_policy_sha256: Sha256
    ticket_policy_sha256: Sha256
    execution_policy_sha256: Sha256
    training_snapshot_sha256: Sha256
    evidence_bundle_sha256: Sha256

    @field_validator("*")
    @classmethod
    def valid_sha256(cls, value: str) -> str:
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError("artifact hashes must be lowercase SHA-256 hex")
        return normalized


class StrategyReleaseBundle(ContractModel):
    schema_version: Literal["strategy-release-bundle.v1"] = "strategy-release-bundle.v1"
    strategy_release_id: str = Field(min_length=8, max_length=120)
    release_stage: Literal["shadow", "candidate", "live", "retired"]
    created_at: datetime
    code_commit: str = Field(min_length=7, max_length=80)
    artifacts: StrategyArtifactHashes
    artifact_paths: Dict[str, str] = Field(default_factory=dict)
    immutable_limits: Dict[str, float] = Field(default_factory=dict)
    approval_id: str = Field(min_length=8, max_length=120)
    approved_by: str = Field(min_length=3, max_length=120)
    bundle_sha256: Sha256

    @field_validator("created_at")
    @classmethod
    def datetime_is_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @field_validator("bundle_sha256")
    @classmethod
    def valid_bundle_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError("bundle_sha256 must be lowercase SHA-256 hex")
        return normalized

    @model_validator(mode="after")
    def artifact_paths_are_known(self):
        known = set(type(self.artifacts).model_fields)
        unknown = set(self.artifact_paths) - known
        if unknown:
            raise ValueError(f"artifact_paths contains unknown artifact keys: {sorted(unknown)}")
        return self
