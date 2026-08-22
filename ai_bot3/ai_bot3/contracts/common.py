from __future__ import annotations

import base64
import hashlib
import os
import time
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class ContractModel(BaseModel):
    """All transport contracts are immutable and reject unknown fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


class UtcModel(ContractModel):
    @field_validator("*", mode="before")
    @classmethod
    def normalize_datetime_strings(cls, value: Any, info):
        if not info.field_name.endswith(("_at", "_from")):
            return value
        if isinstance(value, str) and value.endswith(("Z", "z")):
            return value[:-1] + "+00:00"
        return value


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must include a timezone")
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sortable_id(prefix: str) -> str:
    """Return a compact time-sortable id without a runtime ULID dependency."""

    timestamp = int(time.time() * 1000).to_bytes(6, "big", signed=False)
    entropy = os.urandom(10)
    token = base64.b32encode(timestamp + entropy).decode("ascii").rstrip("=").lower()
    return f"{prefix}_{token}"


def deterministic_id(prefix: str, *parts: object, length: int = 26) -> str:
    digest = hashlib.sha256("\x1f".join(str(part) for part in parts).encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=").lower()
    return f"{prefix}_{token[:length]}"
