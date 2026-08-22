from __future__ import annotations

import base64
import hashlib
import os
import time
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must include a timezone")
    return value.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sortable_id(prefix: str) -> str:
    timestamp = int(time.time() * 1000).to_bytes(6, "big", signed=False)
    token = base64.b32encode(timestamp + os.urandom(10)).decode("ascii").rstrip("=").lower()
    return f"{prefix}_{token}"


def order_link_id(ticket_id: str, role: str = "entry") -> str:
    digest = hashlib.sha256(f"{ticket_id}:{role}".encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=").lower()
    # Bybit's orderLinkId maximum is 36 characters.
    return f"qt_{token[:30]}"[:36]
