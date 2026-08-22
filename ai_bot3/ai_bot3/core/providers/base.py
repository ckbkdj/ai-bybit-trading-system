from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Generic, Optional, TypeVar


T = TypeVar("T")


class ProviderStatus(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    OUTAGE = "outage"


@dataclass(frozen=True)
class ProviderResult(Generic[T]):
    status: ProviderStatus
    data: Optional[T]
    generated_at: datetime
    source: str
    warnings: tuple[str, ...] = field(default_factory=tuple)
    error: Optional[str] = None

    @classmethod
    def failure(cls, source: str, error: str) -> "ProviderResult[T]":
        return cls(ProviderStatus.OUTAGE, None, datetime.now(timezone.utc), source, error=error)


class Provider(ABC, Generic[T]):
    name: str

    @abstractmethod
    def fetch(self, *, as_of: datetime) -> ProviderResult[T]:
        raise NotImplementedError
