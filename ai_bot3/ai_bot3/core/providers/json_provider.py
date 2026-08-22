from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol

from .base import Provider, ProviderResult, ProviderStatus


class JsonTransport(Protocol):
    def __call__(
        self,
        url: str,
        *,
        params: Mapping[str, Any],
        headers: Mapping[str, str],
        timeout_seconds: float,
        verify: bool | str,
    ) -> Any: ...


@dataclass(frozen=True)
class JsonProviderConfig:
    name: str
    endpoint: str
    source_tier: str
    timeout_seconds: float = 10.0
    verify: bool | str = True
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        if not self.name or not self.endpoint or self.source_tier not in {"A", "B", "C"}:
            raise ValueError("provider name, endpoint and source tier are required")
        if self.timeout_seconds <= 0:
            raise ValueError("provider timeout must be positive")


class JsonProvider(Provider[list[Any]]):
    """Configurable adapter for internal or external JSON feeds with injected I/O and parser."""

    def __init__(
        self,
        config: JsonProviderConfig,
        transport: JsonTransport,
        parser: Callable[[Any, datetime, JsonProviderConfig], list[Any]],
    ):
        self.config = config
        self.name = config.name
        self.transport = transport
        self.parser = parser

    def fetch(self, *, as_of: datetime) -> ProviderResult[list[Any]]:
        cutoff = as_of.astimezone(timezone.utc)
        try:
            payload = self.transport(
                self.config.endpoint,
                params={"as_of": cutoff.isoformat().replace("+00:00", "Z")},
                headers=self.config.headers,
                timeout_seconds=self.config.timeout_seconds,
                verify=self.config.verify,
            )
            records = self.parser(payload, cutoff, self.config)
            if not isinstance(records, list):
                raise TypeError("provider parser must return a list")
            return ProviderResult(ProviderStatus.OK, records, cutoff, self.config.name)
        except Exception as exc:
            return ProviderResult.failure(self.config.name, f"{type(exc).__name__}: {exc}")
