from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Mapping, Optional


class RateLimitBlocked(RuntimeError):
    def __init__(self, retry_after_seconds: float):
        super().__init__(f"rate limit blocked for {retry_after_seconds:.3f}s")
        self.retry_after_seconds = retry_after_seconds


@dataclass
class LimitWindow:
    remaining: Optional[int] = None
    reset_at_monotonic: float = 0.0
    next_allowed_monotonic: float = 0.0


class EndpointRateLimiter:
    """UID/endpoint limiter updated from Bybit response headers; it never blind-retries."""

    def __init__(self, minimum_interval_seconds: float = 0.0, clock=time.monotonic):
        self.minimum_interval_seconds = max(0.0, float(minimum_interval_seconds))
        self.clock = clock
        self._windows: dict[tuple[str, str], LimitWindow] = {}
        self._lock = threading.Lock()

    def acquire(self, uid: str, endpoint: str) -> None:
        key = (uid, endpoint)
        with self._lock:
            now = self.clock()
            window = self._windows.setdefault(key, LimitWindow())
            blocked_until = max(window.reset_at_monotonic if window.remaining == 0 else 0, window.next_allowed_monotonic)
            if blocked_until > now:
                raise RateLimitBlocked(blocked_until - now)
            window.next_allowed_monotonic = now + self.minimum_interval_seconds

    def update_from_headers(self, uid: str, endpoint: str, headers: Mapping[str, str]) -> None:
        lower = {str(key).lower(): str(value) for key, value in headers.items()}
        remaining_text = lower.get("x-bapi-limit-status")
        reset_text = lower.get("x-bapi-limit-reset-timestamp")
        retry_after = lower.get("retry-after")
        with self._lock:
            now = self.clock()
            window = self._windows.setdefault((uid, endpoint), LimitWindow())
            try:
                window.remaining = int(remaining_text) if remaining_text is not None else window.remaining
            except ValueError:
                pass
            if retry_after is not None:
                try:
                    window.remaining = 0
                    window.reset_at_monotonic = max(window.reset_at_monotonic, now + float(retry_after))
                except ValueError:
                    pass
            elif reset_text is not None and window.remaining == 0:
                try:
                    reset_seconds = float(reset_text) / 1000.0
                    wall_delta = max(0.0, reset_seconds - time.time())
                    window.reset_at_monotonic = max(window.reset_at_monotonic, now + wall_delta)
                except ValueError:
                    pass
