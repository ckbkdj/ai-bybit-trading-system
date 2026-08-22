import asyncio
import time


class AsyncRateLimiter:
    """最简单的 asyncio 令牌桶（按最小间隔实现）"""
    def __init__(self, interval: float):
        self.interval = interval
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.interval:
                await asyncio.sleep(self.interval - delta)
            self._last = time.time()
