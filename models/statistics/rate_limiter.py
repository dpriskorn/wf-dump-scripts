import asyncio
import random
import time

class RateLimiter:
    def __init__(self, min_interval: float = 0.2):
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - self._last_call)

            if wait_time > 0:
                await asyncio.sleep(wait_time + random.uniform(0, 0.05))

            self._last_call = time.monotonic()
