"""真实限流: 令牌桶中间件,让 SDK 的限流逻辑被真实压测(M1 最小版,仅 /orders)。"""

from __future__ import annotations

import threading
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp


class TokenBucket:
    """线程安全令牌桶。rate=每分钟补充速率,burst=桶容量。"""

    def __init__(self, rate_per_min: int, burst: int) -> None:
        self.rate_per_sec = rate_per_min / 60.0
        self.burst = float(burst)
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate_per_sec)
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class RateLimitMiddleware(BaseHTTPMiddleware):
    """对 /orders 系列路径限流,超限返回 429 + Retry-After。"""

    def __init__(self, app: ASGIApp, rate_per_min: int, burst: int) -> None:
        super().__init__(app)
        self._bucket = TokenBucket(rate_per_min, burst)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path.startswith("/orders") and not self._bucket.try_acquire():
            return JSONResponse(
                status_code=429,
                content={"code": 429, "msg": "rate limited"},
                headers={"Retry-After": "2"},
            )
        return await call_next(request)
