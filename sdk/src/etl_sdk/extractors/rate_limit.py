"""SDK 侧限流: 令牌桶 + 429 自适应降速。

- 每个请求先过桶(阻塞等待到令牌可用)
- 收到 429 时按 Retry-After 扣透支(penalty),连续 3 次 429 自动降速 20%
- 连续 10 分钟无 429 时速率缓慢回升(慢启动恢复)
"""

from __future__ import annotations

import threading
import time

from structlog import get_logger

logger = get_logger(__name__)

ADAPTIVE_CONSECUTIVE_429 = 3
ADAPTIVE_SLOWDOWN_FACTOR = 0.8
ADAPTIVE_RECOVER_AFTER_SECONDS = 600
ADAPTIVE_RECOVER_FACTOR = 1.1


class TokenBucket:
    """线程安全令牌桶。rate_per_min=每分钟补充速率,burst=桶容量(允许短时突发)。"""

    def __init__(self, rate_per_min: int, burst: int | None = None) -> None:
        self.rate_per_sec = rate_per_min / 60.0
        self.burst = float(burst if burst is not None else rate_per_min)
        self._tokens = self.burst
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout_seconds: float = 120.0) -> bool:
        """等待直到拿到令牌;超时返回 False。"""
        deadline = time.monotonic() + timeout_seconds
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate_per_sec)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                wait = (1.0 - self._tokens) / self.rate_per_sec
            if time.monotonic() + wait > deadline:
                return False
            time.sleep(min(wait, 0.2))

    def penalty(self, retry_after_seconds: float) -> None:
        """收到 429 时扣透支: 直接按 Retry-After 时长扣减令牌(可为负)。"""
        with self._lock:
            self._tokens -= max(retry_after_seconds, 0.0) * self.rate_per_sec
            self._last = time.monotonic()


class AdaptiveRateLimiter:
    """带自适应降速的端点级限流器。"""

    def __init__(self, bucket: TokenBucket) -> None:
        self.bucket = bucket
        self.base_rate = bucket.rate_per_sec * 60.0
        self._consecutive_429 = 0
        self._last_429 = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        return self.bucket.acquire()

    def on_429(self, retry_after_seconds: float) -> None:
        """上报 429: 扣透支,并视连续次数降速。"""
        self.bucket.penalty(retry_after_seconds)
        with self._lock:
            now = time.monotonic()
            if now - self._last_429 > ADAPTIVE_RECOVER_AFTER_SECONDS:
                self._consecutive_429 = 0
            self._consecutive_429 += 1
            self._last_429 = now
            if self._consecutive_429 >= ADAPTIVE_CONSECUTIVE_429:
                new_rate = self.bucket.rate_per_sec * 60.0 * ADAPTIVE_SLOWDOWN_FACTOR
                self.bucket.rate_per_sec = new_rate / 60.0
                logger.warning(
                    "rate_limit.slowdown",
                    consecutive_429=self._consecutive_429,
                    rate_per_min=round(new_rate, 1),
                )

    def on_success(self) -> None:
        """长时间无 429 后缓慢恢复速率(不超过基础速率)。"""
        with self._lock:
            now = time.monotonic()
            if (
                self._consecutive_429 > 0
                and now - self._last_429 > ADAPTIVE_RECOVER_AFTER_SECONDS
                and self.bucket.rate_per_sec * 60.0 < self.base_rate
            ):
                self.bucket.rate_per_sec = min(
                    self.base_rate / 60.0,
                    self.bucket.rate_per_sec * ADAPTIVE_RECOVER_FACTOR,
                )
                self._consecutive_429 = 0
                logger.info("rate_limit.recover", rate_per_min=round(self.bucket.rate_per_sec * 60, 1))
