"""令牌桶与自适应降速单元测试(monkeypatch 时间)。"""

from __future__ import annotations

from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket


def test_bucket_penalty_blocks_until_refill(monkeypatch) -> None:
    now = 1000.0
    monkeypatch.setattr("time.monotonic", lambda: now)
    monkeypatch.setattr("time.sleep", lambda s: None)
    # 必须在补丁后构造: 构造时记录的 _last 要与冻结时钟一致
    bucket = TokenBucket(rate_per_min=60, burst=2)

    assert bucket.acquire() is True  # burst 1
    assert bucket.acquire() is True  # burst 2
    assert bucket.acquire(timeout_seconds=0.0) is False  # 空桶

    # penalty: 透支 2s 的量(60/min = 1 token/s)
    bucket.penalty(retry_after_seconds=2.0)
    now += 0.5
    assert bucket.acquire(timeout_seconds=0.0) is False  # -2 + 0.5 还不够 1 个令牌
    now += 2.6  # 累计补充 3.1s,令牌 = -2 + 3.1 = 1.1,足够
    assert bucket.acquire(timeout_seconds=0.0) is True


def test_adaptive_slowdown_after_consecutive_429(monkeypatch) -> None:
    bucket = TokenBucket(rate_per_min=100, burst=100)
    limiter = AdaptiveRateLimiter(bucket)
    now = 1000.0
    monkeypatch.setattr("time.monotonic", lambda: now)

    for _ in range(3):
        limiter.on_429(retry_after_seconds=1.0)
    # 连续 3 次 429 降速 20%: 100 -> 80
    assert round(bucket.rate_per_sec * 60) == 80


def test_adaptive_recovers_after_clean_period(monkeypatch) -> None:
    bucket = TokenBucket(rate_per_min=100, burst=100)
    limiter = AdaptiveRateLimiter(bucket)
    now = 1000.0
    monkeypatch.setattr("time.monotonic", lambda: now)

    for _ in range(3):
        limiter.on_429(retry_after_seconds=1.0)
    now += 601.0  # 超过 10 分钟无 429
    limiter.on_success()
    assert round(bucket.rate_per_sec * 60) == 88  # 80 * 1.1,且不超过基础速率 100
