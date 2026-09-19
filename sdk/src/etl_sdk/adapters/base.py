"""平台适配器抽象: 与 Airflow 完全无关的 HTTP 抽取接口。

真实平台接入 = 实现一个子类(鉴权 + 各实体翻页端点)+ 注册,其余(限流/重试/水位/装载)全部复用。
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    # 仅注解使用;运行时导入会与 extractors 包形成循环依赖
    from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter

logger = structlog.get_logger(__name__)

RETRY_ATTEMPTS = 5
RETRY_WAIT_MIN_SECONDS = 1
RETRY_WAIT_MAX_SECONDS = 120


class RateLimitedError(Exception):
    """平台返回 429。retry_after_seconds 来自 Retry-After 响应头。"""

    def __init__(self, retry_after_seconds: float) -> None:
        super().__init__(f"rate limited, retry after {retry_after_seconds:.0f}s")
        self.retry_after_seconds = retry_after_seconds


class ApiError(Exception):
    """平台返回业务错误(非 429/5xx)。"""


class BasePlatformAdapter(ABC):
    """平台适配器接口。"""

    platform: ClassVar[str] = "base"

    @abstractmethod
    def fetch_orders_page(
        self,
        window_start: datetime,
        window_end: datetime,
        page_params: dict[str, Any],
    ) -> dict[str, Any]:
        """拉取订单一页(窗口 + 翻页参数),返回原始响应 dict(orders + 分页元数据)。"""


class BaseHTTPAdapter(BasePlatformAdapter):
    """HTTP 平台适配器骨架: token 缓存刷新、统一鉴权头、限流、tenacity 重试。"""

    def __init__(
        self,
        *,
        base_url: str,
        client_id: str,
        client_secret: str,
        timeout_seconds: float = 30.0,
        rate_limiter: AdaptiveRateLimiter,
        client: httpx.Client | None = None,
    ) -> None:
        self._client = client or httpx.Client(base_url=base_url, timeout=timeout_seconds)
        self._client_id = client_id
        self._client_secret = client_secret
        self._rate_limiter = rate_limiter
        self._token: str | None = None
        self._token_expires_at = 0.0

    # ---- token 生命周期 ----

    @abstractmethod
    def get_token(self) -> tuple[str, int]:
        """获取 (access_token, expires_in_seconds)。"""

    def _ensure_token(self) -> str:
        """缓存 token,剩余有效期不足 30s 时提前刷新。"""
        if self._token is None or time.time() > self._token_expires_at - 30:
            self._token, expires_in = self.get_token()
            self._token_expires_at = time.time() + expires_in
            logger.info("adapter.token_refresh", platform=self.platform, expires_in=expires_in)
        return self._token

    # ---- 带重试的请求 ----

    @retry(
        # RequestError 覆盖网络/协议/解码等全部请求级瞬态(TransportError/DecodingError/ProtocolError 均其子类)
        retry=retry_if_exception_type(
            (RateLimitedError, httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError)
        ),
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN_SECONDS, max=RETRY_WAIT_MAX_SECONDS),
        reraise=True,
    )
    def _request(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET 请求(限流 + 鉴权 + 429/5xx/超时重试)。JSON 解析失败不重试(数据问题,重试无益)。"""
        if not self._rate_limiter.acquire():
            raise RateLimitedError(retry_after_seconds=60.0)
        headers = {"Authorization": f"Bearer {self._ensure_token()}"}
        try:
            response = self._client.get(path, params=params, headers=headers)
        except httpx.TimeoutException as exc:
            raise exc
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", "5"))
            self._rate_limiter.on_429(retry_after)
            raise RateLimitedError(retry_after_seconds=retry_after)
        if response.status_code >= 500:
            response.raise_for_status()
        if response.status_code >= 400:
            raise ApiError(f"{self.platform} {path} -> HTTP {response.status_code}: {response.text[:200]}")
        self._rate_limiter.on_success()
        try:
            return cast("dict[str, Any]", response.json())
        except json.JSONDecodeError as exc:
            # 坏 JSON 视为瞬态(网关截断/平台异常),包成 DecodingError(属 TransportError)走重试白名单;
            # 重试耗尽仍失败则中断批次 —— 坏数据不进死信(死信只承载"可定位的行"问题)
            raise httpx.DecodingError(f"{self.platform} {path} 返回非法 JSON") from exc
