"""平台适配器: 抽象基类 + 注册表 + 各平台实现。"""

from etl_sdk.adapters.base import ApiError, BaseHTTPAdapter, BasePlatformAdapter, RateLimitedError
from etl_sdk.adapters.mock import MockPlatformAdapter
from etl_sdk.adapters.registry import get_adapter, register

__all__ = [
    "ApiError",
    "BaseHTTPAdapter",
    "BasePlatformAdapter",
    "RateLimitedError",
    "MockPlatformAdapter",
    "get_adapter",
    "register",
]
