"""适配器注册表: 按平台名解析适配器工厂;新平台接入 = 实现 + 注册一行。

工厂(而非类)入册,便于构造时注入限流器/httpx client 等依赖。
"""

from __future__ import annotations

from collections.abc import Callable

from etl_sdk.adapters.base import BasePlatformAdapter

_REGISTRY: dict[str, Callable[..., BasePlatformAdapter]] = {}


def register(platform: str, factory: Callable[..., BasePlatformAdapter]) -> None:
    if platform in _REGISTRY:
        raise ValueError(f"platform {platform!r} already registered")
    _REGISTRY[platform] = factory


def get_adapter(platform: str, **kwargs: object) -> BasePlatformAdapter:
    factory = _REGISTRY.get(platform)
    if factory is None:
        raise ValueError(f"unknown platform {platform!r}; available: {sorted(_REGISTRY)}")
    return factory(**kwargs)
