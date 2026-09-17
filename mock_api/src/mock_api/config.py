"""模拟 API 配置(环境变量前缀 MOCK_API_)。"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MOCK_API_", env_file=".env", extra="ignore")

    token_ttl_seconds: int = 7200  # access token 有效期(2h)
    rate_limit_orders: int = 60  # /orders 每分钟配额
    rate_limit_burst: int = 120
    orders_per_day: int = 500
    seed_base: str = "mock"  # 确定性数据种子前缀
    client_id: str = "test_client"
    client_secret: str = "test_secret"


@lru_cache
def get_settings() -> Settings:
    return Settings()
