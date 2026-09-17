"""全局配置: pydantic-settings 模型,环境变量前缀 ETL_。

配置层级(多店铺预留): 平台默认 < 店铺覆盖 < 环境变量。
店铺级覆盖在 M2 与店铺注册表(registration)一起落地。
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """数仓与元数据库连接配置。"""

    model_config = SettingsConfigDict(env_prefix="ETL_DB_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 3306
    user: str = "etl"
    password: str = "etl_pass"
    meta_schema: str = "etl_meta"
    warehouse_schema: str = "dw"
    charset: str = "utf8mb4"

    @property
    def url(self) -> str:
        """无默认库的服务器级连接(供 DDL 迁移等)。"""
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/?charset={self.charset}"

    @property
    def meta_url(self) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}"
            f"/{self.meta_schema}?charset={self.charset}"
        )

    @property
    def warehouse_url(self) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}"
            f"/{self.warehouse_schema}?charset={self.charset}"
        )


class PlatformSettings(BaseSettings):
    """单个平台的默认配置(店铺级覆盖在 M2 加入)。"""

    model_config = SettingsConfigDict(env_prefix="ETL_PLATFORM_", env_file=".env", extra="ignore")

    name: str = "mock"
    base_url: str = "http://localhost:8080"
    client_id: str = "test_client"
    client_secret: str = "test_secret"
    timeout_seconds: float = 30.0
    page_size: int = 100
    # 端点级限流配额(每分钟请求数)
    rate_limit_orders: int = 60
    rate_limit_details: int = 30


class ExtractionSettings(BaseSettings):
    """抽取窗口与去重参数。"""

    model_config = SettingsConfigDict(env_prefix="ETL_EXTRACTION_", env_file=".env", extra="ignore")

    overlap_minutes: int = 60  # 窗口左移重叠(覆盖迟到更新)
    delay_minutes: int = 5  # 窗口右界滞后(避开源端写入抖动)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ETL_", env_file=".env", extra="ignore")

    env: str = "dev"  # dev/test/prod
    log_level: str = "INFO"
    log_format: str = "console"  # console/json
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    platform: PlatformSettings = Field(default_factory=PlatformSettings)
    extraction: ExtractionSettings = Field(default_factory=ExtractionSettings)


@lru_cache
def get_settings() -> Settings:
    return Settings()
