"""运行时装配工厂: 把配置/连接组装成可执行的抽取器与适配器。

不依赖 Airflow —— dev 脚本(scripts/dev_extract.py)与 Airflow Operator 共用同一套装配,
保证"Airflow 里跑的"与"本地联调的"是同一份代码。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from etl_sdk.adapters.base import BasePlatformAdapter
from etl_sdk.adapters.mock import MockPlatformAdapter
from etl_sdk.alerts import build_alert_manager
from etl_sdk.alerts.base import AlertManager
from etl_sdk.config import Settings
from etl_sdk.dq.contracts import order_items_ods_contract, orders_ods_contract
from etl_sdk.dq.engine import DQEngine
from etl_sdk.extractors.base import BaseExtractor, EntitySpec, TimeWindow
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import PagePaginator, Paginator
from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket
from etl_sdk.extractors.state import WatermarkState
from etl_sdk.loaders.dead_letter import DeadLetterRecorder
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.mappers.orders import order_items_to_ods, order_to_ods

ORDERS_TABLE = "dw.ods_orders"
ITEMS_TABLE = "dw.ods_order_items"
ORDER_COLUMNS = [
    "shop_id",
    "order_id",
    "platform",
    "order_status",
    "buyer_nick",
    "order_amount_cents",
    "payment_amount_cents",
    "refund_amount_cents",
    "raw_json",
    "is_deleted",
    "created_at",
    "updated_at",
    "etl_batch_id",
]
ORDER_PK = ["shop_id", "order_id"]
ITEM_COLUMNS = [
    "shop_id",
    "order_id",
    "item_id",
    "platform",
    "product_id",
    "product_name",
    "quantity",
    "price_cents",
    "raw_json",
    "updated_at",
    "etl_batch_id",
]
ITEM_PK = ["shop_id", "order_id", "item_id"]


def normalize_window(window: TimeWindow) -> TimeWindow:
    """窗口时区规范化: aware → Asia/Shanghai 本地时间 → naive(ADR-001: 时间一律本地时区 naive)。

    Airflow 3 的 data_interval 是 UTC aware;源端(mock 与真实平台)时间约定为本地时区 naive。
    """
    tz = ZoneInfo("Asia/Shanghai")
    start, end = window.start, window.end
    if start.tzinfo is not None:
        start = start.astimezone(tz).replace(tzinfo=None)
    if end.tzinfo is not None:
        end = end.astimezone(tz).replace(tzinfo=None)
    return TimeWindow(start=start, end=end)


def resolve_extract_window(
    run_type: str,
    data_interval_start: datetime | None,
    data_interval_end: datetime | None,
    *,
    overlap: timedelta = timedelta(hours=1),
) -> tuple[str, TimeWindow | None]:
    """按 dag_run.run_type 解析抽取模式与窗口(纯函数,Airflow 侧与脚本侧同源)。

    - backfill: 窗口由 data_interval 显式给定,不推进水位(ADR-003);
      Airflow 3.3 的 data_interval 是"点"(start==end),窗口取 [start - overlap, start),
      对应该 run 的逻辑小时,overlap 吸收迟到更新
    - 其余(scheduled/manual): 增量模式,按水位推进,返回 (run_type, None)
    """
    if run_type == "backfill" and data_interval_start is not None:
        if data_interval_end is None or data_interval_start >= data_interval_end:
            # 点语义: 窗口 = [t - overlap, t)
            return "backfill", TimeWindow(start=data_interval_start - overlap, end=data_interval_start)
        return "backfill", TimeWindow(start=data_interval_start, end=data_interval_end)
    return run_type, None


def resolve_build_since(
    run_type: str,
    data_interval_start: datetime | None,
    now: datetime,
    *,
    default_days: int = 3,
) -> date:
    """数仓构建窗口起点: 回填按 data_interval,否则重建最近 default_days 天。"""
    if run_type == "backfill" and data_interval_start is not None:
        return data_interval_start.date()
    return now.date() - timedelta(days=default_days - 1)


def build_alert_manager_from(settings: Settings) -> AlertManager:
    """告警管理器(配置即启用,全空时兜底日志通道)。"""
    return build_alert_manager(settings.alert)


def build_mock_adapter(settings: Settings, *, base_url: str | None = None) -> MockPlatformAdapter:
    """模拟平台适配器(真实平台接入后按注册表路由到对应实现)。"""
    return MockPlatformAdapter(
        base_url=base_url or settings.platform.base_url,
        client_id=settings.platform.client_id,
        client_secret=settings.platform.client_secret,
        timeout_seconds=settings.platform.timeout_seconds,
        rate_limiter=AdaptiveRateLimiter(TokenBucket(rate_per_min=settings.platform.rate_limit_orders)),
    )


def build_extractor(
    engine: sa.Engine,
    adapter: BasePlatformAdapter,
    settings: Settings,
    *,
    shop_id: int,
    platform: str,
    paginator: Paginator | None = None,
    alert_manager: AlertManager | None = None,
) -> BaseExtractor:
    """组装订单抽取器(主表 ods_orders + 子表 ods_order_items,含死信/DQ/告警)。"""
    orders_spec = EntitySpec(
        table_name=ORDERS_TABLE,
        columns=ORDER_COLUMNS,
        pk_columns=ORDER_PK,
        mapper=lambda r, **kw: order_to_ods(r, **kw),
        contract=orders_ods_contract(),
    )
    items_spec = EntitySpec(
        table_name=ITEMS_TABLE,
        columns=ITEM_COLUMNS,
        pk_columns=ITEM_PK,
        rows_mapper=lambda r, **kw: order_items_to_ods(r, **kw),
        contract=order_items_ods_contract(),
    )
    extractor = BaseExtractor(
        adapter=adapter,
        paginator=paginator or PagePaginator(page_size=settings.platform.page_size),
        entity=orders_spec,
        loader=MySQLBatchLoader(engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK),
        watermark=WatermarkState(engine),
        recorder=BatchRecorder(engine),
        shop_id=shop_id,
        platform=platform,
        overlap=timedelta(minutes=settings.extraction.overlap_minutes),
        delay=timedelta(minutes=settings.extraction.delay_minutes),
        dead_letter=DeadLetterRecorder(engine),
        dead_letter_limit=settings.extraction.dead_letter_limit,
        dq_engine=DQEngine(engine) if settings.extraction.dq_enabled else None,
        alert_manager=alert_manager,
    )
    extractor.add_child(
        items_spec,
        MySQLBatchLoader(engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK),
    )
    return extractor
