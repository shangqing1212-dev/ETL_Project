"""开发联调: 对 compose 环境执行一轮订单抽取(增量或回填)。

用法(项目根目录):
    uv run python scripts/dev_extract.py                 # 增量(按水位)
    uv run python scripts/dev_extract.py backfill 3      # 回填最近 3 天(不推进水位)

依赖: docker compose -f airflow/docker-compose.dev.yaml up -d(mysql + mock-api),
      uv run python scripts/migrate.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

import sqlalchemy as sa
from etl_sdk.adapters.mock import MockPlatformAdapter
from etl_sdk.alerts import build_alert_manager
from etl_sdk.config import get_settings
from etl_sdk.dq.contracts import order_items_ods_contract, orders_ods_contract
from etl_sdk.dq.engine import DQEngine
from etl_sdk.extractors.base import BaseExtractor, EntitySpec, TimeWindow
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import PagePaginator
from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket
from etl_sdk.extractors.state import WatermarkState
from etl_sdk.loaders.dead_letter import DeadLetterRecorder
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.logging_conf import setup_logging
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


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_format == "json")

    engine = sa.create_engine(settings.db.meta_url)
    adapter = MockPlatformAdapter(
        base_url=settings.platform.base_url,
        client_id=settings.platform.client_id,
        client_secret=settings.platform.client_secret,
        timeout_seconds=settings.platform.timeout_seconds,
        rate_limiter=AdaptiveRateLimiter(TokenBucket(rate_per_min=settings.platform.rate_limit_orders)),
    )
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
        paginator=PagePaginator(page_size=settings.platform.page_size),
        entity=orders_spec,
        loader=MySQLBatchLoader(engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK),
        watermark=WatermarkState(engine),
        recorder=BatchRecorder(engine),
        shop_id=1,
        platform=settings.platform.name,
        overlap=timedelta(minutes=settings.extraction.overlap_minutes),
        delay=timedelta(minutes=settings.extraction.delay_minutes),
        dead_letter=DeadLetterRecorder(engine),
        dead_letter_limit=settings.extraction.dead_letter_limit,
        dq_engine=DQEngine(engine) if settings.extraction.dq_enabled else None,
        alert_manager=build_alert_manager(settings.alert),
    )
    extractor.add_child(
        items_spec,
        MySQLBatchLoader(engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK),
    )

    run_type = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    window_override = None
    if run_type == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        end = datetime.now().replace(microsecond=0)
        window_override = TimeWindow(start=end - timedelta(days=days), end=end)
        print(f"[dev_extract] 回填窗口: {window_override.start} ~ {window_override.end}(不推进水位)")

    result = extractor.extract(run_type=run_type, window_override=window_override)
    print(
        f"[dev_extract] done: {result.table_name} run={result.run_type} "
        f"rows_read={result.rows_read} rows_written={result.rows_written} "
        f"dead_letters={result.dead_letters} "
        f"watermark_advanced={result.watermark_advanced} batch={result.batch_id}"
    )


if __name__ == "__main__":
    main()
