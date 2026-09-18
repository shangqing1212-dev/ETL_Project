"""集成测试共享 fixture: 会话级 testcontainers MySQL(已迁移全部 DDL + 授权)。

注意: 本机(中文用户名)跑集成测试需要 PYTHONUTF8=1,见 README。
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from testcontainers.community.mysql import MySqlContainer

from scripts.migrate import migrate

_CLEAN_TABLES = (
    "dw.ods_order_items",
    "dw.ods_orders",
    "dw.dwd_order_items",
    "dw.dwd_orders",
    "dw.dim_shop",
    "dw.dim_date",
    "dw.dim_product",
    "dw.dws_shop_daily",
    "dw.dws_product_daily",
    "dw.ads_shop_overview",
    "dw.ads_daily_kpi",
    "etl_meta.etl_watermark",
    "etl_meta.etl_batch",
    "etl_meta.etl_task_run",
    "etl_meta.etl_load_error",
    "etl_meta.dq_check_result",
)


@pytest.fixture(scope="session")
def mysql_engine():
    with MySqlContainer("mysql:8.4", username="etl", password="etl_pass", root_password="root_pass") as container:
        # testcontainers 返回 mysql://(默认 mysqldb 驱动),统一转 pymysql(项目唯一 MySQL 驱动)
        url = container.get_connection_url().replace("mysql://", "mysql+pymysql://", 1)
        root_url = url.replace("etl:etl_pass@", "root:root_pass@", 1)
        # 建库迁移与授权需 root(容器默认 etl 用户仅有 test 库权限)
        migrate(root_url)
        with sa.create_engine(root_url).begin() as conn:
            for schema in ("etl_meta", "dw"):
                conn.execute(sa.text(f"GRANT ALL PRIVILEGES ON {schema}.* TO 'etl'@'%'"))
        eng = sa.create_engine(url)
        yield eng
        eng.dispose()


@pytest.fixture()
def clean_state(mysql_engine: sa.Engine):
    """每个测试前清空业务表,保证用例互不干扰。"""
    with mysql_engine.begin() as conn:
        for table in _CLEAN_TABLES:
            conn.execute(sa.text(f"DELETE FROM {table}"))
    yield mysql_engine


@pytest.fixture()
def extractor_factory(mysql_engine: sa.Engine):
    """构造抽取器工厂(死信/DQ 测试用): 各容错组件按参数可选注入。"""
    from datetime import datetime

    from etl_sdk.alerts.base import AlertManager
    from etl_sdk.dq.contracts import order_items_ods_contract, orders_ods_contract
    from etl_sdk.dq.engine import DQEngine
    from etl_sdk.extractors.base import BaseExtractor, EntitySpec
    from etl_sdk.extractors.batches import BatchRecorder
    from etl_sdk.extractors.pagination import PagePaginator
    from etl_sdk.extractors.state import WatermarkState
    from etl_sdk.loaders.dead_letter import DeadLetterRecorder
    from etl_sdk.loaders.mysql import MySQLBatchLoader
    from etl_sdk.mappers.orders import order_items_to_ods, order_to_ods

    from tests.integration.helpers import (
        ITEM_COLUMNS,
        ITEM_PK,
        ITEMS_TABLE,
        ORDER_COLUMNS,
        ORDER_PK,
        ORDERS_TABLE,
        CannedAdapter,
    )

    def factory(
        *,
        rows: list[dict],
        now: datetime | None = None,
        dead_letter: bool = True,
        dead_letter_limit: float = 0.0,
        dq: bool = False,
        contract: bool = False,
        alert_manager: AlertManager | None = None,
    ) -> BaseExtractor:
        orders_spec = EntitySpec(
            table_name=ORDERS_TABLE,
            columns=ORDER_COLUMNS,
            pk_columns=ORDER_PK,
            mapper=lambda r, **kw: order_to_ods(r, **kw),
            contract=orders_ods_contract() if contract else None,
        )
        items_spec = EntitySpec(
            table_name=ITEMS_TABLE,
            columns=ITEM_COLUMNS,
            pk_columns=ITEM_PK,
            rows_mapper=lambda r, **kw: order_items_to_ods(r, **kw),
            contract=order_items_ods_contract() if contract else None,
        )
        extractor = BaseExtractor(
            adapter=CannedAdapter(rows),
            paginator=PagePaginator(page_size=100),
            entity=orders_spec,
            loader=MySQLBatchLoader(mysql_engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK),
            watermark=WatermarkState(mysql_engine),
            recorder=BatchRecorder(mysql_engine),
            shop_id=1,
            platform="canned",
            history_start=datetime(2026, 8, 25),
            now_fn=(lambda: now) if now is not None else datetime.now,
            dead_letter=DeadLetterRecorder(mysql_engine) if dead_letter else None,
            dead_letter_limit=dead_letter_limit,
            dq_engine=DQEngine(mysql_engine) if dq else None,
            alert_manager=alert_manager,
        )
        extractor.add_child(
            items_spec,
            MySQLBatchLoader(mysql_engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK),
        )
        return extractor

    return factory
