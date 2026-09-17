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
