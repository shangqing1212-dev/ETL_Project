"""E2E 共享 fixture: testcontainers MySQL + 进程内 mock API(ASGITransport,故障注入共享进程状态)。

E2E 与 integration 的区别: 数据源是真实 mock API 实现(含鉴权/分页/故障注入中间件),
SDK 走真实适配器与重试路径;integration 用 CannedAdapter 注入坏行测装载容错。
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
        url = container.get_connection_url().replace("mysql://", "mysql+pymysql://", 1)
        root_url = url.replace("etl:etl_pass@", "root:root_pass@", 1)
        migrate(root_url)
        with sa.create_engine(root_url).begin() as conn:
            for schema in ("etl_meta", "dw"):
                conn.execute(sa.text(f"GRANT ALL PRIVILEGES ON {schema}.* TO 'etl'@'%'"))
        eng = sa.create_engine(url)
        yield eng
        eng.dispose()


@pytest.fixture()
def clean_state(mysql_engine: sa.Engine):
    """每个测试前清空业务表。"""
    with mysql_engine.begin() as conn:
        for table in _CLEAN_TABLES:
            conn.execute(sa.text(f"DELETE FROM {table}"))
    yield mysql_engine


@pytest.fixture(scope="session")
def mock_app():
    """进程内 mock API 应用(故障注入 set_fault 与中间件共享模块级状态)。"""
    from mock_api.main import app

    return app


@pytest.fixture(scope="session")
def mock_base_url(mock_app):
    """真实 uvicorn 线程服务器(随机端口): httpx 0.28 的 ASGITransport 仅支持 async,
    同步 SDK 走真 HTTP 链路(timeout 注入/JSON 解析均为真实网络行为)。"""
    import threading
    import time

    import uvicorn

    config = uvicorn.Config(mock_app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started, "uvicorn 未能启动"
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)
