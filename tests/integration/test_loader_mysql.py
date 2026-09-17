"""集成测试: MySQLBatchLoader 幂等 upsert 语义(真实 MySQL,testcontainers)。

M1 验收核心: "SDK 能写 1 批数据进 MySQL",且证明:
- 重复执行同一批数据,行数/校验和稳定(幂等)
- 迟到旧数据不会覆盖新数据(updated_at 只升不降)
"""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlalchemy as sa
from etl_sdk.loaders.mysql import MySQLBatchLoader
from testcontainers.community.mysql import MySqlContainer

from scripts.migrate import migrate

TABLE = "etl_meta.t_demo_orders"
COLUMNS = ["shop_id", "order_id", "status", "amount_cents", "updated_at"]
PK_COLUMNS = ["shop_id", "order_id"]


def _demo_row(order_id: str, status: str, amount: int, updated_at: datetime) -> dict:
    return {
        "shop_id": 1,
        "order_id": order_id,
        "status": status,
        "amount_cents": amount,
        "updated_at": updated_at,
    }


@pytest.fixture(scope="module")
def engine():
    with MySqlContainer("mysql:8.4", username="etl", password="etl_pass", root_password="root_pass") as container:
        # testcontainers 返回 mysql://(默认 mysqldb 驱动),统一转 pymysql(项目唯一 MySQL 驱动)
        url = container.get_connection_url().replace("mysql://", "mysql+pymysql://", 1)
        root_url = url.replace("etl:etl_pass@", "root:root_pass@", 1)
        # 建库迁移与授权需 root(容器默认 etl 用户仅有 test 库权限)
        migrate(root_url)
        with sa.create_engine(root_url).begin() as conn:
            conn.execute(sa.text("GRANT ALL PRIVILEGES ON etl_meta.* TO 'etl'@'%'"))
        eng = sa.create_engine(url)
        with eng.begin() as conn:
            conn.execute(
                sa.text(
                    f"CREATE TABLE {TABLE} ("
                    "shop_id BIGINT NOT NULL,"
                    "order_id VARCHAR(64) NOT NULL,"
                    "status VARCHAR(32) NOT NULL,"
                    "amount_cents BIGINT NOT NULL,"
                    "updated_at DATETIME(3) NOT NULL,"
                    "PRIMARY KEY (shop_id, order_id)"
                    ") ENGINE=InnoDB"
                )
            )
        yield eng
        eng.dispose()


def _count(engine: sa.Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar_one()


def _get(engine: sa.Engine, order_id: str) -> tuple[str, datetime]:
    with engine.connect() as conn:
        return conn.execute(
            sa.text(f"SELECT status, updated_at FROM {TABLE} WHERE order_id = :o"),
            {"o": order_id},
        ).one()


def test_upsert_insert_then_idempotent_rerun(engine: sa.Engine) -> None:
    loader = MySQLBatchLoader(engine, TABLE, COLUMNS, PK_COLUMNS)
    t0 = datetime(2026, 9, 1, 10, 0, 0, 0)

    batch = [_demo_row("A", "paid", 100, t0), _demo_row("B", "shipped", 200, t0)]
    loader.upsert(batch)
    assert _count(engine) == 2

    # 同一批重跑: 行数不变、内容稳定(幂等)
    loader.upsert(batch)
    assert _count(engine) == 2
    assert _get(engine, "A") == ("paid", t0)
    assert _get(engine, "B") == ("shipped", t0)


def test_upsert_update_and_monotonic_guard(engine: sa.Engine) -> None:
    loader = MySQLBatchLoader(engine, TABLE, COLUMNS, PK_COLUMNS)
    t1 = datetime(2026, 9, 1, 11, 0, 0, 0)
    t_old = datetime(2026, 9, 1, 9, 0, 0, 0)

    # 更新已有行 + 新增一行
    loader.upsert([_demo_row("A", "completed", 150, t1), _demo_row("C", "pending", 300, t1)])
    assert _count(engine) == 3
    assert _get(engine, "A") == ("completed", t1)

    # 迟到旧数据(updated_at 更早)不得覆盖新状态
    loader.upsert([_demo_row("A", "paid", 100, t_old)])
    assert _get(engine, "A") == ("completed", t1)


def test_chunked_write_and_rowcount(engine: sa.Engine) -> None:
    loader = MySQLBatchLoader(engine, TABLE, COLUMNS, PK_COLUMNS, chunk_size=5)
    t0 = datetime(2026, 9, 2, 8, 0, 0, 0)
    rows = [_demo_row(f"CHUNK{i:03d}", "paid", i, t0) for i in range(23)]
    affected = loader.upsert(rows)
    assert affected == 23, "首次全为 insert,受影响行数应等于行数"
    assert _count(engine) == 3 + 23
