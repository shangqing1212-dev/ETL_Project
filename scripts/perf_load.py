"""装载性能验证: 流式生成 N 行订单+明细,经 MySQLBatchLoader 批量 upsert 进 compose MySQL。

M3 验收基线: 100 万订单 + 100 万明细 < 10 分钟(内存 < 2GB,生成器流式供给)。
数据落在 shop_id=2 / platform=perf,与 dev 演示数据(shop 1)隔离;
同 seed 重跑即幂等更新路径,可用于单独测量 update 吞吐。

用法(项目根目录,需先起 compose mysql 并执行 migrate):
    uv run python scripts/perf_load.py              # 100 万订单 + 100 万明细
    uv run python scripts/perf_load.py --rows 100000
"""

from __future__ import annotations

import argparse
import random
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from etl_sdk.config import get_settings
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.logging_conf import setup_logging

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


def gen_orders(n: int, batch_id: str) -> Iterable[dict[str, Any]]:
    """流式生成订单行(确定性: 同 seed 重跑得到相同行,upsert 变成纯更新路径)。"""
    rng = random.Random(42)
    base = datetime(2026, 9, 1, 0, 0, 0)
    for i in range(n):
        updated = base + timedelta(seconds=rng.randrange(0, 30 * 86_400))
        amount = rng.randrange(100, 200_001)
        yield {
            "shop_id": 2,
            "order_id": f"PERF{i:08d}",
            "platform": "perf",
            "order_status": "paid",
            "buyer_nick": f"perf_user_{i}",
            "order_amount_cents": amount,
            "payment_amount_cents": amount,
            "refund_amount_cents": 0,
            "raw_json": "{}",
            "is_deleted": 0,
            "created_at": updated - timedelta(hours=1),
            "updated_at": updated,
            "etl_batch_id": batch_id,
        }


def gen_items(n: int, batch_id: str) -> Iterable[dict[str, Any]]:
    """流式生成明细行(每订单 1 条,item_id 由订单号推导,与订单生成解耦)。"""
    rng = random.Random(43)
    base = datetime(2026, 9, 1, 0, 0, 0)
    for i in range(n):
        yield {
            "shop_id": 2,
            "order_id": f"PERF{i:08d}",
            "item_id": f"PERF{i:08d}01",
            "platform": "perf",
            "product_id": f"P{rng.randrange(1, 200):05d}",
            "product_name": f"perf商品{i}",
            "quantity": rng.randrange(1, 5),
            "price_cents": rng.randrange(100, 200_001),
            "raw_json": "{}",
            "updated_at": base + timedelta(seconds=rng.randrange(0, 30 * 86_400)),
            "etl_batch_id": batch_id,
        }


def _timed(label: str, fn: Callable[[], object]) -> float:
    started = time.monotonic()
    fn()
    elapsed = time.monotonic() - started
    print(f"[perf_load] {label}: {elapsed:.1f}s")
    return elapsed


def _count(engine: sa.Engine, table: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(sa.text(f"SELECT COUNT(*) FROM {table} WHERE shop_id = 2")).scalar_one())


def main() -> int:
    parser = argparse.ArgumentParser(description="装载性能验证")
    parser.add_argument("--rows", type=int, default=1_000_000, help="订单行数(明细同数)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_format == "json")
    engine = sa.create_engine(settings.db.meta_url)
    orders_loader = MySQLBatchLoader(engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK)
    items_loader = MySQLBatchLoader(engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK)

    batch_id = uuid.uuid4().hex
    rows = args.rows
    print(f"[perf_load] 目标: {rows:,} 订单 + {rows:,} 明细(shop_id=2, batch={batch_id})")
    total_started = time.monotonic()

    _timed("订单 upsert", lambda: orders_loader.upsert(gen_orders(rows, batch_id)))
    _timed("明细 upsert", lambda: items_loader.upsert(gen_items(rows, batch_id)))

    elapsed = time.monotonic() - total_started
    print(f"[perf_load] 总耗时 {elapsed:.1f}s({rows * 2 / elapsed:,.0f} 行/秒)")
    print(f"[perf_load] 落库核对: orders={_count(engine, ORDERS_TABLE):,} items={_count(engine, ITEMS_TABLE):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
