"""确定性数据生成: seed = f"{seed_base}:{date}:{entity}",同一天生成结果完全一致。

可复现性用途: 集成测试可断言精确值;SDK 幂等测试可对比两遍抽取结果。
M2 将加入"近 3 天迟到更新"与故障注入开关。
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from typing import Any

from faker import Faker

from mock_api.config import get_settings

ORDER_STATUSES = ["pending", "paid", "shipped", "completed", "cancelled"]


def _rng(d: date, entity: str, seed_base: str) -> random.Random:
    return random.Random(f"{seed_base}:{d.isoformat()}:{entity}")


def _money(rng: random.Random) -> str:
    """随机金额: 1.00 ~ 2000.00 元,两位小数字符串(模拟平台返回字符串金额)。"""
    cents = rng.randrange(100, 200_001)
    return str((Decimal(cents) / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


@lru_cache(maxsize=64)
def gen_orders_for_day(
    d: date,
    *,
    shop_id: int = 1,
    orders_per_day: int,
    seed_base: str,
) -> tuple[dict[str, Any], ...]:
    """生成某天全部订单(确定性,按 updated_at 升序)。"""
    rng = _rng(d, "orders", seed_base)
    faker = Faker("zh_CN")
    faker.seed_instance(rng.randrange(2**32))

    orders: list[dict[str, Any]] = []
    for seq in range(1, orders_per_day + 1):
        created = datetime.combine(d, time.min) + timedelta(seconds=rng.randrange(0, 86_400))
        # updated 允许跨到次日(锻炼 SDK 重叠窗口的迟到更新场景)
        updated = created + timedelta(minutes=rng.randrange(0, 600))
        amount = _money(rng)
        item_price = _money(rng)
        orders.append(
            {
                "order_id": f"MOCK{d.strftime('%Y%m%d')}{seq:06d}",
                "shop_id": shop_id,
                "status": rng.choice(ORDER_STATUSES),
                "buyer_nick": faker.user_name(),
                "order_amount": amount,
                "payment_amount": amount,
                "refund_amount": "0.00" if rng.random() > 0.05 else amount,
                "created_at": created.isoformat(),
                "updated_at": updated.isoformat(),
                "items": [
                    {
                        "item_id": f"{seq}01",
                        "product_id": f"P{rng.randrange(1, 200):05d}",
                        "product_name": faker.word() + "商品",
                        "quantity": rng.randrange(1, 5),
                        "price": item_price,
                    }
                ],
            }
        )
    orders.sort(key=lambda o: o["updated_at"])
    return tuple(orders)


def gen_orders_between(
    start: datetime,
    end: datetime,
    *,
    shop_id: int = 1,
    seed_base: str,
    orders_per_day: int,
) -> list[dict[str, Any]]:
    """生成 [start, end) 窗口内 updated_at 的全部订单(按 updated_at 升序)。"""
    rows: list[dict[str, Any]] = []
    d = start.date()
    while d <= end.date():
        rows.extend(
            o
            for o in gen_orders_for_day(d, shop_id=shop_id, orders_per_day=orders_per_day, seed_base=seed_base)
            if start <= datetime.fromisoformat(o["updated_at"]) < end
        )
        d += timedelta(days=1)
    return rows


def default_orders_per_day() -> int:
    return get_settings().orders_per_day


def default_seed_base() -> str:
    return get_settings().seed_base
