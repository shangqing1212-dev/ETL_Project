"""维表构建纯函数: 店铺维 SCD2 diff、商品维聚合(polars,无 IO)。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

_ATTR_COLUMNS = ("shop_name", "status")


@dataclass(frozen=True)
class ShopDiff:
    """dim_shop SCD2 一次 diff 的结果: 需要插入的新版本 + 需要关闭的旧版本。"""

    new_rows: list[dict[str, Any]]  # 待 INSERT 的新版本(valid_from/valid_to/is_current 已就绪)
    close_keys: list[int]  # 待关闭(is_current=0, valid_to=effective)的 shop_key


def diff_dim_shop(
    registry: Sequence[Mapping[str, Any]],
    current: Sequence[Mapping[str, Any]],
    *,
    effective: date,
) -> ShopDiff:
    """店铺注册表 vs dim_shop 当前态,产出 SCD2 变更集。

    registry: shops.yaml 条目(shop_id, platform, shop_name, status);
    current: dim_shop 现有行(shop_key, shop_id, platform, shop_name, status, is_current)。
    规则:
    - 注册表有、当前无,或属性变化 → 开新版本(valid_from=effective, is_current=1),关闭旧版本
    - 属性一致 → 不动(重跑幂等)
    - 当前 is_current=1 但注册表已移除 → 关闭该版本(valid_to=effective)
    """
    current_rows = {r["shop_key"]: r for r in current if r.get("is_current") == 1}
    active_by_shop: dict[tuple[int, str], int] = {}
    for key, row in current_rows.items():
        active_by_shop.setdefault((row["shop_id"], row["platform"]), key)

    new_rows: list[dict[str, Any]] = []
    close_keys: list[int] = []
    registered: set[tuple[int, str]] = set()
    for reg in registry:
        shop_id = int(reg["shop_id"])
        platform = str(reg["platform"])
        registered.add((shop_id, platform))
        attrs = {c: reg[c] for c in _ATTR_COLUMNS}

        cur_key = active_by_shop.get((shop_id, platform))
        cur = current_rows.get(cur_key) if cur_key is not None else None
        if cur is not None and all(cur.get(c) == attrs[c] for c in _ATTR_COLUMNS):
            continue  # 无变化
        new_rows.append(
            {
                "shop_id": shop_id,
                "platform": platform,
                **attrs,
                "valid_from": effective,
                "valid_to": None,
                "is_current": 1,
            }
        )
        if cur_key is not None:
            close_keys.append(cur_key)

    # 注册表已移除的店铺: 关闭其当前版本
    for key, row in current_rows.items():
        if (row["shop_id"], row["platform"]) not in registered and key not in close_keys:
            close_keys.append(key)

    return ShopDiff(new_rows=new_rows, close_keys=close_keys)


def aggregate_products(ods_items: pl.DataFrame, *, now: datetime) -> pl.DataFrame:
    """ODS 明细 → dim_product 最新态(纯 polars)。

    输入列: shop_id, product_id, product_name, updated_at(naive datetime)。
    输出列: shop_id, product_id, product_name(按 updated_at 最新),
    first_seen_at(最小 updated_at), last_seen_at(最大), is_active(近 90 天有出现)。
    """
    recent = now - timedelta(days=90)
    grouped = ods_items.group_by(["shop_id", "product_id"]).agg(
        first_seen_at=pl.col("updated_at").min(),
        last_seen_at=pl.col("updated_at").max(),
    )
    latest_name = (
        ods_items.sort("updated_at", descending=True)
        .group_by(["shop_id", "product_id"], maintain_order=True)
        .agg(pl.col("product_name").first())
    )
    return grouped.join(latest_name, on=["shop_id", "product_id"], how="left").with_columns(
        is_active=(pl.col("last_seen_at") >= recent).cast(pl.Int8)
    )
