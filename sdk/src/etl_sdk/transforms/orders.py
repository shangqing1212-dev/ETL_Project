"""ODS → DWD 清洗转换(纯 polars,无 IO): 状态统一枚举 + 明细行金额派生。

状态归一规则(数据字典「字段口径约定」):
- 平台原始码先查 PLATFORM_STATUS_MAP 映射为内部码(mock 与内部码同构,真实平台在此登记差异映射);
- 未登记的码一律归 unknown(不抛错 —— 转换层保证批不中断,未知状态由 DQ/BI 侧暴露);
- 派生 refunded: 原始码 shipped/completed 且退款>0 且实付>0(发货后全额退款)。
"""

from __future__ import annotations

import polars as pl

VALID_STATUS = frozenset({"pending", "paid", "shipped", "completed", "cancelled", "refunded", "unknown"})

# 真实平台接入时登记差异映射,如 {"taobao": {"TRADE_FINISHED": "completed", "TRADE_CLOSED": "cancelled"}}
PLATFORM_STATUS_MAP: dict[str, dict[str, str]] = {}

DWD_ORDER_COLUMNS = [
    "shop_id",
    "order_id",
    "platform",
    "status_raw",
    "status_norm",
    "buyer_nick",
    "order_amount_cents",
    "payment_amount_cents",
    "refund_amount_cents",
    "is_deleted",
    "created_at",
    "updated_at",
    "etl_batch_id",
]
DWD_ITEM_COLUMNS = [
    "shop_id",
    "order_id",
    "item_id",
    "platform",
    "product_id",
    "product_name",
    "quantity",
    "price_cents",
    "amount_cents",
    "updated_at",
    "etl_batch_id",
]


def normalize_status(
    platform: str, status_raw: str | None, *, refund_cents: int | None, payment_cents: int | None
) -> str:
    """单行状态归一(供测试与逐行调用;批处理用 _status_norm 表达式)。"""
    mapped = PLATFORM_STATUS_MAP.get(platform, {}).get(status_raw or "", status_raw or "")
    if mapped not in VALID_STATUS:
        return "unknown"
    if mapped in ("shipped", "completed") and (refund_cents or 0) > 0 and (payment_cents or 0) > 0:
        return "refunded"
    return mapped


def _status_norm_expr(platform: str) -> pl.Expr:
    """向量化状态归一表达式(批量转换,避免逐行 map;输入列为 ODS 的 order_status)。"""
    status_map = PLATFORM_STATUS_MAP.get(platform, {})
    status_raw = pl.col("order_status")
    # 未登记的码原样透传,由 VALID_STATUS 校验兜底归 unknown(空映射时 replace_strict 会报错,直接跳过)
    status_mapped = status_raw.replace_strict(status_map).fill_null(status_raw) if status_map else status_raw
    unknown = ~status_mapped.is_in(list(VALID_STATUS))
    refunded = (
        status_mapped.is_in(["shipped", "completed"])
        & (pl.col("refund_amount_cents").fill_null(0) > 0)
        & (pl.col("payment_amount_cents").fill_null(0) > 0)
    )
    return pl.when(unknown).then(pl.lit("unknown")).when(refunded).then(pl.lit("refunded")).otherwise(status_mapped)


def ods_to_dwd_orders(df: pl.DataFrame, *, platform: str, batch_id: str) -> pl.DataFrame:
    """ods_orders → dwd_orders 行(纯函数;stat_date 是 DWD 生成列,由数据库计算)。"""
    return df.with_columns(status_norm=_status_norm_expr(platform)).select(
        pl.col("shop_id"),
        pl.col("order_id"),
        pl.lit(platform).alias("platform"),
        pl.col("order_status").alias("status_raw"),
        pl.col("status_norm"),
        pl.col("buyer_nick"),
        pl.col("order_amount_cents"),
        pl.col("payment_amount_cents"),
        pl.col("refund_amount_cents"),
        pl.col("is_deleted"),
        pl.col("created_at"),
        pl.col("updated_at"),
        pl.lit(batch_id).alias("etl_batch_id"),
    )


def ods_to_dwd_items(df: pl.DataFrame, *, platform: str, batch_id: str) -> pl.DataFrame:
    """ods_order_items → dwd_order_items 行;行金额 = 单价 × 数量(任一缺失则 NULL)。"""
    amount = pl.col("quantity") * pl.col("price_cents")  # 含 NULL 时结果为 NULL,符合口径
    return df.select(
        pl.col("shop_id"),
        pl.col("order_id"),
        pl.col("item_id"),
        pl.lit(platform).alias("platform"),
        pl.col("product_id"),
        pl.col("product_name"),
        pl.col("quantity"),
        pl.col("price_cents"),
        amount.alias("amount_cents"),
        pl.col("updated_at"),
        pl.lit(batch_id).alias("etl_batch_id"),
    )
