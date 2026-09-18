"""DWD → DWS 日粒度聚合(纯 polars,无 IO)。

GMV 口径(数据字典): status_norm ∈ {paid, shipped, completed, refunded} 且未软删的订单,
gmv = Σ实付额,order_cnt = 订单数,buyer_cnt = 去重买家,refund = Σ退款额,客单价 = gmv / order_cnt。
"""

from __future__ import annotations

import polars as pl

PAID_STATUSES = ("paid", "shipped", "completed", "refunded")

DWS_SHOP_DAILY_COLUMNS = [
    "stat_date",
    "shop_id",
    "platform",
    "gmv_cents",
    "order_cnt",
    "buyer_cnt",
    "refund_cents",
    "avg_order_cents",
]
DWS_PRODUCT_DAILY_COLUMNS = ["stat_date", "shop_id", "product_id", "platform", "sold_qty", "sales_cents"]


def _paid_orders(dwd_orders: pl.DataFrame) -> pl.DataFrame:
    """过滤 GMV 口径的有效订单,并附业务日 stat_date(下单日,created_at 缺失回退 updated_at)。"""
    stat_date = pl.coalesce(pl.col("created_at"), pl.col("updated_at")).dt.date()
    return (
        dwd_orders.filter(pl.col("status_norm").is_in(PAID_STATUSES) & (pl.col("is_deleted").fill_null(0) == 0))
        .with_columns(stat_date.alias("stat_date"))
        .select(
            [
                "shop_id",
                "order_id",
                "platform",
                "stat_date",
                "buyer_nick",
                "payment_amount_cents",
                "refund_amount_cents",
            ]
        )
    )


def aggregate_shop_daily(dwd_orders: pl.DataFrame) -> pl.DataFrame:
    """dwd_orders → dws_shop_daily 行(GMV 口径,按 stat_date+shop_id 汇总)。"""
    paid = _paid_orders(dwd_orders)
    grouped = paid.group_by(["stat_date", "shop_id"]).agg(
        gmv_cents=pl.col("payment_amount_cents").fill_null(0).sum().cast(pl.Int64),
        order_cnt=pl.len().cast(pl.Int32),
        buyer_cnt=pl.col("buyer_nick").n_unique().cast(pl.Int32),
        refund_cents=pl.col("refund_amount_cents").fill_null(0).sum().cast(pl.Int64),
        platform=pl.col("platform").first(),
    )
    return grouped.with_columns(avg_order_cents=(pl.col("gmv_cents") / pl.col("order_cnt")).cast(pl.Int64)).select(
        DWS_SHOP_DAILY_COLUMNS
    )


def aggregate_product_daily(dwd_orders: pl.DataFrame, dwd_items: pl.DataFrame) -> pl.DataFrame:
    """dwd_orders + dwd_order_items → dws_product_daily 行(GMV 口径订单的明细汇总)。

    销量与销售额只统计有效订单里的明细;金额列缺失按 0 计(与 ODS 契约一致,不应出现,兜底而已)。
    """
    paid = _paid_orders(dwd_orders)
    items = paid.join(
        dwd_items.select(["shop_id", "order_id", "product_id", "quantity", "amount_cents"]),
        on=["shop_id", "order_id"],
        how="inner",
    )
    grouped = items.group_by(["stat_date", "shop_id", "product_id"]).agg(
        sold_qty=pl.col("quantity").fill_null(0).sum().cast(pl.Int32),
        sales_cents=pl.col("amount_cents").fill_null(0).sum().cast(pl.Int64),
        platform=pl.col("platform").first(),
    )
    return grouped.select(DWS_PRODUCT_DAILY_COLUMNS)
