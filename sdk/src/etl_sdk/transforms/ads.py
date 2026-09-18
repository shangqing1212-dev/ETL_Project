"""DWS → ADS 宽表(纯 polars,无 IO): 店铺日报(环比/退款率/动销率)与全局 KPI。"""

from __future__ import annotations

from datetime import date

import polars as pl

SHOP_OVERVIEW_COLUMNS = [
    "stat_date",
    "shop_id",
    "platform",
    "gmv_cents",
    "order_cnt",
    "buyer_cnt",
    "refund_cents",
    "avg_order_cents",
    "refund_rate",
    "gmv_dod_pct",
    "order_dod_pct",
    "active_product_cnt",
    "product_total_cnt",
    "sell_through_rate",
]
DAILY_KPI_COLUMNS = [
    "stat_date",
    "platform",
    "gmv_cents",
    "order_cnt",
    "buyer_cnt",
    "refund_cents",
    "avg_order_cents",
    "shop_cnt",
    "gmv_dod_pct",
]


def _dod_pct(df: pl.DataFrame, value_col: str) -> pl.Expr:
    """日环比 %(按分组内按日期排序后与前一天比较;无前日数据为 NULL)。"""
    prev = pl.col(value_col).shift(1)
    return ((pl.col(value_col) - prev) / prev * 100).round(4)


def build_shop_overview(
    dws_shop_daily: pl.DataFrame,
    dws_product_daily: pl.DataFrame,
    product_total_cnt: int,
    *,
    window_start: date,
) -> pl.DataFrame:
    """dws_shop_daily + dws_product_daily + 商品总数 → ads_shop_overview 行。

    dws_shop_daily 需覆盖 [窗口起点-1天, 窗口终点](前一日行仅用于窗口首日环比,本身不产出)。
    动销商品数 = 当日有销售(出现在 dws_product_daily)的商品数;动销率 = 动销/在库总数。
    """
    active = dws_product_daily.group_by(["stat_date", "shop_id"]).agg(active_product_cnt=pl.len().cast(pl.Int32))
    base = dws_shop_daily.join(active, on=["stat_date", "shop_id"], how="left").with_columns(
        pl.col("active_product_cnt").fill_null(0),
        pl.lit(product_total_cnt).cast(pl.Int32).alias("product_total_cnt"),
        refund_rate=(
            pl.when(pl.col("gmv_cents") > 0)
            .then((pl.col("refund_cents") / pl.col("gmv_cents")).round(4))
            .otherwise(pl.lit(0.0))
        ),
    )
    # 动销率引用同帧派生的 product_total_cnt,须与上一 with_columns 分两步(同块内别名不可见)
    base = base.with_columns(
        sell_through_rate=(
            pl.when(pl.col("product_total_cnt") > 0)
            .then((pl.col("active_product_cnt") / pl.col("product_total_cnt")).round(4))
            .otherwise(pl.lit(0.0))
        ),
    )
    base = base.sort(["shop_id", "stat_date"])
    return (
        base.with_columns(
            gmv_dod_pct=_dod_pct(base, "gmv_cents").over("shop_id"),
            order_dod_pct=_dod_pct(base, "order_cnt").over("shop_id"),
        )
        .filter(pl.col("stat_date") >= window_start)
        .select(SHOP_OVERVIEW_COLUMNS)
    )


def build_daily_kpi(dws_shop_daily: pl.DataFrame, *, window_start: date) -> pl.DataFrame:
    """dws_shop_daily → ads_daily_kpi 行(跨店汇总;buyer_cnt 跨店去重后重算)。

    buyer_cnt 跨店去重需要 dwd 明细,这里按店铺数加和仅为近似 —— 调用方(单店铺阶段)不受影响,
    多店铺接入时改为从 dwd_orders 直接聚合。
    """
    grouped = dws_shop_daily.group_by("stat_date").agg(
        gmv_cents=pl.col("gmv_cents").sum().cast(pl.Int64),
        order_cnt=pl.col("order_cnt").sum().cast(pl.Int32),
        buyer_cnt=pl.col("buyer_cnt").sum().cast(pl.Int32),
        refund_cents=pl.col("refund_cents").sum().cast(pl.Int64),
        shop_cnt=pl.len().cast(pl.Int32),
        platform=pl.col("platform").first(),
    )
    grouped = grouped.with_columns(avg_order_cents=(pl.col("gmv_cents") / pl.col("order_cnt")).cast(pl.Int64)).sort(
        ["platform", "stat_date"]
    )
    return (
        grouped.with_columns(gmv_dod_pct=_dod_pct(grouped, "gmv_cents").over("platform"))
        .filter(pl.col("stat_date") >= window_start)
        .select(DAILY_KPI_COLUMNS)
    )
