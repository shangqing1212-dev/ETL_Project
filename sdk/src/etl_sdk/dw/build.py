"""数仓构建编排(SDK 核心,CLI 见 scripts/build_dw.py): ODS → DWD(重算 upsert)→ 维表 → DWS → ADS(insert-overwrite 窗口)。

用法(项目根目录):
    uv run python scripts/build_dw.py                    # 增量构建: 最近 3 天(stat_date 窗口 [today-2, today])
    uv run python scripts/build_dw.py --days 7           # 最近 7 天
    uv run python scripts/build_dw.py --full             # 全量重建(2019-01-01 起)
    uv run python scripts/build_dw.py --since 2026-09-01 # 指定窗口起点(终点=今天)

幂等语义(ADR-003/005):
- dwd_*/dim_* 走 upsert 重算(updated_at 只升不降),同窗口重跑结果一致;
- dws_*/ads_* 走 insert-overwrite(先删窗口内旧值再重插),吸收口径变更;
- dim_shop SCD2 由注册表 diff 驱动,属性不变时重跑为 no-op;
- 失败不写"构建水位"(ODS 水位不参与),重跑自动重算。
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import sqlalchemy as sa
import yaml

from etl_sdk.alerts.base import AlertManager
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.transforms import (
    aggregate_product_daily,
    aggregate_products,
    aggregate_shop_daily,
    build_daily_kpi,
    build_shop_overview,
    diff_dim_shop,
    generate_date_rows,
    ods_to_dwd_items,
    ods_to_dwd_orders,
)
from etl_sdk.transforms.daily import DWS_PRODUCT_DAILY_COLUMNS, DWS_SHOP_DAILY_COLUMNS
from etl_sdk.transforms.orders import DWD_ITEM_COLUMNS, DWD_ORDER_COLUMNS

FULL_HISTORY_START = date(2019, 1, 1)
PRODUCT_ACTIVE_DAYS = 90  # dim_product.is_active 口径: 近 N 天有出现(与 transforms.dims 一致)

# polars schema_overrides 接受 dtype 类或实例(pl.Datetime 是类,pl.Datetime("us") 是实例)
_DataType = pl.DataType | type[pl.DataType]

# 空结果的显式 dtype(见 _read_sql docstring);覆盖全部列,空窗口下转换/join 仍需正确类型
_ORDERS_SCHEMA: dict[str, _DataType] = {
    "shop_id": pl.Int64,
    "order_id": pl.Utf8,
    "order_status": pl.Utf8,
    "buyer_nick": pl.Utf8,
    "order_amount_cents": pl.Int64,
    "payment_amount_cents": pl.Int64,
    "refund_amount_cents": pl.Int64,
    "is_deleted": pl.Int64,
    "created_at": pl.Datetime,
    "updated_at": pl.Datetime,
}
_ITEMS_SCHEMA: dict[str, _DataType] = {
    "shop_id": pl.Int64,
    "order_id": pl.Utf8,
    "item_id": pl.Utf8,
    "product_id": pl.Utf8,
    "product_name": pl.Utf8,
    "quantity": pl.Int64,
    "price_cents": pl.Int64,
    "updated_at": pl.Datetime,
}
_DWS_SHOP_SCHEMA: dict[str, _DataType] = {
    "stat_date": pl.Date,
    "shop_id": pl.Int64,
    "platform": pl.Utf8,
    "gmv_cents": pl.Int64,
    "order_cnt": pl.Int32,
    "buyer_cnt": pl.Int32,
    "refund_cents": pl.Int64,
    "avg_order_cents": pl.Int64,
    "etl_loaded_at": pl.Datetime,
}
_DWS_PRODUCT_SCHEMA: dict[str, _DataType] = {
    "stat_date": pl.Date,
    "shop_id": pl.Int64,
    "product_id": pl.Utf8,
    "platform": pl.Utf8,
    "sold_qty": pl.Int32,
    "sales_cents": pl.Int64,
    "etl_loaded_at": pl.Datetime,
}

DIM_DATE_COLUMNS = [
    "date_key",
    "stat_date",
    "year_num",
    "quarter_num",
    "month_num",
    "day_num",
    "week_of_year",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "holiday_name",
]
DIM_PRODUCT_COLUMNS = ["shop_id", "product_id", "product_name", "first_seen_at", "last_seen_at", "is_active"]
ADS_OVERVIEW_COLUMNS = [
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
ADS_KPI_COLUMNS = [
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


@dataclass
class BuildResult:
    """一次数仓构建的运行摘要(etl_batch 之外的结构化返回,供脚本/测试断言)。"""

    shop_id: int
    platform: str
    since: date
    end: date
    batch_id: str = ""
    orders_read: int = 0
    items_read: int = 0
    dwd_orders_written: int = 0
    dwd_items_written: int = 0
    dws_shop_rows: int = 0
    dws_product_rows: int = 0
    ads_overview_rows: int = 0
    ads_kpi_rows: int = 0


@dataclass
class BuildSummary:
    results: list[BuildResult] = field(default_factory=list)
    shop_versions_added: int = 0  # dim_shop SCD2 本次新开版本数(全局,非店铺维度)

    @property
    def orders_read(self) -> int:
        return sum(r.orders_read for r in self.results)


def load_shop_registry(path: Path) -> list[dict[str, Any]]:
    """读店铺注册表并校验(单源事实: M5 DAG 展开与抽取配置同源)。"""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    shops = raw.get("shops")
    if not isinstance(shops, list) or not shops:
        raise ValueError(f"店铺注册表 {path} 缺少 shops 列表")
    registry: list[dict[str, Any]] = []
    for entry in shops:
        if not isinstance(entry, dict):
            raise ValueError(f"注册表条目必须是映射: {entry!r}")
        try:
            shop_id = int(entry["shop_id"])
            platform = str(entry["platform"])
            shop_name = str(entry["shop_name"])
            status = str(entry.get("status", "active"))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"注册表条目缺 shop_id/platform/shop_name: {entry!r}") from exc
        if status not in ("active", "disabled"):
            raise ValueError(f"注册表条目 status 非法({status}): {entry!r}")
        registry.append({"shop_id": shop_id, "platform": platform, "shop_name": shop_name, "status": status})
    return registry


def _df_rows(df: pl.DataFrame) -> Iterator[dict[str, Any]]:
    """polars DataFrame → dict 行流(懒迭代,喂 MySQLBatchLoader,内存平稳)。"""
    yield from df.iter_rows(named=True)


def _upsert(
    engine: sa.Engine,
    table: str,
    columns: list[str],
    pk: list[str],
    df: pl.DataFrame,
    *,
    monotonic_col: str | None = "updated_at",
) -> int:
    return MySQLBatchLoader(engine, table, columns, pk, monotonic_col=monotonic_col).upsert(_df_rows(df))


def _delete_window(
    engine: sa.Engine,
    table: str,
    window_col: str,
    *,
    since: date,
    end: date,
    shop_id: int | None = None,
    platform: str | None = None,
) -> int:
    """insert-overwrite 语义: 删除窗口内旧值(shop/platform 参数用于限定范围)。"""
    clauses = [f"`{window_col}` >= :since", f"`{window_col}` <= :end"]
    binds: dict[str, Any] = {"since": since, "end": end}
    if shop_id is not None:
        clauses.append("shop_id = :s")
        binds["s"] = shop_id
    if platform is not None:
        clauses.append("platform = :p")
        binds["p"] = platform
    with engine.begin() as conn:
        result = conn.execute(sa.text(f"DELETE FROM {table} WHERE {' AND '.join(clauses)}"), binds)
    return result.rowcount or 0


def _read_sql(
    engine: sa.Engine,
    sql: str | sa.TextClause,
    binds: dict[str, Any],
    *,
    schema_overrides: dict[str, _DataType] | None = None,
) -> pl.DataFrame:
    """polars 读表(参数走 execute_options;TextClause 原样透传,str 自动包装)。

    schema_overrides 保证空结果也有正确 dtype(否则空帧列为 Null 类型,后续日期/比较运算会失败)。
    """
    query = sql if isinstance(sql, sa.TextClause) else sa.text(sql)
    with engine.connect() as conn:
        return pl.read_database(
            query, connection=conn, execute_options={"parameters": binds}, schema_overrides=schema_overrides
        )


def _sync_dim_shop(engine: sa.Engine, registry: list[dict[str, Any]], *, effective: date) -> int:
    """注册表 → dim_shop SCD2(属性变化开新版本并关闭旧版本;无变化 no-op)。"""
    with engine.connect() as conn:
        current = [dict(r._mapping) for r in conn.execute(sa.text("SELECT * FROM dw.dim_shop"))]
    diff = diff_dim_shop(registry, current, effective=effective)
    with engine.begin() as conn:
        for key in diff.close_keys:
            conn.execute(
                sa.text("UPDATE dw.dim_shop SET is_current = 0, valid_to = :d WHERE shop_key = :k"),
                {"d": effective, "k": key},
            )
        for row in diff.new_rows:
            conn.execute(
                sa.text(
                    "INSERT INTO dw.dim_shop (shop_id, platform, shop_name, status, valid_from) "
                    "VALUES (:shop_id, :platform, :shop_name, :status, :valid_from)"
                ),
                row,
            )
    return len(diff.new_rows)


def _fill_dim_date(engine: sa.Engine, *, since: date, now: datetime) -> int:
    """dim_date 幂等填充(覆盖窗口前后各一年,重跑只刷新节假日字段)。"""
    rows = generate_date_rows(since - timedelta(days=365), now.date() + timedelta(days=366))
    with engine.begin() as conn:
        # 行数小(~1100 行),直接逐行 executemany 一次提交
        conn.execute(
            sa.text(
                "INSERT INTO dw.dim_date "
                "(date_key, stat_date, year_num, quarter_num, month_num, day_num, week_of_year, "
                " day_of_week, is_weekend, is_holiday, holiday_name) "
                "VALUES (:date_key, :stat_date, :year_num, :quarter_num, :month_num, :day_num, "
                " :week_of_year, :day_of_week, :is_weekend, :is_holiday, :holiday_name) "
                "AS new ON DUPLICATE KEY UPDATE "
                "is_weekend = new.`is_weekend`, is_holiday = new.`is_holiday`, holiday_name = new.`holiday_name`"
            ),
            rows,
        )
    return len(rows)


def _read_ods_orders(engine: sa.Engine, shop: dict[str, Any], *, since: date, end: date) -> pl.DataFrame:
    sql = sa.text(
        "SELECT shop_id, order_id, order_status, buyer_nick, order_amount_cents, payment_amount_cents, "
        "       refund_amount_cents, is_deleted, created_at, updated_at "
        "FROM dw.ods_orders "
        "WHERE shop_id = :s AND platform = :p "
        "  AND DATE(COALESCE(created_at, updated_at)) >= :since "
        "  AND DATE(COALESCE(created_at, updated_at)) <= :end"
    )
    return _read_sql(
        engine,
        sql,
        {"s": shop["shop_id"], "p": shop["platform"], "since": since, "end": end},
        schema_overrides=_ORDERS_SCHEMA,
    )


def _read_ods_items(engine: sa.Engine, shop: dict[str, Any]) -> pl.DataFrame:
    """读店铺全部明细(商品维需全量;窗口过滤由 join 完成,明细无 created_at)。"""
    sql = sa.text(
        "SELECT shop_id, order_id, item_id, product_id, product_name, quantity, price_cents, updated_at "
        "FROM dw.ods_order_items WHERE shop_id = :s AND platform = :p"
    )
    return _read_sql(engine, sql, {"s": shop["shop_id"], "p": shop["platform"]}, schema_overrides=_ITEMS_SCHEMA)


def _build_shop(
    engine: sa.Engine,
    shop: dict[str, Any],
    *,
    since: date,
    end: date,
    now: datetime,
    recorder: BatchRecorder,
    alert_manager: AlertManager | None,
) -> BuildResult:
    shop_id = shop["shop_id"]
    platform = shop["platform"]
    batch_id = recorder.start_batch(
        table_name="dw.build",
        shop_id=shop_id,
        platform=platform,
        run_type="transform",
        window_start=datetime.combine(since, datetime.min.time()),
        window_end=datetime.combine(end, datetime.min.time()),
    )
    result = BuildResult(shop_id=shop_id, platform=platform, since=since, end=end, batch_id=batch_id)
    try:
        ods_orders = _read_ods_orders(engine, shop, since=since, end=end)
        ods_items = _read_ods_items(engine, shop)
        result.orders_read = ods_orders.height
        result.items_read = ods_items.height

        # DWD: 清洗 + 状态归一,窗口内 upsert 重算(stat_date 为 DWD 生成列,不进列清单)
        dwd_orders = ods_to_dwd_orders(ods_orders, platform=platform, batch_id=batch_id)
        if ods_orders.is_empty():
            # 空窗口: 跳过 join(空帧列为 Null 类型,join 键类型不匹配),保留 items 真实 schema 的空帧
            window_items = ods_items.filter(pl.lit(False))
        else:
            window_items = ods_items.join(
                ods_orders.select(["shop_id", "order_id"]), on=["shop_id", "order_id"], how="inner"
            )
        dwd_items = ods_to_dwd_items(window_items, platform=platform, batch_id=batch_id)
        result.dwd_orders_written = _upsert(
            engine, "dw.dwd_orders", DWD_ORDER_COLUMNS, ["shop_id", "order_id"], dwd_orders
        )
        result.dwd_items_written = _upsert(
            engine, "dw.dwd_order_items", DWD_ITEM_COLUMNS, ["shop_id", "order_id", "item_id"], dwd_items
        )

        # dim_product: 最新态轨(全量明细重算,last_seen/name 收敛)
        products = aggregate_products(ods_items, now=now)
        _upsert(engine, "dw.dim_product", DIM_PRODUCT_COLUMNS, ["shop_id", "product_id"], products, monotonic_col=None)

        # DWS: insert-overwrite 窗口(删旧插新,吸收口径变更)
        shop_daily = aggregate_shop_daily(dwd_orders)
        product_daily = aggregate_product_daily(dwd_orders, dwd_items)
        _delete_window(
            engine, "dw.dws_shop_daily", "stat_date", since=since, end=end, shop_id=shop_id, platform=platform
        )
        _delete_window(
            engine, "dw.dws_product_daily", "stat_date", since=since, end=end, shop_id=shop_id, platform=platform
        )
        result.dws_shop_rows = _upsert(
            engine,
            "dw.dws_shop_daily",
            DWS_SHOP_DAILY_COLUMNS,
            ["stat_date", "shop_id"],
            shop_daily,
            monotonic_col=None,
        )
        result.dws_product_rows = _upsert(
            engine,
            "dw.dws_product_daily",
            DWS_PRODUCT_DAILY_COLUMNS,
            ["stat_date", "shop_id", "product_id"],
            product_daily,
            monotonic_col=None,
        )

        # ADS 店铺日报: 环比需前一日 DWS 行;动销数来自商品日汇总;在库总数来自 dim_product
        with engine.connect() as conn:
            product_total = conn.execute(
                sa.text("SELECT COUNT(*) FROM dw.dim_product WHERE shop_id = :s"), {"s": shop_id}
            ).scalar_one()
        overview = build_shop_overview(
            _read_sql(
                engine,
                "SELECT * FROM dw.dws_shop_daily WHERE shop_id = :s AND platform = :p AND stat_date >= :since",
                {"s": shop_id, "p": platform, "since": since - timedelta(days=1)},
                schema_overrides=_DWS_SHOP_SCHEMA,
            ),
            _read_sql(
                engine,
                "SELECT * FROM dw.dws_product_daily WHERE shop_id = :s AND platform = :p AND stat_date >= :since",
                {"s": shop_id, "p": platform, "since": since},
                schema_overrides=_DWS_PRODUCT_SCHEMA,
            ),
            int(product_total),
            window_start=since,
        )
        _delete_window(
            engine, "dw.ads_shop_overview", "stat_date", since=since, end=end, shop_id=shop_id, platform=platform
        )
        result.ads_overview_rows = _upsert(
            engine, "dw.ads_shop_overview", ADS_OVERVIEW_COLUMNS, ["stat_date", "shop_id"], overview, monotonic_col=None
        )

        recorder.finish_batch(
            batch_id,
            status="success",
            rows_read=result.orders_read + result.items_read,
            rows_written=result.dwd_orders_written + result.dwd_items_written,
        )
        return result
    except Exception as exc:  # noqa: BLE001 —— 批次失败统一记账 + 告警后重抛
        recorder.finish_batch(batch_id, status="failed", error_msg=str(exc))
        if alert_manager is not None:
            alert_manager.send(
                title="数仓构建失败",
                text=f"shop_id={shop_id} platform={platform} 窗口=[{since}, {end}] 失败: {exc}",
                level="error",
            )
        raise


def _build_daily_kpi(
    engine: sa.Engine,
    platform: str,
    *,
    since: date,
    end: date,
    recorder: BatchRecorder,
    alert_manager: AlertManager | None,
) -> int:
    """跨店全局 KPI(平台级;读全平台 DWS,环比窗口含前一日)。"""
    batch_id = recorder.start_batch(
        table_name="dw.build_kpi",
        shop_id=0,  # 平台级构建无店铺维度
        platform=platform,
        run_type="transform",
        window_start=datetime.combine(since, datetime.min.time()),
        window_end=datetime.combine(end, datetime.min.time()),
    )
    try:
        kpi = build_daily_kpi(
            _read_sql(
                engine,
                "SELECT * FROM dw.dws_shop_daily WHERE platform = :p AND stat_date >= :since",
                {"p": platform, "since": since - timedelta(days=1)},
                schema_overrides=_DWS_SHOP_SCHEMA,
            ),
            window_start=since,
        )
        _delete_window(engine, "dw.ads_daily_kpi", "stat_date", since=since, end=end, platform=platform)
        written = _upsert(
            engine, "dw.ads_daily_kpi", ADS_KPI_COLUMNS, ["stat_date", "platform"], kpi, monotonic_col=None
        )
        recorder.finish_batch(batch_id, status="success", rows_written=written)
        return written
    except Exception as exc:  # noqa: BLE001
        recorder.finish_batch(batch_id, status="failed", error_msg=str(exc))
        if alert_manager is not None:
            alert_manager.send(
                title="数仓构建失败(KPI)",
                text=f"platform={platform} 窗口=[{since}, {end}] KPI 构建失败: {exc}",
                level="error",
            )
        raise


def run_build(
    engine: sa.Engine,
    registry: list[dict[str, Any]],
    *,
    since: date,
    now: datetime,
    alert_manager: AlertManager | None = None,
) -> BuildSummary:
    """数仓构建编排(幂等;各店结果独立,单店失败不影响批次记账并重抛)。

    顺序: dim_date 填充 → dim_shop SCD2 diff → 逐店 DWD/dim_product/DWS/店铺 ADS →
    按平台汇总 ads_daily_kpi。
    """
    end = now.date()
    if since > end:
        raise ValueError(f"窗口起点 {since} 晚于终点 {end}")

    _fill_dim_date(engine, since=since, now=now)
    summary = BuildSummary(shop_versions_added=_sync_dim_shop(engine, registry, effective=end))
    recorder = BatchRecorder(engine)

    for shop in registry:
        summary.results.append(
            _build_shop(engine, shop, since=since, end=end, now=now, recorder=recorder, alert_manager=alert_manager)
        )

    platforms = sorted({str(shop["platform"]) for shop in registry})
    for platform in platforms:
        for result in summary.results:
            if result.platform == platform:
                result.ads_kpi_rows = _build_daily_kpi(
                    engine, platform, since=since, end=end, recorder=recorder, alert_manager=alert_manager
                )
                break
    return summary
