"""转换层单测: 状态归一、ODS→DWD、DWS 聚合口径、ADS 宽表、SCD2 diff、日期/商品维。"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest
from etl_sdk.transforms import (
    aggregate_product_daily,
    aggregate_products,
    aggregate_shop_daily,
    build_daily_kpi,
    build_shop_overview,
    diff_dim_shop,
    generate_date_rows,
    normalize_status,
    ods_to_dwd_items,
    ods_to_dwd_orders,
)


def _orders_df(rows: list[dict]) -> pl.DataFrame:
    """构造 dwd_orders 形状的测试帧(status_norm 已归一,模拟 build_dw 内部输入)。"""
    base = {
        "shop_id": 1,
        "order_id": "",
        "platform": "mock",
        "status_raw": "",
        "status_norm": "paid",
        "buyer_nick": "",
        "order_amount_cents": 0,
        "payment_amount_cents": 0,
        "refund_amount_cents": 0,
        "is_deleted": 0,
        "created_at": datetime(2026, 9, 15, 10, 0),
        "updated_at": datetime(2026, 9, 15, 10, 0),
        "etl_batch_id": "b1",
    }
    for i, row in enumerate(rows):
        merged = dict(base)
        merged.update(row)
        merged.setdefault("order_id", f"o{i}")
        rows[i] = merged
    return pl.DataFrame(rows)


# ---- 状态归一 ----


def test_normalize_status_passthrough() -> None:
    assert normalize_status("mock", "paid", refund_cents=0, payment_cents=100) == "paid"


def test_normalize_status_refunded_derivation() -> None:
    # 发货后全额退款 → refunded;cancelled 带退款不是"退款单"
    assert normalize_status("mock", "completed", refund_cents=100, payment_cents=100) == "refunded"
    assert normalize_status("mock", "cancelled", refund_cents=100, payment_cents=100) == "cancelled"


def test_normalize_status_unknown() -> None:
    assert normalize_status("mock", "mystery", refund_cents=0, payment_cents=0) == "unknown"
    assert normalize_status("mock", None, refund_cents=0, payment_cents=0) == "unknown"


def test_normalize_status_platform_map() -> None:
    from etl_sdk.transforms.orders import PLATFORM_STATUS_MAP

    PLATFORM_STATUS_MAP["taobao"] = {"TRADE_FINISHED": "completed"}
    try:
        assert normalize_status("taobao", "TRADE_FINISHED", refund_cents=0, payment_cents=0) == "completed"
    finally:
        del PLATFORM_STATUS_MAP["taobao"]


# ---- ODS → DWD ----


def test_ods_to_dwd_orders() -> None:
    df = pl.DataFrame(
        [
            {
                "shop_id": 1,
                "order_id": "o1",
                "order_status": "shipped",
                "buyer_nick": "u1",
                "order_amount_cents": 100,
                "payment_amount_cents": 90,
                "refund_amount_cents": 90,
                "is_deleted": 0,
                "created_at": datetime(2026, 9, 15),
                "updated_at": datetime(2026, 9, 16),
            }
        ]
    )
    out = ods_to_dwd_orders(df, platform="mock", batch_id="b1")
    row = out.to_dicts()[0]
    assert row["status_norm"] == "refunded"  # shipped + 退款 → 派生
    assert row["platform"] == "mock" and row["etl_batch_id"] == "b1"
    assert "stat_date" not in out.columns  # 生成列由数据库计算


def test_ods_to_dwd_items_amount() -> None:
    df = pl.DataFrame(
        [
            {
                "shop_id": 1,
                "order_id": "o1",
                "item_id": "i1",
                "product_id": "p1",
                "product_name": "n1",
                "quantity": 3,
                "price_cents": 250,
                "updated_at": datetime(2026, 9, 15),
            },
            {
                "shop_id": 1,
                "order_id": "o1",
                "item_id": "i2",
                "product_id": "p2",
                "product_name": "n2",
                "quantity": None,
                "price_cents": 100,
                "updated_at": datetime(2026, 9, 15),
            },
        ]
    )
    rows = ods_to_dwd_items(df, platform="mock", batch_id="b1").to_dicts()
    assert rows[0]["amount_cents"] == 750
    assert rows[1]["amount_cents"] is None  # 数量缺失 → 行金额 NULL


# ---- DWS 聚合口径 ----


def test_aggregate_shop_daily_gmv_scope() -> None:
    """GMV 口径: 只算 paid/shipped/completed/refunded;pending/cancelled/软删不计。"""
    df = _orders_df(
        [
            {"status_norm": "paid", "payment_amount_cents": 100, "refund_amount_cents": 0, "buyer_nick": "a"},
            {"status_norm": "pending", "payment_amount_cents": 999, "buyer_nick": "b"},
            {"status_norm": "cancelled", "payment_amount_cents": 999, "buyer_nick": "c"},
            {"status_norm": "refunded", "payment_amount_cents": 50, "refund_amount_cents": 50, "buyer_nick": "a"},
            {"status_norm": "paid", "payment_amount_cents": 100, "is_deleted": 1, "buyer_nick": "d"},
        ]
    )
    rows = aggregate_shop_daily(df).to_dicts()
    assert len(rows) == 1
    row = rows[0]
    assert row["stat_date"] == date(2026, 9, 15)
    assert row["gmv_cents"] == 150  # 100 + 50,不含 pending/cancelled/软删
    assert row["order_cnt"] == 2
    assert row["buyer_cnt"] == 1  # a 买两单去重
    assert row["refund_cents"] == 50
    assert row["avg_order_cents"] == 75  # 150/2


def test_aggregate_product_daily_only_paid_items() -> None:
    orders = _orders_df(
        [
            {"order_id": "paid_o", "status_norm": "paid", "payment_amount_cents": 100},
            {"order_id": "cxl_o", "status_norm": "cancelled", "payment_amount_cents": 0},
        ]
    )
    items = pl.DataFrame(
        [
            {"shop_id": 1, "order_id": "paid_o", "product_id": "p1", "quantity": 2, "amount_cents": 200},
            {"shop_id": 1, "order_id": "cxl_o", "product_id": "p9", "quantity": 5, "amount_cents": 500},
        ]
    )
    rows = aggregate_product_daily(orders, items).to_dicts()
    assert rows == [
        {
            "stat_date": date(2026, 9, 15),
            "shop_id": 1,
            "product_id": "p1",
            "platform": "mock",
            "sold_qty": 2,
            "sales_cents": 200,
        }
    ]


# ---- ADS ----


def test_build_shop_overview_dod_and_rates() -> None:
    dws = pl.DataFrame(
        [
            {
                "stat_date": date(2026, 9, 14),
                "shop_id": 1,
                "platform": "mock",
                "gmv_cents": 100,
                "order_cnt": 2,
                "buyer_cnt": 2,
                "refund_cents": 10,
                "avg_order_cents": 50,
            },
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 1,
                "platform": "mock",
                "gmv_cents": 200,
                "order_cnt": 4,
                "buyer_cnt": 3,
                "refund_cents": 0,
                "avg_order_cents": 50,
            },
        ]
    )
    prod_daily = pl.DataFrame(
        [
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 1,
                "product_id": "p1",
                "platform": "mock",
                "sold_qty": 1,
                "sales_cents": 100,
            },
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 1,
                "product_id": "p2",
                "platform": "mock",
                "sold_qty": 1,
                "sales_cents": 100,
            },
        ]
    )
    rows = build_shop_overview(dws, prod_daily, product_total_cnt=8, window_start=date(2026, 9, 15)).to_dicts()
    assert len(rows) == 1  # 窗口外的前一日行只用于环比,不产出
    row = rows[0]
    assert row["stat_date"] == date(2026, 9, 15)
    assert row["gmv_dod_pct"] == pytest.approx(100.0)  # 100 → 200
    assert row["order_dod_pct"] == pytest.approx(100.0)
    assert row["refund_rate"] == pytest.approx(0.0)
    assert row["active_product_cnt"] == 2
    assert row["product_total_cnt"] == 8
    assert row["sell_through_rate"] == pytest.approx(0.25)


def test_build_shop_overview_zero_guards() -> None:
    """gmv=0 / 在库=0 时退款率与动销率为 0,不除零。"""
    dws = pl.DataFrame(
        [
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 1,
                "platform": "mock",
                "gmv_cents": 0,
                "order_cnt": 0,
                "buyer_cnt": 0,
                "refund_cents": 0,
                "avg_order_cents": 0,
            },
        ]
    )
    row = build_shop_overview(
        dws, pl.DataFrame(schema={"stat_date": pl.Date, "shop_id": pl.Int64}), 0, window_start=date(2026, 9, 15)
    ).to_dicts()[0]
    assert row["refund_rate"] == 0 and row["sell_through_rate"] == 0
    assert row["gmv_dod_pct"] is None  # 无前日数据


def test_build_daily_kpi() -> None:
    dws = pl.DataFrame(
        [
            {
                "stat_date": date(2026, 9, 14),
                "shop_id": 1,
                "platform": "mock",
                "gmv_cents": 100,
                "order_cnt": 2,
                "buyer_cnt": 2,
                "refund_cents": 0,
                "avg_order_cents": 50,
            },
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 1,
                "platform": "mock",
                "gmv_cents": 100,
                "order_cnt": 2,
                "buyer_cnt": 2,
                "refund_cents": 0,
                "avg_order_cents": 50,
            },
            {
                "stat_date": date(2026, 9, 15),
                "shop_id": 2,
                "platform": "mock",
                "gmv_cents": 300,
                "order_cnt": 3,
                "buyer_cnt": 3,
                "refund_cents": 0,
                "avg_order_cents": 100,
            },
        ]
    )
    rows = build_daily_kpi(dws, window_start=date(2026, 9, 15)).to_dicts()
    assert len(rows) == 1
    row = rows[0]
    assert row["gmv_cents"] == 400 and row["order_cnt"] == 5 and row["shop_cnt"] == 2
    assert row["avg_order_cents"] == 80
    assert row["gmv_dod_pct"] == pytest.approx(300.0)  # 100 → 400


# ---- 维表 ----


def test_diff_dim_shop_new_and_changed() -> None:
    registry = [{"shop_id": 1, "platform": "mock", "shop_name": "新店名", "status": "active"}]
    current = [
        {"shop_key": 10, "shop_id": 1, "platform": "mock", "shop_name": "旧店名", "status": "active", "is_current": 1}
    ]
    diff = diff_dim_shop(registry, current, effective=date(2026, 9, 17))
    assert diff.close_keys == [10]
    assert diff.new_rows == [
        {
            "shop_id": 1,
            "platform": "mock",
            "shop_name": "新店名",
            "status": "active",
            "valid_from": date(2026, 9, 17),
            "valid_to": None,
            "is_current": 1,
        }
    ]


def test_diff_dim_shop_unchanged_is_noop() -> None:
    registry = [{"shop_id": 1, "platform": "mock", "shop_name": "店", "status": "active"}]
    current = [
        {"shop_key": 10, "shop_id": 1, "platform": "mock", "shop_name": "店", "status": "active", "is_current": 1}
    ]
    diff = diff_dim_shop(registry, current, effective=date(2026, 9, 17))
    assert diff.close_keys == [] and diff.new_rows == []  # 重跑幂等


def test_diff_dim_shop_removed_closes_current() -> None:
    registry: list[dict] = []
    current = [
        {"shop_key": 10, "shop_id": 1, "platform": "mock", "shop_name": "店", "status": "active", "is_current": 1},
        {"shop_key": 11, "shop_id": 1, "platform": "mock", "shop_name": "店", "status": "active", "is_current": 0},
    ]
    diff = diff_dim_shop(registry, current, effective=date(2026, 9, 17))
    assert diff.close_keys == [10]  # 只关当前版本


def test_generate_date_rows() -> None:
    rows = generate_date_rows(date(2026, 9, 15), date(2026, 9, 16))
    assert len(rows) == 2
    tue = rows[0]
    assert tue["date_key"] == 20260915
    assert tue["day_of_week"] == 2  # 2026-09-15 是周二
    assert tue["is_weekend"] == 0 and tue["is_holiday"] == 0
    assert rows[0] == generate_date_rows(date(2026, 9, 15), date(2026, 9, 16))[0]  # 确定性


def test_generate_date_rows_holiday() -> None:
    rows = {r["stat_date"]: r for r in generate_date_rows(date(2026, 10, 1), date(2026, 10, 1))}
    row = rows[date(2026, 10, 1)]
    assert row["is_holiday"] == 1 and row["holiday_name"] == "国庆节"


def test_aggregate_products() -> None:
    items = pl.DataFrame(
        [
            {"shop_id": 1, "product_id": "p1", "product_name": "旧名", "updated_at": datetime(2026, 6, 1)},
            {"shop_id": 1, "product_id": "p1", "product_name": "新名", "updated_at": datetime(2026, 9, 15)},
            {"shop_id": 1, "product_id": "p2", "product_name": "老货", "updated_at": datetime(2025, 1, 1)},
        ]
    )
    rows = aggregate_products(items, now=datetime(2026, 9, 17)).to_dicts()
    by_id = {r["product_id"]: r for r in rows}
    p1 = by_id["p1"]
    assert p1["product_name"] == "新名"  # 取 updated_at 最新
    assert p1["first_seen_at"] == datetime(2026, 6, 1)
    assert p1["last_seen_at"] == datetime(2026, 9, 15)
    assert p1["is_active"] == 1
    assert by_id["p2"]["is_active"] == 0  # 超出 90 天 → 非活跃
