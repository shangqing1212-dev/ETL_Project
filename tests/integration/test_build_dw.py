"""数仓构建集成测试: ODS 种子 → run_build → 校验 DWD/DWS/ADS 数值与口径、幂等、窗口覆盖语义。"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import sqlalchemy as sa
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.mappers.orders import order_items_to_ods, order_to_ods

from scripts.build_dw import load_shop_registry, run_build
from tests.integration.helpers import (
    ITEM_COLUMNS,
    ITEM_PK,
    ITEMS_TABLE,
    ORDER_COLUMNS,
    ORDER_PK,
    ORDERS_TABLE,
    make_order,
)

NOW = datetime(2026, 9, 17, 10, 0)
REGISTRY = [{"shop_id": 1, "platform": "mock", "shop_name": "演示店铺", "status": "active"}]


@pytest.fixture()
def seed_ods(mysql_engine: sa.Engine):
    """把 make_order 行直接写入 ODS(绕过抽取,聚焦构建层)。"""

    def _seed(orders: list[dict]) -> None:
        order_rows = [order_to_ods(o, batch_id="b-seed", platform="mock") for o in orders]
        item_rows = [r for o in orders for r in order_items_to_ods(o, batch_id="b-seed", platform="mock")]
        MySQLBatchLoader(mysql_engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK).upsert(order_rows)
        MySQLBatchLoader(mysql_engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK).upsert(item_rows)

    return _seed


def _scalar(engine: sa.Engine, sql: str) -> object:
    with engine.connect() as conn:
        return conn.execute(sa.text(sql)).scalar_one()


def _rows(engine: sa.Engine, sql: str) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.text(sql))]


def test_build_end_to_end_and_idempotent(mysql_engine: sa.Engine, clean_state, seed_ods) -> None:
    seed_ods(
        [
            # 9-15 有效订单 x2(payment 100.00 + 50.00,后者全额退款 → refunded)
            make_order(
                1,
                updated=datetime(2026, 9, 15, 8, 0),
                payment_amount="100.00",
                items=[
                    {"item_id": "101", "product_id": "P001", "product_name": "商品", "quantity": 2, "price": "1.00"},
                    {"item_id": "102", "product_id": "P002", "product_name": "商品", "quantity": 1, "price": "2.00"},
                ],
            ),
            make_order(
                2,
                updated=datetime(2026, 9, 15, 9, 0),
                status="completed",
                payment_amount="50.00",
                refund_amount="50.00",
                items=[
                    {"item_id": "201", "product_id": "P001", "product_name": "商品", "quantity": 1, "price": "1.00"}
                ],
            ),
            # 9-16 取消单(不进 GMV,但进 DWD)
            make_order(3, updated=datetime(2026, 9, 16, 8, 0), status="cancelled", payment_amount="30.00"),
            # 9-14 窗口外(默认窗口 [9-15, 9-17],不应重建)
            make_order(4, updated=datetime(2026, 9, 14, 8, 0), payment_amount="40.00"),
        ]
    )

    summary = run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    result = summary.results[0]
    assert result.orders_read == 3  # 窗口内 3 单(o4 在窗口外)
    assert result.dwd_orders_written == 3
    assert summary.shop_versions_added == 1

    # DWD: 状态归一 + 生成列 stat_date
    dwd = _rows(mysql_engine, "SELECT * FROM dw.dwd_orders ORDER BY order_id")
    assert [r["order_id"] for r in dwd] == ["C0001", "C0002", "C0003"]
    assert {r["order_id"]: r["status_norm"] for r in dwd} == {
        "C0001": "paid",
        "C0002": "refunded",
        "C0003": "cancelled",
    }
    assert dwd[0]["stat_date"] == date(2026, 9, 15)  # 生成列 = 下单日

    # DWS: GMV 口径(15000 分 = 10000 + 5000;cancelled 不计;o4 不在窗口)
    dws = _rows(mysql_engine, "SELECT * FROM dw.dws_shop_daily ORDER BY stat_date")
    assert len(dws) == 1
    row = dws[0]
    assert row["stat_date"] == date(2026, 9, 15)
    assert row["gmv_cents"] == 15000 and row["order_cnt"] == 2
    assert row["buyer_cnt"] == 2 and row["refund_cents"] == 5000
    assert row["avg_order_cents"] == 7500

    prod = _rows(mysql_engine, "SELECT * FROM dw.dws_product_daily ORDER BY product_id")
    assert {r["product_id"]: (r["sold_qty"], r["sales_cents"]) for r in prod} == {
        "P001": (3, 300),  # o1 x2 + o2 x1,单价 1.00 元
        "P002": (1, 200),  # o1 第二件,单价 2.00 元
    }

    # ADS 店铺日报: 环比无前日 → NULL;退款率 1/3;商品维全量 → 在库=4(含取消单 o3、窗口外 o4 的商品),动销=2
    ads = _rows(mysql_engine, "SELECT * FROM dw.ads_shop_overview")
    assert len(ads) == 1
    a = ads[0]
    assert a["gmv_cents"] == 15000 and a["gmv_dod_pct"] is None
    assert float(a["refund_rate"]) == pytest.approx(0.3333)
    assert a["active_product_cnt"] == 2 and a["product_total_cnt"] == 4
    assert float(a["sell_through_rate"]) == pytest.approx(0.5)

    # ADS KPI: 平台聚合
    kpi = _rows(mysql_engine, "SELECT * FROM dw.ads_daily_kpi")
    assert len(kpi) == 1 and kpi[0]["gmv_cents"] == 15000 and kpi[0]["shop_cnt"] == 1

    # 维表: dim_shop 当前版本 + dim_date 覆盖窗口
    shop_rows = _rows(mysql_engine, "SELECT * FROM dw.dim_shop WHERE is_current = 1")
    assert len(shop_rows) == 1 and shop_rows[0]["shop_name"] == "演示店铺"
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM dw.dim_date WHERE stat_date = '2026-09-15'") == 1

    # 批次记账成功
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM etl_meta.etl_batch WHERE status = 'success'") >= 2

    # 幂等重跑: 数值不变(etl_loaded_at 是写入时间戳,重跑会刷新)、SCD2 no-op、DWS 行数不翻倍
    summary2 = run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    assert summary2.shop_versions_added == 0

    def _strip(rows: list[dict]) -> list[dict]:
        return [{k: v for k, v in r.items() if k != "etl_loaded_at"} for r in rows]

    assert _strip(_rows(mysql_engine, "SELECT * FROM dw.dws_shop_daily")) == _strip(dws)
    assert _strip(_rows(mysql_engine, "SELECT * FROM dw.ads_shop_overview")) == _strip(ads)


def test_rebuild_absorbs_ods_change(mysql_engine: sa.Engine, clean_state, seed_ods) -> None:
    """同窗口重跑吸收 ODS 变更: DWD upsert 更新、DWS/ADS insert-overwrite 覆盖旧值。"""
    seed_ods([make_order(1, updated=datetime(2026, 9, 15, 8, 0), payment_amount="100.00")])
    run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    assert _scalar(mysql_engine, "SELECT gmv_cents FROM dw.dws_shop_daily WHERE stat_date = '2026-09-15'") == 10000

    # ODS 更新(updated_at 后移、金额改为 200.00)→ 重跑同窗口
    seed_ods([make_order(1, updated=datetime(2026, 9, 15, 9, 0), payment_amount="200.00")])
    run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    assert _scalar(mysql_engine, "SELECT gmv_cents FROM dw.dws_shop_daily WHERE stat_date = '2026-09-15'") == 20000
    assert _scalar(mysql_engine, "SELECT payment_amount_cents FROM dw.dwd_orders WHERE order_id = 'C0001'") == 20000
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM dw.dws_shop_daily") == 1  # 覆盖非追加


def test_window_rebuild_overwrites_only_window(mysql_engine: sa.Engine, clean_state, seed_ods) -> None:
    """insert-overwrite 语义: 窗口内旧值被覆盖(停售商品行消失),窗口外历史行不受影响。"""
    seed_ods(
        [
            make_order(
                1,
                updated=datetime(2026, 9, 15, 8, 0),
                payment_amount="100.00",
                items=[{"item_id": "101", "product_id": "P1", "product_name": "商品", "quantity": 1, "price": "1.00"}],
            ),
            make_order(
                2,
                updated=datetime(2026, 9, 16, 8, 0),
                payment_amount="200.00",
                items=[{"item_id": "201", "product_id": "P1", "product_name": "商品", "quantity": 1, "price": "1.00"}],
            ),
        ]
    )
    run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM dw.dws_shop_daily") == 2

    # 9-16 订单商品改为 P2 → 只重建 9-16 窗口
    seed_ods(
        [
            make_order(
                2,
                updated=datetime(2026, 9, 16, 9, 0),
                payment_amount="200.00",
                items=[{"item_id": "201", "product_id": "P2", "product_name": "商品", "quantity": 1, "price": "1.00"}],
            )
        ]
    )
    run_build(mysql_engine, REGISTRY, since=date(2026, 9, 16), now=NOW)
    rows = _rows(mysql_engine, "SELECT stat_date FROM dw.dws_shop_daily ORDER BY stat_date")
    assert rows == [{"stat_date": date(2026, 9, 15)}, {"stat_date": date(2026, 9, 16)}]  # 窗口外 9-15 保留
    prod_16 = _rows(mysql_engine, "SELECT product_id FROM dw.dws_product_daily WHERE stat_date = '2026-09-16'")
    assert prod_16 == [{"product_id": "P2"}]  # 窗口内旧商品行被覆盖(P1 消失)
    prod_15 = _rows(mysql_engine, "SELECT product_id FROM dw.dws_product_daily WHERE stat_date = '2026-09-15'")
    assert prod_15 == [{"product_id": "P1"}]
    assert _scalar(mysql_engine, "SELECT gmv_cents FROM dw.dws_shop_daily WHERE stat_date = '2026-09-15'") == 10000


def test_full_rebuild_covers_history(mysql_engine: sa.Engine, clean_state, seed_ods) -> None:
    """--full 语义: 全量窗口覆盖任意历史订单。"""
    seed_ods([make_order(1, updated=datetime(2026, 8, 1, 8, 0), payment_amount="100.00")])
    run_build(mysql_engine, REGISTRY, since=date(2026, 9, 15), now=NOW)
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM dw.dwd_orders") == 0

    run_build(mysql_engine, REGISTRY, since=date(2019, 1, 1), now=NOW)
    assert _scalar(mysql_engine, "SELECT COUNT(*) FROM dw.dwd_orders") == 1
    assert _scalar(mysql_engine, "SELECT gmv_cents FROM dw.dws_shop_daily WHERE stat_date = '2026-08-01'") == 10000


def test_load_shop_registry_validation(tmp_path) -> None:
    p = tmp_path / "shops.yaml"
    p.write_text(
        "shops:\n  - shop_id: 1\n    platform: mock\n    shop_name: 店\n    status: active\n", encoding="utf-8"
    )
    assert load_shop_registry(p) == [{"shop_id": 1, "platform": "mock", "shop_name": "店", "status": "active"}]

    p.write_text("shops: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="缺少 shops"):
        load_shop_registry(p)

    p.write_text("shops:\n  - shop_id: 1\n    platform: mock\n    shop_name: 店\n    status: 幽灵\n", encoding="utf-8")
    with pytest.raises(ValueError, match="status 非法"):
        load_shop_registry(p)
