"""pandera 契约单测: 好坏行分离、非法值拦截、严格模式缺列拦截。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from etl_sdk.dq.contracts import order_items_ods_contract, orders_ods_contract


def _order_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "order_id": "MOCK1",
        "shop_id": 1,
        "platform": "mock",
        "order_status": "paid",
        "buyer_nick": "u1",
        "order_amount_cents": 1000,
        "payment_amount_cents": 1000,
        "refund_amount_cents": 0,
        "raw_json": "{}",
        "is_deleted": 0,
        "created_at": datetime(2026, 9, 1, 10, 0, 0),
        "updated_at": datetime(2026, 9, 1, 10, 0, 0),
        "etl_batch_id": "b1",
    }
    row.update(overrides)
    return row


def test_good_rows_pass() -> None:
    result = orders_ods_contract().validate([_order_row(), _order_row(order_id="MOCK2")])
    assert result.bad_count == 0
    assert len(result.good_rows) == 2


def test_negative_amount_rejected() -> None:
    result = orders_ods_contract().validate([_order_row(order_amount_cents=-1)])
    assert result.bad_count == 1
    assert "order_amount_cents" in result.bad_rows[0][1]


def test_empty_order_id_rejected() -> None:
    result = orders_ods_contract().validate([_order_row(order_id="")])
    assert result.bad_count == 1


def test_missing_column_rejects_all_rows_strict() -> None:
    row = _order_row()
    del row["order_id"]
    result = orders_ods_contract().validate([row])
    assert result.bad_count == 1
    assert result.good_rows == []


def test_mixed_rows_split() -> None:
    result = orders_ods_contract().validate(
        [_order_row(order_id="OK1"), _order_row(order_amount_cents=-5), _order_row(order_id="OK2")]
    )
    assert len(result.good_rows) == 2
    assert result.bad_count == 1


def test_items_contract_rejects_zero_quantity() -> None:
    row = {
        "order_id": "MOCK1",
        "shop_id": 1,
        "item_id": "101",
        "platform": "mock",
        "product_id": "P1",
        "product_name": "x",
        "quantity": 0,
        "price_cents": 100,
        "raw_json": "{}",
        "updated_at": datetime(2026, 9, 1, 10, 0, 0),
        "etl_batch_id": "b1",
    }
    result = order_items_ods_contract().validate([row])
    assert result.bad_count == 1
