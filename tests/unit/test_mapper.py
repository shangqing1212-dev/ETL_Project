"""订单行映射单元测试(元转分、raw_json、明细展平)。"""

from __future__ import annotations

import json

import pytest
from etl_sdk.mappers.orders import order_items_to_ods, order_to_ods, yuan_to_cents


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12.34", 1234),
        ("0.05", 5),
        (12.34, 1234),
        ("100", 10000),
        (0, 0),
    ],
)
def test_yuan_to_cents(value, expected: int) -> None:
    assert yuan_to_cents(value) == expected


def test_yuan_to_cents_invalid_raises() -> None:
    with pytest.raises(ValueError, match="invalid amount"):
        yuan_to_cents("abc")


def test_order_to_ods_fields() -> None:
    row = {
        "order_id": "MOCK001",
        "shop_id": 1,
        "status": "paid",
        "buyer_nick": "tester",
        "order_amount": "10.50",
        "payment_amount": "10.50",
        "refund_amount": "0.00",
        "created_at": "2026-09-01T08:00:00",
        "updated_at": "2026-09-01T09:00:00",
        "items": [{"item_id": "i1"}],
    }
    ods = order_to_ods(row, batch_id="b1", platform="mock")
    assert ods["order_amount_cents"] == 1050
    assert ods["refund_amount_cents"] == 0
    assert ods["is_deleted"] == 0
    assert ods["etl_batch_id"] == "b1"
    assert ods["platform"] == "mock"
    assert json.loads(ods["raw_json"])["order_id"] == "MOCK001"  # 原始报文可还原


def test_order_items_flatten_inherits_order_ts() -> None:
    row = {
        "order_id": "O1",
        "shop_id": 1,
        "updated_at": "2026-09-01T09:00:00",
        "items": [
            {"item_id": "i1", "product_id": "P1", "product_name": "A", "quantity": 2, "price": "3.50"},
            {"item_id": "i2", "product_id": "P2", "product_name": "B", "quantity": 1, "price": "0.10"},
        ],
    }
    items = order_items_to_ods(row, batch_id="b1", platform="mock")
    assert len(items) == 2
    assert items[0]["price_cents"] == 350
    assert items[1]["price_cents"] == 10
    assert all(i["updated_at"].isoformat() == "2026-09-01T09:00:00" for i in items)
