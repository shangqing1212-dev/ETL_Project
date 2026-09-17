"""API 订单行 -> ODS 行映射: 金额元转分(ADR-002)、raw_json 原样落库、明细展平。"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def yuan_to_cents(value: Any) -> int:
    """金额字符串/数值(元)-> 分。非法值抛 ValueError(装载层捕获后进死信表)。"""
    try:
        cents = (Decimal(str(value)) * 100).quantize(Decimal("1"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid amount: {value!r}") from exc
    return int(cents)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def order_to_ods(row: dict[str, Any], *, batch_id: str, platform: str) -> dict[str, Any]:
    return {
        "shop_id": int(row["shop_id"]),
        "order_id": str(row["order_id"]),
        "platform": platform,
        "order_status": str(row.get("status") or ""),
        "buyer_nick": row.get("buyer_nick") or "",
        "order_amount_cents": yuan_to_cents(row["order_amount"]),
        "payment_amount_cents": yuan_to_cents(row["payment_amount"]),
        "refund_amount_cents": yuan_to_cents(row.get("refund_amount") or 0),
        "raw_json": json.dumps(row, ensure_ascii=False),
        "is_deleted": 1 if row.get("is_deleted") else 0,
        "created_at": _dt(row["created_at"]),
        "updated_at": _dt(row["updated_at"]),
        "etl_batch_id": batch_id,
    }


def order_items_to_ods(row: dict[str, Any], *, batch_id: str, platform: str) -> list[dict[str, Any]]:
    """订单明细展平;明细没有独立时间戳,沿用所属订单的 updated_at(与 DDL 注释一致)。"""
    order_updated_at = _dt(row["updated_at"])
    return [
        {
            "shop_id": int(row["shop_id"]),
            "order_id": str(row["order_id"]),
            "item_id": str(item["item_id"]),
            "platform": platform,
            "product_id": str(item.get("product_id") or ""),
            "product_name": item.get("product_name") or "",
            "quantity": int(item.get("quantity") or 0),
            "price_cents": yuan_to_cents(item["price"]),
            "raw_json": json.dumps(item, ensure_ascii=False),
            "updated_at": order_updated_at,
            "etl_batch_id": batch_id,
        }
        for item in row.get("items") or []
    ]
