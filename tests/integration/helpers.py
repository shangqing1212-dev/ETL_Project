"""集成测试共享构造: 固定行集适配器、订单行工厂、告警录制通道。

CannedAdapter 绕开 mock_api 的确定性数据生成,让测试可以精确注入坏行
(非法金额/缺字段/陈旧时间戳),用于死信与 DQ 的集成验证。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from etl_sdk.adapters.base import BasePlatformAdapter
from etl_sdk.alerts.base import AlertChannel

ORDERS_TABLE = "dw.ods_orders"
ITEMS_TABLE = "dw.ods_order_items"
ORDER_COLUMNS = [
    "shop_id",
    "order_id",
    "platform",
    "order_status",
    "buyer_nick",
    "order_amount_cents",
    "payment_amount_cents",
    "refund_amount_cents",
    "raw_json",
    "is_deleted",
    "created_at",
    "updated_at",
    "etl_batch_id",
]
ORDER_PK = ["shop_id", "order_id"]
ITEM_COLUMNS = [
    "shop_id",
    "order_id",
    "item_id",
    "platform",
    "product_id",
    "product_name",
    "quantity",
    "price_cents",
    "raw_json",
    "updated_at",
    "etl_batch_id",
]
ITEM_PK = ["shop_id", "order_id", "item_id"]


class CannedAdapter(BasePlatformAdapter):
    """固定行集适配器: 首页返回全部行,第二页为空(配合 PagePaginator)。"""

    platform = "canned"

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def fetch_orders_page(
        self, window_start: datetime, window_end: datetime, page_params: dict[str, Any]
    ) -> dict[str, Any]:
        page_no = int(page_params.get("page_no", 1))
        if page_no > 1:
            return {"orders": [], "page_no": page_no, "total": len(self._rows)}
        return {"orders": self._rows, "page_no": page_no, "total": len(self._rows)}


def make_order(i: int, *, updated: datetime | None = None, **overrides: Any) -> dict[str, Any]:
    """构造一条 mock 风格订单行;金额/字段可覆盖,用于制造坏行。"""
    ts = updated or datetime(2026, 9, 1, 10, 0, 0)
    row: dict[str, Any] = {
        "order_id": f"C{i:04d}",
        "shop_id": 1,
        "status": "paid",
        "buyer_nick": f"user{i}",
        "order_amount": "12.34",
        "payment_amount": "12.34",
        "refund_amount": "0.00",
        "created_at": ts.isoformat(),
        "updated_at": ts.isoformat(),
        "items": [
            {
                "item_id": f"{i}01",
                "product_id": f"P{i:03d}",
                "product_name": "商品",
                "quantity": 1,
                "price": "1.00",
            }
        ],
    }
    row.update(overrides)
    return row


class RecordingChannel(AlertChannel):
    """录制告警调用供断言。"""

    name = "recording"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def send(self, title: str, text: str, *, level: str) -> None:
        self.calls.append((title, text, level))
