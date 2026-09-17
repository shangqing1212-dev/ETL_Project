"""API 实体模型(与 ODS 表 1:1 镜像)。金额以 Decimal 承载源端"元",入仓前统一转分(ADR-002)。"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    item_id: str
    product_id: str
    product_name: str = ""
    quantity: int = Field(ge=1)
    price: Decimal = Field(ge=0)  # 单位: 元(源端原样)


class Order(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: str
    shop_id: int
    status: str
    buyer_nick: str = ""
    order_amount: Decimal = Field(ge=0)  # 单位: 元
    payment_amount: Decimal = Field(ge=0)
    refund_amount: Decimal = Field(default=Decimal("0"), ge=0)
    created_at: datetime
    updated_at: datetime
    items: list[OrderItem] = Field(default_factory=list)
    is_deleted: bool = False
