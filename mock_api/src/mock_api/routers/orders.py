"""订单列表接口: 按 updated_at 时间窗 + 双分页模式(页码/游标),鉴权依赖 verify_token。"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from mock_api.auth import verify_token
from mock_api.datagen import (
    default_orders_per_day,
    default_seed_base,
    gen_orders_between,
)

router = APIRouter(prefix="/orders", tags=["orders"], dependencies=[Depends(verify_token)])


@router.get("")
def list_orders(
    updated_start: Annotated[datetime, Query(description="增量窗口起点(含)")],
    updated_end: Annotated[datetime, Query(description="增量窗口终点(不含)")],
    page_no: Annotated[int | None, Query(ge=1)] = None,
    page_cursor: Annotated[str | None, Query(description="游标(空串为起点)")] = None,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    rows = gen_orders_between(
        updated_start,
        updated_end,
        shop_id=1,
        seed_base=default_seed_base(),
        orders_per_day=default_orders_per_day(),
    )
    total = len(rows)

    if page_cursor is not None:
        # 游标模式: 游标为已返回条数的字符串形式(模拟平台不透明游标)
        start = int(page_cursor) if page_cursor else 0
        page = rows[start : start + page_size]
        next_start = start + len(page)
        return {
            "total": total,
            "page_size": page_size,
            "next_cursor": str(next_start) if next_start < total else "",
            "orders": page,
        }

    page_no = page_no or 1
    start = (page_no - 1) * page_size
    return {
        "total": total,
        "page_no": page_no,
        "page_size": page_size,
        "orders": rows[start : start + page_size],
    }
