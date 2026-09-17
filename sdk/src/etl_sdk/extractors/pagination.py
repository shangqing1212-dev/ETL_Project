"""分页策略: 页码模式与游标模式,统一接口供适配器驱动翻页。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Paginator(ABC):
    """翻页策略接口。

    约定: 每页响应为 dict,含 "orders" 列表;页码模式另有 "total",游标模式另有 "next_cursor"。
    next_params 返回 None 表示翻页结束。
    """

    page_size: int = 100

    @abstractmethod
    def initial_params(self) -> dict[str, Any]:
        """第一页请求参数(除时间窗外)。"""

    @abstractmethod
    def next_params(self, page: dict[str, Any]) -> dict[str, Any] | None:
        """根据当前页响应计算下一页参数;无下一页返回 None。"""


class PagePaginator(Paginator):
    """页码分页: page_no 递增,依赖响应的 total 与每页实际条数判断结束。"""

    def __init__(self, page_size: int = 100) -> None:
        self.page_size = page_size

    def initial_params(self) -> dict[str, Any]:
        return {"page_no": 1, "page_size": self.page_size}

    def next_params(self, page: dict[str, Any]) -> dict[str, Any] | None:
        page_no = int(page["page_no"])
        total = int(page["total"])
        if page_no * self.page_size >= total:
            return None
        return {"page_no": page_no + 1, "page_size": self.page_size}


class CursorPaginator(Paginator):
    """游标分页: 透传响应中的 next_cursor;next_cursor 为空表示结束。"""

    def __init__(self, page_size: int = 100) -> None:
        self.page_size = page_size

    def initial_params(self) -> dict[str, Any]:
        return {"page_cursor": "", "page_size": self.page_size}

    def next_params(self, page: dict[str, Any]) -> dict[str, Any] | None:
        cursor = page.get("next_cursor")
        if not cursor:
            return None
        return {"page_cursor": cursor, "page_size": self.page_size}
