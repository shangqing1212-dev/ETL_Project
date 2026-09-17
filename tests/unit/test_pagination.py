"""分页策略单元测试。"""

from __future__ import annotations

from etl_sdk.extractors.pagination import CursorPaginator, PagePaginator


def test_page_paginator_stops_at_total() -> None:
    p = PagePaginator(page_size=100)
    assert p.initial_params() == {"page_no": 1, "page_size": 100}
    assert p.next_params({"page_no": 1, "total": 250}) == {"page_no": 2, "page_size": 100}
    assert p.next_params({"page_no": 2, "total": 250}) == {"page_no": 3, "page_size": 100}
    assert p.next_params({"page_no": 3, "total": 250}) is None  # 3*100 >= 250


def test_page_paginator_exact_boundary() -> None:
    p = PagePaginator(page_size=20)
    assert p.next_params({"page_no": 1, "total": 20}) is None


def test_cursor_paginator_stops_on_empty_cursor() -> None:
    p = CursorPaginator(page_size=100)
    assert p.initial_params() == {"page_cursor": "", "page_size": 100}
    assert p.next_params({"next_cursor": "100"}) == {"page_cursor": "100", "page_size": 100}
    assert p.next_params({"next_cursor": ""}) is None
    assert p.next_params({}) is None
