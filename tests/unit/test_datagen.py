"""模拟数据生成确定性测试。"""

from __future__ import annotations

from datetime import date, datetime

from mock_api.datagen import gen_orders_between, gen_orders_for_day


def test_same_day_twice_identical() -> None:
    d = date(2026, 9, 1)
    a = gen_orders_for_day(d, shop_id=1, orders_per_day=50, seed_base="mock")
    b = gen_orders_for_day(d, shop_id=1, orders_per_day=50, seed_base="mock")
    assert a == b
    assert len(a) == 50


def test_different_days_differ() -> None:
    a = gen_orders_for_day(date(2026, 9, 1), orders_per_day=50, seed_base="mock")
    b = gen_orders_for_day(date(2026, 9, 2), orders_per_day=50, seed_base="mock")
    assert a != b


def test_window_filter_and_sort() -> None:
    rows = gen_orders_between(
        datetime(2026, 9, 1, 12, 0, 0),
        datetime(2026, 9, 1, 18, 0, 0),
        shop_id=1,
        seed_base="mock",
        orders_per_day=200,
    )
    assert rows, "窗口内应有数据"
    assert all(
        datetime(2026, 9, 1, 12, 0, 0) <= datetime.fromisoformat(o["updated_at"]) < datetime(2026, 9, 1, 18, 0, 0)
        for o in rows
    )
    updateds = [o["updated_at"] for o in rows]
    assert updateds == sorted(updateds), "应按 updated_at 升序返回(供断点续传使用)"


def test_window_result_cached() -> None:
    """同窗口翻页多次调用返回同一缓存对象(M6 修复: 分页不再每页重算全窗口)。"""
    args = (datetime(2026, 9, 3, 0, 0), datetime(2026, 9, 5, 0, 0))
    a = gen_orders_between(*args, shop_id=1, seed_base="mock", orders_per_day=100)
    b = gen_orders_between(*args, shop_id=1, seed_base="mock", orders_per_day=100)
    assert a is b  # 缓存命中(同对象),非重新生成
    assert a == b and len(a) > 0
