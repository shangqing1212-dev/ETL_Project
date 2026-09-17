"""窗口计算与批内去重单元测试。"""

from __future__ import annotations

from datetime import datetime, timedelta

from etl_sdk.extractors.base import DEFAULT_HISTORY_START, compute_window, dedup_by_pk


def test_window_without_watermark_starts_at_history() -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    w = compute_window(None, now, overlap=timedelta(hours=1), delay=timedelta(minutes=5))
    assert w.start == DEFAULT_HISTORY_START
    assert w.end == datetime(2026, 9, 1, 23, 55)


def test_window_with_watermark_applies_overlap() -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    wm = datetime(2026, 9, 1, 10, 0, 0)
    w = compute_window(wm, now, overlap=timedelta(hours=1), delay=timedelta(minutes=5))
    assert w.start == datetime(2026, 9, 1, 9, 0)  # wm - 1h
    assert w.end == datetime(2026, 9, 1, 23, 55)


def test_dedup_keeps_newest_updated_at() -> None:
    rows = [
        {"order_id": "A", "status": "old", "updated_at": "2026-09-01T10:00:00"},
        {"order_id": "B", "status": "x", "updated_at": "2026-09-01T09:00:00"},
        {"order_id": "A", "status": "new", "updated_at": "2026-09-01T11:00:00"},
    ]
    out = dedup_by_pk(rows, ["order_id"])
    assert len(out) == 2
    by_id = {r["order_id"]: r for r in out}
    assert by_id["A"]["status"] == "new"
    assert by_id["B"]["status"] == "x"
