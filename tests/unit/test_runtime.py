"""运行时装配与窗口决策单测(纯函数,不依赖 Airflow)。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import sqlalchemy as sa
from etl_sdk.config import Settings
from etl_sdk.extractors.base import TimeWindow
from etl_sdk.runtime import (
    build_extractor,
    build_mock_adapter,
    normalize_window,
    resolve_build_since,
    resolve_extract_window,
)


def test_resolve_extract_window_backfill_uses_interval() -> None:
    start, end = datetime(2026, 9, 10, 8), datetime(2026, 9, 10, 9)
    run_type, window = resolve_extract_window("backfill", start, end)
    assert run_type == "backfill"
    assert window == TimeWindow(start=start, end=end)


def test_resolve_extract_window_backfill_point_interval() -> None:
    """Airflow 3.3 backfill 的 data_interval 是点(start==end)→ 窗口 = [t-1h, t)。"""
    t = datetime(2026, 9, 10, 8)
    run_type, window = resolve_extract_window("backfill", t, t)
    assert run_type == "backfill"
    assert window == TimeWindow(start=t - timedelta(hours=1), end=t)


def test_normalize_window_aware_to_local_naive() -> None:
    """UTC aware → Asia/Shanghai naive(ADR-001)。"""
    window = TimeWindow(
        start=datetime(2026, 9, 10, 3, 0, tzinfo=UTC),
        end=datetime(2026, 9, 10, 4, 0, tzinfo=UTC),
    )
    norm = normalize_window(window)
    assert norm == TimeWindow(start=datetime(2026, 9, 10, 11, 0), end=datetime(2026, 9, 10, 12, 0))
    assert norm.start.tzinfo is None


def test_resolve_extract_window_scheduled_is_incremental() -> None:
    # 调度/手动: 窗口交给水位,返回 None
    run_type, window = resolve_extract_window("scheduled", datetime(2026, 9, 10), datetime(2026, 9, 10, 1))
    assert run_type == "scheduled" and window is None


def test_resolve_extract_window_backfill_without_interval() -> None:
    # 防御: backfill 缺 data_interval 时退回普通模式(不伪造窗口)
    run_type, window = resolve_extract_window("backfill", None, None)
    assert run_type == "backfill" and window is None


def test_resolve_build_since() -> None:
    now = datetime(2026, 9, 17, 10, 0)
    assert resolve_build_since("backfill", datetime(2026, 9, 1, 8), now) == date(2026, 9, 1)
    assert resolve_build_since("scheduled", None, now) == date(2026, 9, 15)  # 最近 3 天: [9-15, 9-17]
    assert resolve_build_since("scheduled", None, now, default_days=7) == date(2026, 9, 11)


def test_build_mock_adapter_reads_settings() -> None:
    settings = Settings(_env_file=None)
    adapter = build_mock_adapter(settings, base_url="http://api:8080")
    assert str(adapter._client.base_url).rstrip("/") == "http://api:8080"  # noqa: SLF001
    assert adapter._client_id == "test_client"  # noqa: SLF001


def test_build_extractor_assembles(monkeypatch) -> None:
    """装配冒烟: 构造期不连库,用桩 engine 验证组件注入与店铺/平台透传。"""
    fake_engine = cast(sa.Engine, SimpleNamespace())
    adapter = build_mock_adapter(Settings(_env_file=None))
    extractor = build_extractor(
        fake_engine,
        adapter,
        Settings(_env_file=None),
        shop_id=7,
        platform="mock",
        alert_manager=None,
    )
    assert extractor._shop_id == 7  # noqa: SLF001 —— 装配结果断言
    assert extractor._platform == "mock"  # noqa: SLF001
    assert extractor._children_spec is not None  # noqa: SLF001 —— 子表(明细)已注册
    assert extractor._children_spec.table_name == "dw.ods_order_items"  # noqa: SLF001
