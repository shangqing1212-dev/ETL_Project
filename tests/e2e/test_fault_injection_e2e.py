"""故障注入 E2E: 真实 mock API(进程内)+ 真实 MySQL + 真实 SDK 重试链路。

矩阵(每模式断言最终成功且行集与无故障基线一致):
- http_500:      5xx 由 tenacity 指数退避重试吸收
- rate_limit_429:429 读 Retry-After,限流桶扣透支,重试吸收
- timeout:       响应超时(客户端超时 < 注入延迟)重试吸收
- malformed_json:坏 JSON 按瞬态重试(httpx.DecodingError ∈ TransportError 白名单)
- cursor_reset:  游标重置回起点 → 翻页重复,批内去重 + upsert 幂等吸收,不重不漏
"""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from etl_sdk.adapters.mock import MockPlatformAdapter
from etl_sdk.config import Settings
from etl_sdk.extractors.base import TimeWindow
from etl_sdk.extractors.pagination import CursorPaginator, Paginator
from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket
from etl_sdk.runtime import build_extractor

WINDOW = TimeWindow(start=datetime(2026, 9, 15), end=datetime(2026, 9, 16))  # 1 天 = 500 单,page_size 100 → 5 页
NOW = datetime(2026, 9, 17, 10, 0)

# 客户端超时 1s;timeout 注入延迟 3s(> 客户端超时)以保证触发 ReadTimeout
CLIENT_TIMEOUT = 1.0


def _make_extractor(
    engine: sa.Engine,
    base_url: str,
    *,
    paginator: Paginator | None = None,
) -> object:
    adapter = MockPlatformAdapter(
        base_url=base_url,
        client_id="test_client",
        client_secret="test_secret",
        timeout_seconds=CLIENT_TIMEOUT,
        rate_limiter=AdaptiveRateLimiter(TokenBucket(rate_per_min=10000)),
    )
    settings = Settings(_env_file=None)
    return build_extractor(
        engine,
        adapter,
        settings,
        shop_id=1,
        platform="mock",
        paginator=paginator,
        alert_manager=None,
    )


def _rows(engine: sa.Engine) -> list[tuple[str, str]]:
    """ODS 行集校验和(业务全列,排除 etl_batch_id/etl_loaded_at 两个装载痕迹列)。"""
    with engine.connect() as conn:
        return [
            tuple(str(v) for v in r)
            for r in conn.execute(
                sa.text(
                    "SELECT order_id, order_status, buyer_nick, order_amount_cents, payment_amount_cents, "
                    "refund_amount_cents, is_deleted, created_at, updated_at "
                    "FROM dw.ods_orders ORDER BY order_id"
                )
            )
        ]


def _batch_status(engine: sa.Engine) -> str:
    with engine.connect() as conn:
        return conn.execute(
            sa.text("SELECT status FROM etl_meta.etl_batch ORDER BY started_at DESC LIMIT 1")
        ).scalar_one()


def test_e2e_fault_matrix(mysql_engine: sa.Engine, clean_state, mock_base_url: str, monkeypatch) -> None:
    """各故障模式下全量抽取最终成功,行集与无故障基线一致。"""
    from mock_api import faults

    extractor = _make_extractor(mysql_engine, mock_base_url)
    result = extractor.extract(run_type="full", window_override=WINDOW)
    assert result.rows_read >= 390, f"无故障基线应读到 400+ 单,实际 {result.rows_read}"
    baseline = _rows(mysql_engine)
    assert len(baseline) >= 390
    assert _batch_status(mysql_engine) == "success"

    # 每模式: 清表重跑,断言行集与基线一致(证明故障被吸收且不重不漏)
    for mode in ("http_500", "rate_limit_429", "malformed_json", "cursor_reset", "timeout"):
        faults.set_fault(mode)
        monkeypatch.setattr(faults, "FAULT_TIMEOUT_SECONDS", 3.0)
        try:
            with mysql_engine.begin() as conn:
                for table in ("dw.ods_order_items", "dw.ods_orders", "etl_meta.etl_batch"):
                    conn.execute(sa.text(f"DELETE FROM {table}"))
            result = extractor.extract(run_type="full", window_override=WINDOW)
            assert _batch_status(mysql_engine) == "success", f"{mode} 模式批次未成功"
            assert result.rows_read >= 390, f"{mode} 模式读到 {result.rows_read} 行(少于基线 398 漏读)"
            assert _rows(mysql_engine) == baseline, f"{mode} 模式行集与基线不一致"
        finally:
            faults.set_fault("none")


def test_e2e_cursor_reset_no_dup(mysql_engine: sa.Engine, clean_state, mock_base_url: str) -> None:
    """cursor_reset 走游标分页: 翻页重复被批内去重吸收,行集精确等于基线。"""
    from mock_api import faults

    baseline_extractor = _make_extractor(mysql_engine, mock_base_url)
    baseline_extractor.extract(run_type="full", window_override=WINDOW)
    baseline = _rows(mysql_engine)

    with mysql_engine.begin() as conn:
        for table in ("dw.ods_order_items", "dw.ods_orders", "etl_meta.etl_batch"):
            conn.execute(sa.text(f"DELETE FROM {table}"))

    faults.set_fault("cursor_reset")
    try:
        cursor_extractor = _make_extractor(mysql_engine, mock_base_url, paginator=CursorPaginator(page_size=100))
        result = cursor_extractor.extract(run_type="full", window_override=WINDOW)
        assert result.rows_read >= 390  # 游标重置会读到重复页,总数可大于 500
        assert _rows(mysql_engine) == baseline, "去重后行集应精确等于基线"
    finally:
        faults.set_fault("none")


def test_e2e_incremental_after_fault(mysql_engine: sa.Engine, clean_state, mock_base_url: str) -> None:
    """故障后再跑增量(同窗口): 幂等 upsert,行集不变,水位推进。"""
    from mock_api import faults

    extractor = _make_extractor(mysql_engine, mock_base_url)
    first = extractor.extract(run_type="full", window_override=WINDOW)
    assert first.rows_read >= 390
    baseline = _rows(mysql_engine)

    # 水位推进到窗口终点后,增量 run 同窗口(overlap 重叠)应重读并幂等更新
    second = extractor.extract(run_type="incremental", window_override=WINDOW)
    assert second.rows_read >= 390
    assert _rows(mysql_engine) == baseline
    assert _batch_status(mysql_engine) == "success"

    # 故障注入下增量重跑同样收敛
    faults.set_fault("http_500")
    try:
        third = extractor.extract(run_type="incremental", window_override=WINDOW)
        assert _rows(mysql_engine) == baseline
        assert third.rows_read >= 390
    finally:
        faults.set_fault("none")
