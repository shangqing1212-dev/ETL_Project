"""增量抽取集成测试: mock_api 进程内(ASGITransport)+ 真实 MySQL。

验证 M2 验收:
- 增量跑两遍结果一致(幂等)
- 水位正确推进、失败不推进(两阶段提交)
- 断点续传不重不漏(故障注入 429/500/超时重试后成功;游标重置去重)
- 游标分页与页码分页结果一致
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

import httpx
import pytest
import sqlalchemy as sa
import uvicorn
from etl_sdk.adapters.mock import MockPlatformAdapter
from etl_sdk.extractors.base import BaseExtractor, EntitySpec, TimeWindow
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import CursorPaginator, PagePaginator
from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket
from etl_sdk.extractors.state import WatermarkState
from etl_sdk.loaders.mysql import MySQLBatchLoader
from etl_sdk.mappers.orders import order_items_to_ods, order_to_ods
from mock_api.datagen import gen_orders_between
from mock_api.faults import set_fault
from mock_api.main import create_app

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


@pytest.fixture()
def http_client():
    """mock_api 以真实 uvicorn 服务器跑在后台线程(端口随机),测试走真实 HTTP。

    选择真实服务器而非 ASGITransport: 同步 httpx.Client 不支持纯异步 transport,
    且真实网络栈让超时/断连类故障测试更贴近生产。
    """
    server = uvicorn.Server(uvicorn.Config(create_app(), host="127.0.0.1", port=0, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started, "mock_api 启动超时"
    port = server.servers[0].sockets[0].getsockname()[1]
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5.0)
    yield client
    client.close()
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture()
def extractor_factory(mysql_engine: sa.Engine, http_client: httpx.Client):
    """构造可复用的增量抽取器(可指定 paginator 与 now)。"""

    def factory(*, paginator, now: datetime) -> BaseExtractor:
        # 测试用高速限流,避免桶成为瓶颈;限流行为本身由 mock_api 与单元测试覆盖
        limiter = AdaptiveRateLimiter(TokenBucket(rate_per_min=6000, burst=6000))
        adapter = MockPlatformAdapter(
            base_url="http://mock",
            client_id="test_client",
            client_secret="test_secret",
            timeout_seconds=5.0,
            rate_limiter=limiter,
            client=http_client,
        )
        orders_spec = EntitySpec(
            table_name=ORDERS_TABLE,
            columns=ORDER_COLUMNS,
            pk_columns=ORDER_PK,
            mapper=lambda r, **kw: order_to_ods(r, **kw),
        )
        # 子实体规格: rows_mapper 返回子行列表(由 add_child 展平装载)
        items_spec = EntitySpec(
            table_name=ITEMS_TABLE,
            columns=ITEM_COLUMNS,
            pk_columns=ITEM_PK,
            rows_mapper=lambda r, **kw: order_items_to_ods(r, **kw),
        )
        extractor = BaseExtractor(
            adapter=adapter,
            paginator=paginator,
            entity=orders_spec,
            loader=MySQLBatchLoader(mysql_engine, ORDERS_TABLE, ORDER_COLUMNS, ORDER_PK),
            watermark=WatermarkState(mysql_engine),
            recorder=BatchRecorder(mysql_engine),
            shop_id=1,
            platform="mock",
            # 测试限定历史起点: mock API 每次翻页请求都会重生成窗口内全部数据,
            # 起点过远(如默认 2020)会让单页请求退化为百万行计算
            history_start=datetime(2026, 8, 25),
            now_fn=lambda: now,
        )
        extractor.add_child(
            items_spec,
            MySQLBatchLoader(mysql_engine, ITEMS_TABLE, ITEM_COLUMNS, ITEM_PK),
        )
        return extractor

    return factory


def _count(engine: sa.Engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar_one()


def _watermark(engine: sa.Engine, table: str) -> datetime | None:
    with engine.connect() as conn:
        return conn.execute(
            sa.text(
                "SELECT watermark_value FROM etl_meta.etl_watermark "
                "WHERE table_name = :t AND shop_id = 1 AND platform = 'mock'"
            ),
            {"t": table},
        ).scalar_one_or_none()


def _expected_orders(window_start: datetime, window_end: datetime) -> list[dict]:
    return gen_orders_between(window_start, window_end, shop_id=1, seed_base="mock", orders_per_day=500)


def test_incremental_two_runs_idempotent(clean_state: sa.Engine, extractor_factory) -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    extractor = extractor_factory(paginator=PagePaginator(page_size=100), now=now)

    # 首跑: 全量起步,窗口 [2020-01-01, 2026-09-01 23:55)
    result1 = extractor.extract(run_type="incremental")
    expected1 = _expected_orders(result1.window.start, result1.window.end)
    assert result1.rows_read == len(expected1)
    assert _count(clean_state, ORDERS_TABLE) == len(expected1)
    assert result1.watermark_advanced is True
    assert _watermark(clean_state, ORDERS_TABLE) == result1.window.end
    assert _count(clean_state, ITEMS_TABLE) == sum(len(o["items"]) for o in expected1)

    # 同窗口重跑: 只重读 overlap 部分,upsert 幂等,行数与水位不变
    result2 = extractor.extract(run_type="incremental")
    assert result2.rows_read > 0  # overlap 窗口内确有已装载数据
    assert _count(clean_state, ORDERS_TABLE) == len(expected1)
    assert result2.watermark_advanced is False

    # 时钟前进: 捞到跨天迟到更新与次日新增
    extractor3 = extractor_factory(paginator=PagePaginator(page_size=100), now=datetime(2026, 9, 2, 1, 0, 0))
    result3 = extractor3.extract(run_type="incremental")
    expected3 = _expected_orders(datetime(2026, 8, 25), result3.window.end)
    assert _count(clean_state, ORDERS_TABLE) == len(expected3)
    assert result3.rows_read > 0


def test_malformed_json_recovers_via_retry(clean_state: sa.Engine, extractor_factory) -> None:
    """坏 JSON 按瞬态重试(M6 起: DecodingError ∈ RequestError 重试白名单),最终成功并推进水位。

    "失败不动水位"由 M3 死信阈值中止测试覆盖(DeadLetterLimitExceeded → failed + 水位不动)。
    """
    now = datetime(2026, 9, 2, 0, 0, 0)
    extractor = extractor_factory(paginator=PagePaginator(page_size=100), now=now)
    set_fault("malformed_json")
    try:
        result = extractor.extract(run_type="incremental")
    finally:
        set_fault("none")
    expected = _expected_orders(result.window.start, result.window.end)
    assert result.rows_read == len(expected)
    assert _count(clean_state, ORDERS_TABLE) == len(expected)
    assert _watermark(clean_state, ORDERS_TABLE) == result.window.end


def test_http_500_fault_recovers_via_retry(clean_state: sa.Engine, extractor_factory) -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    extractor = extractor_factory(paginator=PagePaginator(page_size=100), now=now)
    set_fault("http_500")
    try:
        result = extractor.extract(run_type="incremental")
    finally:
        set_fault("none")
    expected = _expected_orders(result.window.start, result.window.end)
    assert result.rows_read == len(expected)
    assert _count(clean_state, ORDERS_TABLE) == len(expected)


def test_timeout_fault_recovers_via_retry(clean_state: sa.Engine, extractor_factory) -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    extractor = extractor_factory(paginator=PagePaginator(page_size=100), now=now)
    set_fault("timeout")
    try:
        result = extractor.extract(run_type="incremental")
    finally:
        set_fault("none")
    expected = _expected_orders(result.window.start, result.window.end)
    assert _count(clean_state, ORDERS_TABLE) == len(expected)


def test_cursor_pagination_matches_page_pagination(clean_state: sa.Engine, extractor_factory) -> None:
    now = datetime(2026, 9, 2, 0, 0, 0)
    window = TimeWindow(start=datetime(2026, 9, 1, 0, 0, 0), end=datetime(2026, 9, 1, 12, 0, 0))
    expected = _expected_orders(window.start, window.end)

    extractor = extractor_factory(paginator=CursorPaginator(page_size=100), now=now)
    result = extractor.extract(run_type="backfill", window_override=window)
    assert result.rows_read == len(expected)
    assert _count(clean_state, ORDERS_TABLE) == len(expected)


def test_cursor_reset_fault_stays_consistent(clean_state: sa.Engine, extractor_factory) -> None:
    """游标重置造成重复页 -> 批内去重 + upsert 幂等 -> 不重不漏。"""
    now = datetime(2026, 9, 2, 0, 0, 0)
    window = TimeWindow(start=datetime(2026, 9, 1, 0, 0, 0), end=datetime(2026, 9, 2, 0, 0, 0))
    expected = _expected_orders(window.start, window.end)
    assert len(expected) > 200, "窗口内数据量需超过两页才能触发游标重置路径"

    extractor = extractor_factory(paginator=CursorPaginator(page_size=100), now=now)
    set_fault("cursor_reset")
    try:
        result = extractor.extract(run_type="backfill", window_override=window)
    finally:
        set_fault("none")
    # 重复页会造成 rows_read > 期望,但装载后行数精确等于期望(去重+幂等)
    assert result.rows_read >= len(expected)
    assert _count(clean_state, ORDERS_TABLE) == len(expected)
