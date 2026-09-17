"""DQ 引擎集成测试: 规则同步幂等、六类规则检出、block 中止批次、warn 仅告警。

规则源为测试专用 YAML(与生产 sql/dq_rules.yaml 同机制,参数选可注入失败的形式);
生产规则文件本身由 tests/unit/test_dq_defs.py::test_production_rules_yaml_is_valid 校验。
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
import sqlalchemy as sa
from etl_sdk.alerts.base import AlertManager
from etl_sdk.dq.defs import load_rule_specs, sync_rules
from etl_sdk.dq.engine import DQBlockedError, DQEngine

from tests.integration.helpers import ITEMS_TABLE, ORDERS_TABLE, RecordingChannel, make_order

NOW = datetime(2026, 9, 2, 0, 0, 0)
WINDOW = (datetime(2026, 8, 25, 0, 0, 0), datetime(2026, 9, 1, 23, 55, 0))
# 窗口内但距 NOW 约 6.5 天: 超出 freshness 阈值(1440 分钟),模拟"源端数据停滞"
STALE = datetime(2026, 8, 26, 10, 0, 0)

TEST_RULES = """
- table_name: dw.ods_orders
  rule_type: null_rate
  params: {column: buyer_nick, max_rate: 0.0}
  severity: block
- table_name: dw.ods_orders
  rule_type: unique
  params: {columns: [order_id]}
  severity: block
- table_name: dw.ods_orders
  rule_type: range
  params: {column: order_amount_cents, min: 0, max: 500000}
  severity: warn
- table_name: dw.ods_orders
  rule_type: freshness
  params: {column: updated_at, max_age_minutes: 1440}
  severity: block
- table_name: dw.ods_orders
  rule_type: row_count_delta
  params: {max_deviation: 0.5, lookback_batches: 5}
  severity: warn
- table_name: dw.ods_order_items
  rule_type: referential
  params:
    child_column: order_id
    child_shop_column: shop_id
    parent_table: dw.ods_orders
    parent_column: order_id
    parent_shop_column: shop_id
  severity: block
"""


@pytest.fixture()
def dq_rules_synced(clean_state: sa.Engine, tmp_path):
    """同步测试规则进 dq_check_def(dq_check_def 不在 clean_state 清理范围,靠幂等复用)。"""
    p = tmp_path / "rules.yaml"
    p.write_text(TEST_RULES, encoding="utf-8")
    specs = load_rule_specs(p)
    sync_rules(clean_state, specs)
    return specs


def _run_dq(
    engine: sa.Engine,
    *,
    table: str,
    batch_id: str,
    rows_read: int = 0,
    now: datetime = NOW,
    window: tuple[datetime, datetime] = WINDOW,
):
    return DQEngine(engine).run(
        table_name=table,
        batch_id=batch_id,
        shop_id=1,
        platform="canned",
        run_type="incremental",
        rows_read=rows_read,
        now=now,
        window_start=window[0],
        window_end=window[1],
    )


def _outcome(report, rule_type: str):
    return next(o for o in report.outcomes if o.rule_type == rule_type)


def _batch_status(engine: sa.Engine) -> str:
    with engine.connect() as conn:
        return conn.execute(
            sa.text("SELECT status FROM etl_meta.etl_batch ORDER BY started_at DESC LIMIT 1")
        ).scalar_one()


def test_sync_rules_idempotent(dq_rules_synced, clean_state: sa.Engine) -> None:
    with clean_state.connect() as conn:
        first = conn.execute(sa.text("SELECT COUNT(*) FROM etl_meta.dq_check_def")).scalar_one()
    second = sync_rules(clean_state, dq_rules_synced)
    assert second == {"inserted": 0, "skipped": 6}
    with clean_state.connect() as conn:
        after = conn.execute(sa.text("SELECT COUNT(*) FROM etl_meta.dq_check_def")).scalar_one()
    assert after == first


def test_all_rules_pass_on_clean_batch(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    alert = RecordingChannel()
    result = extractor_factory(
        rows=[make_order(i) for i in range(5)], now=NOW, dq=True, alert_manager=AlertManager([alert])
    ).extract(run_type="incremental")

    assert result.watermark_advanced is True
    assert alert.calls == []
    with clean_state.connect() as conn:
        results = conn.execute(
            sa.text(
                "SELECT rule_type, passed FROM etl_meta.dq_check_result r "
                "JOIN etl_meta.dq_check_def d ON r.check_id = d.check_id WHERE r.batch_id = :b"
            ),
            {"b": result.batch_id},
        ).all()
    assert len(results) == 6  # 六类规则全部执行
    assert all(passed == 1 for _, passed in results)


def test_block_rule_stale_data_aborts_batch(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    alert = RecordingChannel()
    extractor = extractor_factory(
        rows=[make_order(i, updated=STALE) for i in range(5)],
        now=NOW,
        dq=True,
        alert_manager=AlertManager([alert]),
    )
    with pytest.raises(DQBlockedError, match="freshness"):
        extractor.extract(run_type="incremental")

    assert _batch_status(clean_state) == "failed"
    with clean_state.connect() as conn:
        watermark = conn.execute(
            sa.text(
                "SELECT COUNT(*) FROM etl_meta.etl_watermark "
                "WHERE table_name = :t AND shop_id = 1 AND platform = 'canned'"
            ),
            {"t": ORDERS_TABLE},
        ).scalar_one()
    assert watermark == 0  # block 失败不推进水位
    assert [lv for _, _, lv in alert.calls] == ["error"]


def test_warn_rule_violation_does_not_block(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    alert = RecordingChannel()
    rows = [make_order(i) for i in range(4)]
    rows.append(make_order(9, order_amount="-5.00"))  # 金额越界 -> range(warn) 失败

    result = extractor_factory(rows=rows, now=NOW, dq=True, alert_manager=AlertManager([alert])).extract(
        run_type="incremental"
    )

    assert result.watermark_advanced is True  # warn 不阻断
    assert [lv for _, _, lv in alert.calls] == ["warning"]
    assert "range" in alert.calls[0][1]


def test_unique_detects_cross_shop_duplicate(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    extractor_factory(rows=[make_order(1)], now=NOW).extract(run_type="incremental")
    # 同 order_id 出现在另一个 shop(PK 允许,但业务上订单号应全局唯一)
    with clean_state.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO dw.ods_orders (shop_id, order_id, platform, updated_at) "
                "VALUES (2, 'C0001', 'canned', NOW(3))"
            )
        )
    report = _run_dq(clean_state, table=ORDERS_TABLE, batch_id=uuid.uuid4().hex)
    assert _outcome(report, "unique").passed is False


def test_null_rate_detects_nulls(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    result = extractor_factory(rows=[make_order(1)], now=NOW).extract(run_type="incremental")
    with clean_state.begin() as conn:
        conn.execute(sa.text("UPDATE dw.ods_orders SET buyer_nick = NULL WHERE order_id = 'C0001'"))
    report = _run_dq(
        clean_state,
        table=ORDERS_TABLE,
        batch_id=result.batch_id,
        window=(result.window.start, result.window.end),
    )
    assert _outcome(report, "null_rate").passed is False


def test_referential_detects_orphans(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    extractor_factory(rows=[make_order(1), make_order(2)], now=NOW).extract(run_type="incremental")
    with clean_state.begin() as conn:
        conn.execute(sa.text("DELETE FROM dw.ods_orders WHERE order_id = 'C0002'"))  # 明细孤儿化
    report = _run_dq(clean_state, table=ITEMS_TABLE, batch_id=uuid.uuid4().hex)
    assert _outcome(report, "referential").passed is False


def test_row_count_delta_detects_anomaly(dq_rules_synced, clean_state: sa.Engine) -> None:
    # 造 3 批成功历史(rows_read=100),本批 300 行 -> 偏差 200% > 50% 阈值
    with clean_state.begin() as conn:
        for _ in range(3):
            conn.execute(
                sa.text(
                    "INSERT INTO etl_meta.etl_batch "
                    "(batch_id, table_name, shop_id, platform, run_type, status, rows_read) "
                    "VALUES (:b, :t, 1, 'canned', 'incremental', 'success', 100)"
                ),
                {"b": uuid.uuid4().hex, "t": ORDERS_TABLE},
            )
    report = _run_dq(clean_state, table=ORDERS_TABLE, batch_id=uuid.uuid4().hex, rows_read=300)
    outcome = _outcome(report, "row_count_delta")
    assert outcome.passed is False
    assert "偏差" in (outcome.detail or "")

    report_ok = _run_dq(clean_state, table=ORDERS_TABLE, batch_id=uuid.uuid4().hex, rows_read=100)
    assert _outcome(report_ok, "row_count_delta").passed is True


def test_rule_results_recorded_with_batch_link(dq_rules_synced, clean_state: sa.Engine, extractor_factory) -> None:
    result = extractor_factory(rows=[make_order(i) for i in range(3)], now=NOW, dq=True).extract(run_type="incremental")
    with clean_state.connect() as conn:
        rows = conn.execute(
            sa.text("SELECT actual_value, expected_value, passed FROM etl_meta.dq_check_result WHERE batch_id = :b"),
            {"b": result.batch_id},
        ).all()
    assert len(rows) == 6
    assert all(r[0] is not None and r[1] is not None for r in rows)  # 实际值/期望值都落库
