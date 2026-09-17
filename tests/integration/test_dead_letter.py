"""死信集成测试: 坏行落 etl_load_error 不中断整批;超阈值中止批次并告警。

与 M2 的 malformed_json 故障测试分工: 那里是"整页坏 JSON -> 批次失败"
(页面级故障,重试无益);这里是"个别坏行 -> 死信隔离"(行级故障,批照常推进)。
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
import sqlalchemy as sa
from etl_sdk.alerts.base import AlertManager
from etl_sdk.loaders.dead_letter import DeadLetterLimitExceeded

from tests.integration.helpers import ORDERS_TABLE, RecordingChannel, make_order

NOW = datetime(2026, 9, 2, 0, 0, 0)


def _dead_letter_rows(engine: sa.Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text("SELECT table_name, error_type, error_msg, raw_row FROM etl_meta.etl_load_error ORDER BY error_id")
        )
        return [
            {
                "table_name": r[0],
                "error_type": r[1],
                "error_msg": r[2],
                "raw_row": json.loads(r[3]) if r[3] else None,
            }
            for r in rows
        ]


def _count(engine: sa.Engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar_one()


def test_mapper_error_goes_to_dead_letter_batch_continues(clean_state: sa.Engine, extractor_factory) -> None:
    rows = [make_order(i) for i in range(10)]
    rows.append(make_order(99, order_amount="abc"))  # 金额非法 -> 映射抛 ValueError
    broken = make_order(100)
    del broken["order_id"]  # 缺主键字段 -> 映射抛 KeyError
    rows.append(broken)

    result = extractor_factory(rows=rows, now=NOW).extract(run_type="incremental")

    # 3 条死信: 金额非法行(订单侧)、缺 order_id 行(订单侧 + 明细侧各一次)
    assert result.dead_letters == 3
    assert result.watermark_advanced is True  # 坏行不阻断批
    assert _count(clean_state, ORDERS_TABLE) == 10

    dead = _dead_letter_rows(clean_state)
    assert len(dead) == 3
    assert all(d["error_type"] == "mapper_error" for d in dead)
    assert any("invalid amount" in d["error_msg"] for d in dead)
    # 缺 order_id 的坏行已原样入死信(原始报文可据此排障)
    assert any(
        d["raw_row"] is not None and d["raw_row"].get("buyer_nick") == "user100" and "order_id" not in d["raw_row"]
        for d in dead
    )


def test_contract_violation_goes_to_dead_letter(clean_state: sa.Engine, extractor_factory) -> None:
    rows = [make_order(i) for i in range(2)]
    rows.append(make_order(3, order_amount="-5.00"))  # 映射通过(转分=-500),契约拦截(金额非负)

    result = extractor_factory(rows=rows, now=NOW, contract=True).extract(run_type="incremental")

    assert result.dead_letters == 1
    assert _count(clean_state, ORDERS_TABLE) == 2
    dead = _dead_letter_rows(clean_state)
    assert len(dead) == 1
    assert dead[0]["error_type"] == "schema_mismatch"
    assert "order_amount_cents" in dead[0]["error_msg"]
    assert dead[0]["raw_row"]["order_id"] == "C0003"


def test_dead_letter_limit_exceeded_aborts_batch(clean_state: sa.Engine, extractor_factory) -> None:
    rows = [make_order(i) for i in range(10)]
    rows.extend(make_order(100 + i, order_amount="abc") for i in range(3))
    alert = RecordingChannel()

    with pytest.raises(DeadLetterLimitExceeded, match="超过阈值"):
        extractor_factory(rows=rows, now=NOW, dead_letter_limit=0.1, alert_manager=AlertManager([alert])).extract(
            run_type="incremental"
        )

    # 超阈值即中止: 好行未装载(死信率检查在 upsert 之前),水位不推进,批次 failed + error 告警
    assert _count(clean_state, ORDERS_TABLE) == 0
    with clean_state.connect() as conn:
        status = conn.execute(
            sa.text("SELECT status FROM etl_meta.etl_batch ORDER BY started_at DESC LIMIT 1")
        ).scalar_one()
        watermark = conn.execute(
            sa.text(
                "SELECT COUNT(*) FROM etl_meta.etl_watermark "
                "WHERE table_name = :t AND shop_id = 1 AND platform = 'canned'"
            ),
            {"t": ORDERS_TABLE},
        ).scalar_one()
    assert status == "failed"
    assert watermark == 0
    assert [lv for _, _, lv in alert.calls] == ["error"]
