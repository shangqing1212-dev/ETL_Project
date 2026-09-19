"""dq_monitor: 周期 DQ 扫描(守护"抽取管线健康",独立于抽取批次)。

- 对每店铺的 ODS 表跑最近 24h 窗口 DQ 规则;block 失败 → 任务失败 + error 告警
- 调度: 每日 08:00 / 20:00(与抽取错峰,避免和批次 DQ 重复告警)
- 结果落 dq_check_result(run_type=monitor),与抽取批次的结果同表可对照
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk.definitions.dag import dag
from etl_sdk.airflow.operators.dq_scan import DqScanOperator
from etl_sdk.dw.build import load_shop_registry

SHOPS = load_shop_registry(Path(__file__).parent / "config" / "shops.yaml")

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="dq_monitor",
    schedule="0 8,20 * * *",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    tags=["dq", "monitor"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def dq_monitor() -> None:
    DqScanOperator.partial(task_id="dq_scan", window_hours=24).expand_kwargs(
        [{"shop_id": shop["shop_id"], "platform": shop["platform"]} for shop in SHOPS]
    )


dq_monitor()
