"""etl_orders: 按店铺抽取订单(主表+子表同批,水位两阶段提交)。

- 解析期只读 shops.yaml(注册表),零 DB 访问(Airflow 3 硬约束)
- 调度: 每小时;回填(airflow dags backfill)时窗口=data_interval,不推进水位(ADR-003)
- 失败重试: 3 次,间隔 5 分钟;批次记账与告警由 SDK 抽取器完成
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk.definitions.dag import dag
from etl_sdk.airflow.operators.etl_table import EtlTableOperator
from etl_sdk.dw.build import load_shop_registry

SHOPS = load_shop_registry(Path(__file__).parent / "config" / "shops.yaml")

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="etl_orders",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    tags=["etl", "mock", "ods"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def etl_orders() -> None:
    EtlTableOperator.partial(task_id="extract_orders").expand_kwargs(
        [{"shop_id": shop["shop_id"], "platform": shop["platform"]} for shop in SHOPS]
    )


etl_orders()
