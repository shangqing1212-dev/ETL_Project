"""dws_daily: 数仓构建(ODS → DWD → DWS → ADS,幂等;注册表内全店铺单任务)。

- 调度: 每日 02:30;重建最近 3 天窗口(覆盖当日凌晨前的迟到更新)
- 回填时窗口=data_interval(如回填 7 天,每天重建当天起 3 天窗口?否 —— 回填起点=data_interval_start)
- 幂等语义见 etl_sdk/dw/build.py(upsert 重算 / insert-overwrite / SCD2 no-op)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk.definitions.dag import dag
from etl_sdk.airflow.operators.dw_build import DwBuildOperator

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
}

SHOP_YAML = str(Path(__file__).parent / "config" / "shops.yaml")


@dag(
    dag_id="dws_daily",
    schedule="30 2 * * *",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    tags=["dw", "dws", "ads"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def dws_daily() -> None:
    DwBuildOperator(
        task_id="build_dw",
        shop_yaml=SHOP_YAML,
        default_days=3,
    )


dws_daily()
