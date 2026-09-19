"""DwBuildOperator: 数仓构建任务(ODS → DWD → DWS → ADS,幂等)。

窗口语义与 scripts/build_dw.py 一致: 回填按 data_interval,常规重建最近 N 天;
每次构建写 etl_task_run。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk.bases.operator import BaseOperator
from etl_sdk.alerts.base import AlertManager
from etl_sdk.config import Settings, get_settings
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.runtime import build_alert_manager_from, resolve_build_since

DEFAULT_SHOP_YAML = Path("/opt/airflow/dags/config/shops.yaml")  # Airflow 容器内挂载路径


class DwBuildOperator(BaseOperator):
    """全量构建注册表内所有店铺(单任务,店铺循环在 SDK 内;店铺数少时比 expand 更易回填)。"""

    template_fields = ("since", "shop_yaml")

    def __init__(
        self,
        *,
        since: str | None = None,
        shop_yaml: str = str(DEFAULT_SHOP_YAML),
        default_days: int = 3,
        etl_meta_conn_id: str = "etl_meta",
        alert_manager: AlertManager | None = None,
        settings: Settings | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.since = since
        self.shop_yaml = shop_yaml
        self.default_days = default_days
        self.etl_meta_conn_id = etl_meta_conn_id
        self._alert_manager = alert_manager
        self._settings = settings

    def execute(self, context: Any) -> dict[str, Any]:
        from etl_sdk.airflow.hooks.etl_meta import EtlMetaHook
        from etl_sdk.dw.build import load_shop_registry, run_build

        settings = self._settings or get_settings()
        engine = EtlMetaHook(self.etl_meta_conn_id).get_engine()
        dag_run = context["dag_run"]
        now = datetime.now().replace(microsecond=0)
        since = resolve_build_since(
            dag_run.run_type,
            context.get("data_interval_start"),
            now,
            default_days=self.default_days,
        )
        if self.since:  # 显式模板参数优先(如手动指定回填窗口)
            since = datetime.fromisoformat(self.since).date()
        registry = load_shop_registry(Path(self.shop_yaml))

        started = datetime.now()
        try:
            summary = run_build(
                engine,
                registry,
                since=since,
                now=now,
                alert_manager=self._alert_manager or build_alert_manager_from(settings),
            )
            BatchRecorder(engine).record_task_run(
                dag_id=dag_run.dag_id,
                task_id=self.task_id,
                status="success",
                data_interval_start=context.get("data_interval_start"),
                data_interval_end=context.get("data_interval_end"),
                duration_ms=int((datetime.now() - started).total_seconds() * 1000),
            )
            return {
                "since": since.isoformat(),
                "shops": len(summary.results),
                "orders_read": summary.orders_read,
                "shop_versions_added": summary.shop_versions_added,
            }
        except Exception as exc:  # noqa: BLE001 —— 记账后重抛,交给 Airflow retries
            BatchRecorder(engine).record_task_run(
                dag_id=dag_run.dag_id,
                task_id=self.task_id,
                status="failed",
                data_interval_start=context.get("data_interval_start"),
                data_interval_end=context.get("data_interval_end"),
                error=str(exc),
                duration_ms=int((datetime.now() - started).total_seconds() * 1000),
            )
            raise
        finally:
            engine.dispose()
