"""EtlTableOperator: 一个店铺实体的增量抽取任务(薄装配层,业务逻辑全部在 etl_sdk)。

- 读取 dag_run.run_type: backfill 时窗口= data_interval(不推进水位),否则按水位增量
- 每轮抽取写 etl_task_run(dag_id/task_id/data_interval/耗时),独立于 Airflow 内部表
- 失败重抛 → Airflow retries 接管;etl_batch 已记录 failed + error 告警(抽取器内部)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk.bases.operator import BaseOperator
from etl_sdk.alerts.base import AlertManager
from etl_sdk.config import Settings, get_settings
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.runtime import (
    build_alert_manager_from,
    build_extractor,
    build_mock_adapter,
    normalize_window,
    resolve_extract_window,
)


class EtlTableOperator(BaseOperator):
    """按店铺抽取订单实体(主表+子表同批,水位两阶段提交)。"""

    template_fields = ("run_type", "window_start", "window_end", "shop_id", "platform")

    def __init__(
        self,
        *,
        shop_id: int,
        platform: str,
        etl_meta_conn_id: str = "etl_meta",
        api_conn_id: str = "mock_api",
        alert_manager: AlertManager | None = None,
        settings: Settings | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.shop_id = shop_id
        self.platform = platform
        self.etl_meta_conn_id = etl_meta_conn_id
        self.api_conn_id = api_conn_id
        self._alert_manager = alert_manager
        self._settings = settings
        # 模板字段占位: Airflow 3 校验 template_fields 必须真实存在;
        # 实际窗口在 execute 内按 dag_run 解析,渲染值仅供 UI/日志展示
        self.run_type = ""
        self.window_start: datetime | None = None
        self.window_end: datetime | None = None

    def execute(self, context: Any) -> dict[str, Any]:
        from airflow.sdk.bases.hook import BaseHook
        from etl_sdk.airflow.hooks.etl_meta import EtlMetaHook

        settings = self._settings or get_settings()
        dag_run = context["dag_run"]
        run_type, window = resolve_extract_window(
            dag_run.run_type,
            context.get("data_interval_start"),
            context.get("data_interval_end"),
            overlap=timedelta(minutes=settings.extraction.overlap_minutes),
        )
        if window is not None:
            window = normalize_window(window)

        api_conn = BaseHook.get_connection(self.api_conn_id)
        base_url = api_conn.get_extra_dejson().get("base_url") or f"http://{api_conn.host}:{api_conn.port}"
        adapter = build_mock_adapter(settings, base_url=base_url)
        engine = EtlMetaHook(self.etl_meta_conn_id).get_engine()
        extractor = build_extractor(
            engine,
            adapter,
            settings,
            shop_id=int(self.shop_id),
            platform=str(self.platform),
            alert_manager=self._alert_manager or build_alert_manager_from(settings),
        )

        started = datetime.now()
        try:
            result = extractor.extract(run_type=run_type, window_override=window)
            BatchRecorder(engine).record_task_run(
                dag_id=dag_run.dag_id,
                task_id=self.task_id,
                status="success",
                data_interval_start=context.get("data_interval_start"),
                data_interval_end=context.get("data_interval_end"),
                duration_ms=int((datetime.now() - started).total_seconds() * 1000),
            )
            return {
                "batch_id": result.batch_id,
                "rows_read": result.rows_read,
                "rows_written": result.rows_written,
                "dead_letters": result.dead_letters,
                "watermark_advanced": result.watermark_advanced,
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
