"""DqScanOperator: 周期 DQ 扫描(独立于抽取批次,守护"抽取没坏"这一假设)。

对注册表店铺的 ODS 表跑最近 N 小时窗口的 DQ 规则;block 失败 → 任务失败 + error 告警,
warn 失败 → 告警不阻断。结果照常落 dq_check_result。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from airflow.sdk.bases.operator import BaseOperator
from etl_sdk.alerts.base import AlertManager
from etl_sdk.config import Settings, get_settings
from etl_sdk.dq.engine import DQEngine
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.runtime import build_alert_manager_from

MONITOR_TABLES = ("dw.ods_orders", "dw.ods_order_items")


class DqScanOperator(BaseOperator):
    """按店铺扫描 ODS 表最近 window_hours 的 DQ 规则(monitor 批次)。"""

    template_fields = ("window_hours", "shop_id", "platform")

    def __init__(
        self,
        *,
        shop_id: int,
        platform: str,
        window_hours: int = 24,
        tables: tuple[str, ...] = MONITOR_TABLES,
        etl_meta_conn_id: str = "etl_meta",
        alert_manager: AlertManager | None = None,
        settings: Settings | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.shop_id = shop_id
        self.platform = platform
        self.window_hours = window_hours
        self.tables = tables
        self.etl_meta_conn_id = etl_meta_conn_id
        self._alert_manager = alert_manager
        self._settings = settings

    def execute(self, context: Any) -> dict[str, Any]:
        from etl_sdk.airflow.hooks.etl_meta import EtlMetaHook

        settings = self._settings or get_settings()
        engine = EtlMetaHook(self.etl_meta_conn_id).get_engine()
        alert_manager = self._alert_manager or build_alert_manager_from(settings)
        dag_run = context["dag_run"]
        now = datetime.now().replace(microsecond=0)
        window_start = now - timedelta(hours=int(self.window_hours))
        batch_id = uuid.uuid4().hex
        recorder = BatchRecorder(engine)
        dq = DQEngine(engine)

        started = datetime.now()
        blocked: list[str] = []
        warned: list[str] = []
        try:
            for table in self.tables:
                rows_read = self._count_window_rows(
                    engine, table, int(self.shop_id), str(self.platform), window_start, now
                )
                report = dq.run(
                    table_name=table,
                    batch_id=batch_id,
                    shop_id=int(self.shop_id),
                    platform=str(self.platform),
                    run_type="monitor",
                    rows_read=rows_read,
                    now=now,
                    window_start=window_start,
                    window_end=now,
                )
                block_failures = report.block_failures()
                warn_failures = report.warn_failures()
                if block_failures:
                    blocked.append(table)
                    alert_manager.send(
                        title=f"DQ 扫描 block: {table}",
                        text=f"shop={self.shop_id} platform={self.platform} 最近 {self.window_hours}h "
                        f"block 失败 {len(block_failures)} 条",
                        level="error",
                    )
                if warn_failures:
                    warned.append(table)
                    alert_manager.send(
                        title=f"DQ 扫描 warn: {table}",
                        text=f"shop={self.shop_id} platform={self.platform} 最近 {self.window_hours}h "
                        f"warn 失败 {len(warn_failures)} 条",
                        level="warning",
                    )

            status = "failed" if blocked else "success"
            if blocked:  # block 失败按任务失败处理(Airflow retries/告警链路接管)
                raise RuntimeError(f"DQ block: {', '.join(blocked)}")
            recorder.record_task_run(
                dag_id=dag_run.dag_id,
                task_id=self.task_id,
                status=status,
                data_interval_start=context.get("data_interval_start"),
                data_interval_end=context.get("data_interval_end"),
                duration_ms=int((datetime.now() - started).total_seconds() * 1000),
            )
            return {"blocked": blocked, "warned": warned, "batch_id": batch_id}
        except Exception as exc:  # noqa: BLE001 —— 记账后重抛
            recorder.record_task_run(
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

    @staticmethod
    def _count_window_rows(
        engine: sa.Engine, table: str, shop_id: int, platform: str, window_start: datetime, window_end: datetime
    ) -> int:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM {table} "
                        "WHERE shop_id = :s AND platform = :p "
                        "AND updated_at >= :ws AND updated_at < :we"
                    ),
                    {"s": shop_id, "p": platform, "ws": window_start, "we": window_end},
                ).scalar_one()
            )
