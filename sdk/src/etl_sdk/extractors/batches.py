"""批次与任务运行记录: 写 etl_meta.etl_batch / etl_meta.etl_task_run(血缘与排障中枢)。"""

from __future__ import annotations

import uuid
from datetime import datetime

import sqlalchemy as sa
from structlog import get_logger

logger = get_logger(__name__)


class BatchRecorder:
    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine

    def start_batch(
        self,
        *,
        table_name: str,
        shop_id: int,
        platform: str,
        run_type: str,
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> str:
        batch_id = uuid.uuid4().hex
        with self.engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO etl_meta.etl_batch "
                    "(batch_id, table_name, shop_id, platform, run_type, window_start, window_end, status) "
                    "VALUES (:b, :t, :s, :p, :rt, :ws, :we, 'running')"
                ),
                {
                    "b": batch_id,
                    "t": table_name,
                    "s": shop_id,
                    "p": platform,
                    "rt": run_type,
                    "ws": window_start,
                    "we": window_end,
                },
            )
        logger.info(
            "batch.start",
            batch_id=batch_id,
            table=table_name,
            shop_id=shop_id,
            run_type=run_type,
            window_start=window_start.isoformat() if window_start else None,
            window_end=window_end.isoformat() if window_end else None,
        )
        return batch_id

    def finish_batch(
        self,
        batch_id: str,
        *,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        error_msg: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.text(
                    "UPDATE etl_meta.etl_batch "
                    "SET status = :st, rows_read = :rr, rows_written = :rw, error_msg = :em, ended_at = NOW(3) "
                    "WHERE batch_id = :b"
                ),
                {"st": status, "rr": rows_read, "rw": rows_written, "em": error_msg, "b": batch_id},
            )
        logger.info(
            "batch.finish",
            batch_id=batch_id,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
        )

    def record_task_run(
        self,
        *,
        dag_id: str,
        task_id: str,
        status: str,
        data_interval_start: datetime | None = None,
        data_interval_end: datetime | None = None,
        error: str | None = None,
        duration_ms: int | None = None,
    ) -> str:
        """记录 SDK 层任务运行(独立于 Airflow 内部表;M5 起由 Operator 调用)。"""
        run_id = uuid.uuid4().hex
        with self.engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO etl_meta.etl_task_run "
                    "(run_id, dag_id, task_id, data_interval_start, data_interval_end, "
                    "status, error, duration_ms, ended_at) "
                    "VALUES (:r, :d, :t, :ds, :de, :st, :e, :dm, NOW(3))"
                ),
                {
                    "r": run_id,
                    "d": dag_id,
                    "t": task_id,
                    "ds": data_interval_start,
                    "de": data_interval_end,
                    "st": status,
                    "e": error,
                    "dm": duration_ms,
                },
            )
        return run_id
