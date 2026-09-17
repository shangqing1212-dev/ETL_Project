"""坏行死信: 映射/契约失败的原始行落 etl_meta.etl_load_error,不中断整批。

语义: 死信是"该行已记录在案",批照常推进;死信率超阈值时由抽取编排层
中止批次(见 extractors.base),阈值语义属于策略,不属于记录器。
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from structlog import get_logger

logger = get_logger(__name__)

DEAD_LETTER_CHUNK = 500


class DeadLetterLimitExceeded(Exception):
    """死信率超阈值: 由抽取编排层中止批次(水位不推进,触发 error 告警)。"""


@dataclass(frozen=True)
class DeadLetterRow:
    """一条死信记录: 原始行 + 错误分类 + 消息。"""

    raw_row: Mapping[str, Any] | None
    error_type: str
    error_msg: str


class DeadLetterRecorder:
    """批量写 etl_load_error(raw_row 序列化为 JSON,非 JSON 安全对象则丢原始值)。"""

    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine

    def record_many(
        self,
        *,
        batch_id: str,
        table_name: str,
        rows: Iterable[DeadLetterRow],
    ) -> int:
        """写入一批死信,返回条数。单行不可序列化时降级为空 raw_row,不抛异常。"""
        total = 0
        buffer: list[dict[str, Any]] = []
        with self.engine.connect() as conn:
            for row in rows:
                buffer.append(
                    {
                        "batch_id": batch_id,
                        "table_name": table_name,
                        "raw_row": _safe_dumps(row.raw_row),
                        "error_type": row.error_type,
                        "error_msg": row.error_msg[:2000],  # 防超长消息撑爆 TEXT 行
                    }
                )
                if len(buffer) >= DEAD_LETTER_CHUNK:
                    total += self._flush(conn, buffer)
                    buffer = []
            if buffer:
                total += self._flush(conn, buffer)
        logger.info(
            "dead_letter.recorded",
            table=table_name,
            batch_id=batch_id,
            count=total,
        )
        return total

    @staticmethod
    def _flush(conn: sa.engine.Connection, buffer: list[dict[str, Any]]) -> int:
        with conn.begin():
            result = conn.execute(
                sa.text(
                    "INSERT INTO etl_meta.etl_load_error "
                    "(batch_id, table_name, raw_row, error_type, error_msg) "
                    "VALUES (:batch_id, :table_name, :raw_row, :error_type, :error_msg)"
                ),
                buffer,
            )
        return result.rowcount or 0


def _safe_dumps(raw: Mapping[str, Any] | None) -> str | None:
    if raw is None:
        return None
    try:
        return json.dumps(raw, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return json.dumps({"unserializable": repr(raw)[:1000]}, ensure_ascii=False)
