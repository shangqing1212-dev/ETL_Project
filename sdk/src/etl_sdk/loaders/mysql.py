"""MySQL 批量装载器: 分批幂等 upsert(ODS/DWD 通用)。

设计要点:
- MySQL 8.0.19+ 行别名语法(INSERT ... AS new ON DUPLICATE KEY UPDATE),避开已废弃的 VALUES()
- 单调列(updated_at)只升不降: 防迟到旧数据覆盖新数据(重叠窗口 + 回填场景)
- 2000 行/块 executemany,块级事务提交 —— 失败重跑无需大事务回滚,幂等由 upsert 语义保证
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from structlog import get_logger

logger = get_logger(__name__)

DEFAULT_CHUNK_SIZE = 2000


def _quote(name: str) -> str:
    """按 . 分段加反引号(schema.table -> `schema`.`table`)。"""
    return ".".join(f"`{part}`" for part in name.split("."))


def build_upsert_sql(
    table: str,
    columns: Sequence[str],
    pk_columns: Sequence[str],
    monotonic_col: str | None = "updated_at",
) -> str:
    """生成幂等 upsert SQL(行别名语法)。

    - 主键列不更新
    - 指定 monotonic_col 时,所有非主键列的新值都受其新旧比较门控:
      新行单调列更旧(迟到旧数据)则整行不覆盖 —— 防止旧状态回退
    - 行别名(new)引用新行;旧行引用必须带表名限定,否则 MySQL 判为歧义列
    """
    pk_set = set(pk_columns)
    existing_table = _quote(table)
    updates = []
    for c in columns:
        if c in pk_set:
            continue
        existing = f"{existing_table}.`{c}`"
        if monotonic_col is not None:
            guard = f"new.`{monotonic_col}` > {existing_table}.`{monotonic_col}`"
            updates.append(f"`{c}` = IF({guard}, new.`{c}`, {existing})")
        else:
            updates.append(f"`{c}` = new.`{c}`")
    if not updates:
        raise ValueError("upsert 至少需要一个非主键更新列")

    col_clause = ", ".join(f"`{c}`" for c in columns)
    # 冒号命名绑定: SQLAlchemy 按方言转译(pymysql → %(name)s),与 dict 参数 executemany 原生匹配
    val_clause = ", ".join(f":{c}" for c in columns)
    return (
        f"INSERT INTO {table} ({col_clause}) VALUES ({val_clause}) AS new ON DUPLICATE KEY UPDATE {', '.join(updates)}"
    )


class MySQLBatchLoader:
    """分批幂等 upsert 装载器。

    输入行以 dict 形式给出,缺列自动置 NULL;多余键被忽略。
    """

    def __init__(
        self,
        engine: sa.Engine,
        table: str,
        columns: Sequence[str],
        pk_columns: Sequence[str],
        monotonic_col: str | None = "updated_at",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.engine = engine
        self.table = table
        self.columns = list(columns)
        self.chunk_size = chunk_size
        self.sql = build_upsert_sql(table, self.columns, pk_columns, monotonic_col)

    def upsert(self, rows: Iterable[Mapping[str, Any]]) -> int:
        """分批写入,返回受影响行数(insert=1, update=2,executemany 语义下为 MySQL 计数)。"""
        total = 0
        buffer: list[dict[str, Any]] = []
        with self.engine.connect() as conn:
            for row in rows:
                buffer.append({c: row.get(c) for c in self.columns})
                if len(buffer) >= self.chunk_size:
                    total += self._flush(conn, buffer)
                    buffer = []
            if buffer:
                total += self._flush(conn, buffer)
        logger.info(
            "loader.upsert.done",
            table=self.table,
            chunks=(total and (total - 1) // self.chunk_size + 1) or 0,
        )
        return total

    def _flush(self, conn: Connection, buffer: Sequence[Mapping[str, Any]]) -> int:
        with conn.begin():
            result = conn.execute(sa.text(self.sql), list(buffer))
        return result.rowcount or 0
