"""DDL 迁移执行器: 按文件名顺序执行 sql/ddl/*.sql,通过 etl_meta.schema_migrations 记录已应用版本。

用法:
    .venv/Scripts/python.exe scripts/migrate.py [mysql+pymysql://user:pass@host:port/?charset=utf8mb4]

约定(见 sql/ddl 文件头): 每条语句以独立一行的 ";" 结尾,文件内不出现过程体/触发器,
否则拆分逻辑会失效 —— 复杂语句请拆分到多个文件或改用独立迁移工具。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import sqlalchemy as sa

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_DIR = PROJECT_ROOT / "sql" / "ddl"

DEFAULT_URL = "mysql+pymysql://etl:etl_pass@localhost:3306/?charset=utf8mb4"

_BOOTSTRAP_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS etl_meta.schema_migrations ("
    "version VARCHAR(64) PRIMARY KEY,"
    "applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)"
    ") ENGINE=InnoDB COMMENT='DDL 版本记录'"
)


def _split_statements(sql: str) -> list[str]:
    """按行尾 ';' 拆分语句(DDL 文件约定,见模块 docstring)。"""
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    cleaned = "\n".join(lines)
    return [s.strip() for s in re.split(r";\s*\n", cleaned) if s.strip()]


def migrate(url: str, ddl_dir: Path = DDL_DIR) -> list[str]:
    """按序应用 ddl_dir 中未执行过的 *.sql,返回本次应用的版本列表。"""
    engine = sa.create_engine(url)
    try:
        # 引导: schema_migrations 表本身必须存在才能记账
        with engine.begin() as conn:
            conn.execute(sa.text("CREATE DATABASE IF NOT EXISTS etl_meta DEFAULT CHARACTER SET utf8mb4"))
            conn.execute(sa.text(_BOOTSTRAP_SCHEMA))
        with engine.connect() as conn:
            applied = {row[0] for row in conn.execute(sa.text("SELECT version FROM etl_meta.schema_migrations"))}

        applied_now: list[str] = []
        for f in sorted(ddl_dir.glob("*.sql")):
            version = f.stem
            if version in applied:
                continue
            statements = _split_statements(f.read_text(encoding="utf-8"))
            with engine.begin() as conn:
                for stmt in statements:
                    conn.execute(sa.text(stmt))
                conn.execute(
                    sa.text("INSERT INTO etl_meta.schema_migrations (version) VALUES (:v)"),
                    {"v": version},
                )
            applied_now.append(version)
        return applied_now
    finally:
        engine.dispose()


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    versions = migrate(url)
    if versions:
        for v in versions:
            print(f"[migrate] applied {v}")
    else:
        print("[migrate] up to date")


if __name__ == "__main__":
    main()
