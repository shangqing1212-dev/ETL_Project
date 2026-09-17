"""DQ 规则同步: sql/dq_rules.yaml -> etl_meta.dq_check_def(幂等,可重复执行)。

用法(项目根目录):
    uv run python scripts/migrate.py                      # 先保证表结构存在
    uv run python scripts/sync_dq_rules.py [--yaml PATH]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import sqlalchemy as sa
from etl_sdk.config import get_settings
from etl_sdk.dq.defs import load_rule_specs, sync_rules
from etl_sdk.logging_conf import setup_logging

DEFAULT_YAML = Path(__file__).resolve().parent.parent / "sql" / "dq_rules.yaml"


def main() -> int:
    parser = argparse.ArgumentParser(description="同步 DQ 规则定义进 etl_meta.dq_check_def")
    parser.add_argument("--yaml", type=Path, default=DEFAULT_YAML, help="规则 YAML 文件路径")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    specs = load_rule_specs(args.yaml)
    engine = sa.create_engine(settings.db.meta_url)
    result = sync_rules(engine, specs)
    print(f"[sync_dq_rules] {args.yaml}: {len(specs)} 条规则,inserted={result['inserted']} skipped={result['skipped']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
