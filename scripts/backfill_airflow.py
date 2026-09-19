"""Airflow 回填封装: 在 dev compose 内创建 backfill(run_type=backfill,SDK 不推进水位)。

用法(项目根目录,dev compose 已起):
    uv run python scripts/backfill_airflow.py etl_orders --start 2026-09-12 --end 2026-09-18
    uv run python scripts/backfill_airflow.py dws_daily  --start 2026-09-01 --end 2026-09-18 --rerun-failed

说明(Airflow 3.3 新回填命令组):
- 底层: airflow backfill create --dag-id DAG --from-date START --to-date END(两端含)
- backfill 的 dag_run.run_type=backfill → 抽取窗口=data_interval,不推进水位(ADR-003);
  dws_daily 构建窗口起点=data_interval_start。
- --rerun-failed → --reprocess-behavior failed(只重跑已存在的失败 run)
- 与实时链路对比验证(M6): 同窗口回填结果应与实时跑一致(幂等 upsert/insert-overwrite)。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

COMPOSE_FILE = Path(__file__).resolve().parents[1] / "airflow" / "docker-compose.dev.yaml"
EXEC_CONTAINER = "etl-airflow-scheduler"


def main() -> int:
    parser = argparse.ArgumentParser(description="Airflow DAG 回填(dev compose,3.3 backfill create)")
    parser.add_argument("dag_id", help="DAG id(etl_orders / dws_daily / dq_monitor)")
    parser.add_argument("--start", required=True, help="回填窗口起点 YYYY-MM-DD(含)")
    parser.add_argument("--end", required=True, help="回填窗口终点 YYYY-MM-DD(含)")
    parser.add_argument("--rerun-failed", action="store_true", help="只重跑失败 run(reprocess-behavior=failed)")
    parser.add_argument("--dry-run", action="store_true", help="只打印命令不执行")
    args = parser.parse_args()

    cmd = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "-T",
        EXEC_CONTAINER,
        "airflow",
        "backfill",
        "create",
        "--dag-id",
        args.dag_id,
        "--from-date",
        args.start,
        "--to-date",
        args.end,
    ]
    if args.rerun_failed:
        cmd.extend(["--reprocess-behavior", "failed"])
    cmd.append("--dry-run" if args.dry_run else "--no-run-on-latest-version")

    print(f"[backfill] {'[dry-run] ' if args.dry_run else ''}{' '.join(cmd)}")
    if args.dry_run:
        return 0
    result = subprocess.run(cmd, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
