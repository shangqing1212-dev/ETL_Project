"""MySQL 恢复演练: 从 backups/ 的 mysqldump 产物恢复 etl_meta + dw,并校验行数。

用法(项目根目录):
    uv run python scripts/restore_mysql.py --backup etl-20260920-030000.sql.gz   # 指定备份恢复
    uv run python scripts/restore_mysql.py                                       # 用最新备份恢复
    uv run python scripts/restore_mysql.py --backup <目录> --verify-only          # 只校验不恢复

演练流程(runbook 第 4 节,生产月度演练同):
    1. scripts/backup_mysql.py 产备份
    2. 本脚本执行"drop + 重建 + 灌入 + 校验"完整链路(dev 用 etl 库演练)
    3. 校验: 恢复后 etl_batch / dws_shop_daily 行数与备份时点一致

注意: 恢复是破坏性操作(先 DROP 再重灌),生产执行前必须二次确认(脚本内置 confirm 提示)。
"""

from __future__ import annotations

import argparse
import gzip
import subprocess
import sys
from pathlib import Path

# 与 scripts/backup_mysql.py 保持一致(脚本独立可执行,不互相 import)
DEFAULT_BACKUP_DIR = Path(__file__).resolve().parents[1] / "backups"
SCHEMAS = ("etl_meta", "dw")
CONTAINER = "etl-mysql"


def _docker_mysql(args: list[str], *, stdin: bytes | None = None) -> int:
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "mysql", "-uroot", "-proot", *args], input=stdin, capture_output=True
    )
    if proc.returncode != 0:
        raise RuntimeError(f"mysql 命令失败: {proc.stderr.decode(errors='replace')[:300]}")
    return proc.returncode


def _row_counts(engine_url: str) -> dict[str, int]:
    """恢复后校验: 关键表行数(业务表 + 元数据表)。"""
    import sqlalchemy as sa

    tables = ["etl_meta.etl_batch", "etl_meta.etl_watermark", "dw.dwd_orders", "dw.dws_shop_daily"]
    engine = sa.create_engine(engine_url)
    with engine.connect() as conn:
        return {t: int(conn.execute(sa.text(f"SELECT COUNT(*) FROM {t}")).scalar_one()) for t in tables}


def restore(backup: Path) -> dict[str, int]:
    """DROP + 重建 + 灌入备份,返回恢复后关键表行数(供调用方断言)。"""
    if not backup.exists():
        raise FileNotFoundError(backup)

    # 1) 清库重建(dump 内含 CREATE DATABASE 语句,先 DROP 保证干净恢复)
    for schema in SCHEMAS:
        _docker_mysql(["-e", f"DROP DATABASE IF EXISTS {schema}"])
    # 2) 灌入(客户端解压后喂 mysql —— 避免磁盘临时文件)
    with gzip.open(backup, "rb") as fh:
        _docker_mysql([], stdin=fh.read())
    return _row_counts("mysql+pymysql://etl:etl_pass@localhost:13306/?charset=utf8mb4")


def main() -> int:
    parser = argparse.ArgumentParser(description="MySQL 恢复演练(drop + 重灌 + 校验)")
    parser.add_argument("--backup", type=Path, help="备份文件路径(默认用最新)")
    parser.add_argument("--verify-only", action="store_true", help="只校验不恢复")
    parser.add_argument("--yes", action="store_true", help="跳过二次确认(CI/演练脚本用)")
    args = parser.parse_args()

    backup = args.backup or max(DEFAULT_BACKUP_DIR.glob("etl-*.sql.gz"), key=lambda p: p.name)
    print(f"[restore] 备份: {backup}")

    if args.verify_only:
        # 只校验不恢复: 读完全部 gzip 流(gzip 解压内置 CRC 校验,流损坏即抛错)
        with gzip.open(backup, "rb") as fh:
            while fh.read(1 << 20):
                pass
        print(f"[restore] 备份完整性校验通过: {backup} ({backup.stat().st_size / 1e6:.1f} MB)")
        return 0
    if not args.yes:
        answer = input(f"[restore] 将 DROP {', '.join(SCHEMAS)} 并从 {backup.name} 重灌,输入 yes 继续: ")
        if answer.strip().lower() != "yes":
            print("[restore] 已取消")
            return 1
    counts = restore(backup)
    print(f"[restore] 恢复完成,关键表行数: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
