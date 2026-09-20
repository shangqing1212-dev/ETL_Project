"""MySQL 备份: 每日 mysqldump 逻辑备份(etl_meta + dw),保留最近 N 份。

用法(项目根目录,compose 的 mysql 运行中):
    uv run python scripts/backup_mysql.py                 # 备份到 backups/(默认)
    uv run python scripts/backup_mysql.py --keep 14       # 保留 14 份
    uv run python scripts/backup_mysql.py --list          # 列出现有备份

生产差异(runbook 第 4 节):
- 本脚本对应"每日 mysqldump(etl_meta + ADS)"计划项;生产另配每周 XtraBackup 物理全量
  + binlog PITR(见 deploy/mysql/my.cnf 的 binlog 配置),由生产机 cron 调度本脚本。
- 恢复演练: scripts/restore_mysql.py --backup <目录>(dev 每月演练一次,生产演练周期同)。
"""

from __future__ import annotations

import argparse
import gzip
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKUP_DIR = PROJECT_ROOT / "backups"
CONTAINER = "etl-mysql"  # dev compose 容器;生产改为直连 mysql 命令(见 docstring)
SCHEMAS = ("etl_meta", "dw")


def run_backup(backup_dir: Path, *, container: str = CONTAINER) -> Path:
    """执行一次全库逻辑备份,返回产物路径 backups/etl-YYYYmmdd-HHMMSS.sql.gz。"""
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = backup_dir / f"etl-{stamp}.sql.gz"
    # dev 用 root(仅 dev);生产凭据与主机由部署流程注入,等价命令:
    #   mysqldump --single-transaction --routines --triggers --databases etl_meta dw | gzip > ...
    # 注意: gzip 文件对象不能直接作 Popen.stdout —— 其 fileno() 落到裸文件 fd,dump 会
    # 绕过压缩写出纯文本(2026-09-20 踩坑);必须 PIPE + 流式写入 gzip。
    proc = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            container,
            "mysqldump",
            "-uroot",
            "-proot",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--databases",
            *SCHEMAS,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    with gzip.open(out_path, "wb") as fh:
        while chunk := proc.stdout.read(1 << 20):
            fh.write(chunk)
    err = proc.stderr.read() if proc.stderr is not None else b""
    if proc.wait() != 0:
        out_path.unlink(missing_ok=True)
        raise RuntimeError(f"mysqldump 失败: {err.decode(errors='replace')[:300]}")
    # 产物自校验: 完整读一遍 gzip 流(解压内置 CRC 校验),确认备份真实可恢复
    with gzip.open(out_path, "rb") as fh:
        while fh.read(1 << 20):
            pass
    print(f"[backup] 完成: {out_path}({out_path.stat().st_size / 1024 / 1024:.1f} MB,已自校验可解压)")
    return out_path


def prune(backup_dir: Path, keep: int) -> None:
    """按文件名时间戳保留最近 keep 份。"""
    backups = sorted(backup_dir.glob("etl-*.sql.gz"))
    for old in backups[:-keep]:
        old.unlink()
        print(f"[backup] 清理旧备份 {old.name}")
    print(f"[backup] 现存 {len(backups[-keep:])} 份")


def main() -> int:
    parser = argparse.ArgumentParser(description="MySQL 每日逻辑备份(etl_meta + dw)")
    parser.add_argument("--dir", type=Path, default=DEFAULT_BACKUP_DIR)
    parser.add_argument("--keep", type=int, default=7, help="保留份数")
    parser.add_argument("--list", action="store_true", help="只列出现有备份")
    parser.add_argument("--container", default=CONTAINER)
    args = parser.parse_args()

    if args.list:
        for p in sorted(args.dir.glob("etl-*.sql.gz")):
            print(
                f"{p.name}  {p.stat().st_size / 1024 / 1024:.1f} MB  {datetime.fromtimestamp(p.stat().st_mtime):%F %T}"
            )
        return 0

    run_backup(args.dir, container=args.container)
    prune(args.dir, args.keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
