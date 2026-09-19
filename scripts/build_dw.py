"""数仓构建 CLI: ODS → DWD → DWS → ADS(编排核心在 etl_sdk.dw.build,Airflow 侧共用)。

用法(项目根目录):
    uv run python scripts/build_dw.py                    # 增量构建: 最近 3 天(stat_date 窗口 [today-2, today])
    uv run python scripts/build_dw.py --days 7           # 最近 7 天
    uv run python scripts/build_dw.py --full             # 全量重建(2019-01-01 起)
    uv run python scripts/build_dw.py --since 2026-09-01 # 指定窗口起点(终点=今天)
    uv run python scripts/build_dw.py --shop-yaml PATH   # 指定店铺注册表

幂等语义见 etl_sdk/dw/build.py 模块 docstring(ADR-003/005)。
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import sqlalchemy as sa
from etl_sdk.alerts import build_alert_manager
from etl_sdk.config import get_settings
from etl_sdk.dw.build import FULL_HISTORY_START, load_shop_registry, run_build
from etl_sdk.logging_conf import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHOP_YAML = PROJECT_ROOT / "dags" / "config" / "shops.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(description="数仓构建: ODS → DWD → DWS → ADS")
    parser.add_argument("--full", action="store_true", help="全量重建(2019-01-01 起)")
    parser.add_argument("--days", type=int, default=3, help="增量构建窗口天数(默认 3,含今日)")
    parser.add_argument("--since", type=str, default=None, help="窗口起点 YYYY-MM-DD(优先级高于 --days)")
    parser.add_argument("--shop-yaml", type=Path, default=DEFAULT_SHOP_YAML, help="店铺注册表路径")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_format == "json")
    now = datetime.now().replace(microsecond=0)
    if args.since:
        since = date.fromisoformat(args.since)
    elif args.full:
        since = FULL_HISTORY_START
    else:
        if args.days < 1:
            parser.error("--days 必须 >= 1")
        since = now.date() - timedelta(days=args.days - 1)

    registry = load_shop_registry(args.shop_yaml)
    engine = sa.create_engine(settings.db.meta_url)
    alert_manager = build_alert_manager(settings.alert)
    try:
        summary = run_build(engine, registry, since=since, now=now, alert_manager=alert_manager)
    finally:
        engine.dispose()

    print(f"[build_dw] 窗口 [{since}, {now.date()}] 店铺 {len(registry)} 个 scd2 新版本={summary.shop_versions_added}")
    for r in summary.results:
        print(
            f"[build_dw] shop={r.shop_id} platform={r.platform} "
            f"ods={r.orders_read}/{r.items_read} dwd={r.dwd_orders_written}/{r.dwd_items_written} "
            f"dws={r.dws_shop_rows}/{r.dws_product_rows} ads={r.ads_overview_rows} kpi={r.ads_kpi_rows}"
        )


if __name__ == "__main__":
    main()
