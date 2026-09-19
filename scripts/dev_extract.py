"""开发联调: 对 compose 环境执行一轮订单抽取(增量或回填)。

用法(项目根目录):
    uv run python scripts/dev_extract.py                 # 增量(按水位)
    uv run python scripts/dev_extract.py backfill 3      # 回填最近 3 天(不推进水位)

依赖: docker compose -f airflow/docker-compose.dev.yaml up -d(mysql + mock-api),
      uv run python scripts/migrate.py
装配逻辑与 Airflow EtlTableOperator 共用 etl_sdk.runtime 工厂(保证同源)。
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

import sqlalchemy as sa
from etl_sdk.config import get_settings
from etl_sdk.extractors.base import TimeWindow
from etl_sdk.logging_conf import setup_logging
from etl_sdk.runtime import build_alert_manager_from, build_extractor, build_mock_adapter


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, json_output=settings.log_format == "json")

    engine = sa.create_engine(settings.db.meta_url)
    adapter = build_mock_adapter(settings)
    extractor = build_extractor(
        engine,
        adapter,
        settings,
        shop_id=1,
        platform=settings.platform.name,
        alert_manager=build_alert_manager_from(settings),
    )

    run_type = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    window_override = None
    if run_type == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        end = datetime.now().replace(microsecond=0)
        window_override = TimeWindow(start=end - timedelta(days=days), end=end)
        print(f"[dev_extract] 回填窗口: {window_override.start} ~ {window_override.end}(不推进水位)")

    result = extractor.extract(run_type=run_type, window_override=window_override)
    print(
        f"[dev_extract] done: {result.table_name} run={result.run_type} "
        f"rows_read={result.rows_read} rows_written={result.rows_written} "
        f"dead_letters={result.dead_letters} "
        f"watermark_advanced={result.watermark_advanced} batch={result.batch_id}"
    )


if __name__ == "__main__":
    main()
