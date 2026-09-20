"""ETL 监控指标导出器: 读 etl_meta 输出 Prometheus 文本格式(标准库 http.server,零额外依赖)。

监控策略(ADR-006): 只盯 4 个面板 —— DAG 成功率、任务时长 p95、DQ 失败数、批次延迟。
Airflow 自带 metrics 走 statsd(需额外 exporter),ETL 侧直接以 etl_meta 为唯一事实源,
指标口径与数仓一致,不引入第二套告警系统。

用法(项目根目录,dev compose mysql 运行中):
    uv run python scripts/etl_metrics_exporter.py [--port 9101]
    curl http://localhost:9101/metrics
    # 接入: deploy/prometheus/prometheus.yml 已含 scrape 配置;Grafana 面板见 deploy/grafana/

指标(近 24h 窗口):
- etl_batch_success_rate{table}: 抽取批次成功率
- etl_batch_delay_minutes{shop_id,platform}: 最新批次窗口终点距 now 的延迟(新鲜度)
- etl_task_duration_ms_p95{dag_id}: SDK 任务时长 p95
- etl_dq_failures_total: DQ 失败结果数(近 24h)
- etl_load_errors_total: 死信数(近 24h)
- etl_watermark_age_minutes{table,shop_id,platform}: 水位延迟
"""

from __future__ import annotations

import argparse
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import sqlalchemy as sa
from etl_sdk.config import get_settings

WINDOW = "24 HOUR"


def collect(engine: sa.Engine) -> list[str]:
    """聚合 etl_meta → Prometheus 文本行(查询近 24h)。"""
    lines: list[str] = []
    with engine.connect() as conn:
        # 批次成功率(近 24h,按表)
        for table, total, success in conn.execute(
            sa.text(
                "SELECT table_name, COUNT(*), SUM(status = 'success') "
                "FROM etl_meta.etl_batch WHERE started_at >= NOW(3) - INTERVAL 24 HOUR GROUP BY table_name"
            ),
        ):
            rate = float(success or 0) / float(total)
            lines.append(f'etl_batch_success_rate{{table="{table}"}} {rate:.4f}')

        # 批次延迟(各店铺/平台最新成功批次的窗口终点距现在)
        for table, shop_id, platform, delay_min in conn.execute(
            sa.text(
                "SELECT t.table_name, t.shop_id, t.platform, "
                "TIMESTAMPDIFF(MINUTE, MAX(t.window_end), NOW(3)) "
                "FROM etl_meta.etl_batch t "
                "INNER JOIN (SELECT table_name, shop_id, platform, MAX(started_at) AS last_started "
                "FROM etl_meta.etl_batch WHERE status = 'success' AND window_end IS NOT NULL "
                "GROUP BY table_name, shop_id, platform) latest "
                "ON t.table_name = latest.table_name AND t.shop_id = latest.shop_id "
                "AND t.platform = latest.platform AND t.started_at = latest.last_started "
                "GROUP BY t.table_name, t.shop_id, t.platform"
            )
        ):
            lines.append(
                f'etl_batch_delay_minutes{{table="{table}",shop_id="{shop_id}",platform="{platform}"}} {delay_min}'
            )

        # 任务时长 p95(近 24h,按 DAG;MySQL 无 percentile 聚合,用子查询近似 p95)
        for dag_id, p95_ms in conn.execute(
            sa.text(
                "SELECT dag_id, MAX(duration_ms) AS p95_approx FROM ("
                "  SELECT dag_id, duration_ms, "
                "  PERCENT_RANK() OVER (PARTITION BY dag_id ORDER BY duration_ms) AS pr "
                "  FROM etl_meta.etl_task_run WHERE started_at >= NOW(3) - INTERVAL 24 HOUR "
                "  AND duration_ms IS NOT NULL"
                ") ranked WHERE pr <= 0.95 GROUP BY dag_id"
            ),
        ):
            lines.append(f'etl_task_duration_ms_p95{{dag_id="{dag_id}"}} {p95_ms}')

        # DQ 失败数(近 24h)
        dq_failures = conn.execute(
            sa.text(
                "SELECT COUNT(*) FROM etl_meta.dq_check_result "
                "WHERE evaluated_at >= NOW(3) - INTERVAL 24 HOUR AND passed = 0"
            ),
        ).scalar_one()
        lines.append(f"etl_dq_failures_total {dq_failures}")

        # 死信数(近 24h)
        dead = conn.execute(
            sa.text(
                "SELECT COUNT(*) FROM etl_meta.etl_load_error le "
                "JOIN etl_meta.etl_batch b ON le.batch_id = b.batch_id "
                "WHERE b.started_at >= NOW(3) - INTERVAL 24 HOUR"
            ),
        ).scalar_one()
        lines.append(f"etl_load_errors_total {dead}")

        # 水位延迟
        for table, shop_id, platform, age_min in conn.execute(
            sa.text(
                "SELECT table_name, shop_id, platform, TIMESTAMPDIFF(MINUTE, watermark_value, NOW(3)) "
                "FROM etl_meta.etl_watermark WHERE watermark_type = 'updated_at'"
            )
        ):
            lines.append(
                f'etl_watermark_age_minutes{{table="{table}",shop_id="{shop_id}",platform="{platform}"}} {age_min}'
            )
    return lines


class MetricsHandler(BaseHTTPRequestHandler):
    engine: sa.Engine

    def do_GET(self) -> None:  # noqa: N802 —— http.server 命名约定
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = "\n".join(collect(self.engine)) + "\n"
        payload = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args: object) -> None:  # noqa: A002 —— 静默访问日志
        return


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL Prometheus 指标导出器")
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--interval", type=float, default=0.0, help="退出前运行秒数(0=常驻;测试用)")
    args = parser.parse_args()

    engine = sa.create_engine(get_settings().db.meta_url)
    if args.interval > 0:  # 一次性输出(冒烟测试用)
        print("\n".join(collect(engine)))
        return 0

    handler = type("Handler", (MetricsHandler,), {"engine": engine})
    server = ThreadingHTTPServer(("0.0.0.0", args.port), handler)
    print(f"[metrics] 监听 :{args.port}/metrics(数据源 etl_meta,窗口近 24h)")
    with suppress(KeyboardInterrupt):
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
