"""Superset 初始化: 数据源 → 数据集 → 图表 → 3 张看板(幂等,按名称查重,重复执行不重复创建)。

用法(需先起 superset compose):
    uv run python scripts/init_superset.py                # 增量创建(已存在的跳过)
    uv run python scripts/init_superset.py --reset        # 按名称删除本脚本产物后重建

看板(与 M4 验收对应):
- 店铺日报(ads_shop_overview): GMV/订单量/退款率/客单价趋势 + 日报明细表
- 全局 KPI(ads_daily_kpi): GMV 与订单量大数卡 + 趋势
- 订单明细(dwd_orders): 状态分布饼图 + 订单明细表 + 每日订单量

说明: 图表参数为 ECharts v2 最小集,Superset 渲染时补默认值;视觉微调直接在 UI 上做,
本脚本保证"数据源 → 图表 → 看板"结构可复现。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from typing import Any
from urllib.parse import quote

import httpx

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin123")
# Superset 与数仓同处 etl-dev 容器网络,直连 etl-mysql:3306(生产改真实地址)
# 驱动用 pymysql(镜像内已装,见 superset/Dockerfile;mysqlclient 新版无 wheel)
WAREHOUSE_URI = os.environ.get("ETL_BI_DB_URI", "mysql+pymysql://etl:etl_pass@etl-mysql:3306/dw?charset=utf8mb4")

DATABASE_NAME = "ETL数仓"
SCHEMA = "dw"  # Superset 的 schema 概念对应 MySQL 的 database
DATASET_TABLES = ["dwd_orders", "ads_shop_overview", "ads_daily_kpi"]

# ---- 图表定义(viz_type + params;SQL 指标用 adhoc 表达式)----


def _adhoc(label: str, sql: str) -> dict[str, str]:
    return {"expressionType": "SQL", "label": label, "sqlExpression": sql}


def _timeseries_params(metric_label: str, metric_sql: str, *, bar: bool) -> str:
    params = {
        "adhoc_filters": [],
        "groupby": [],
        "metrics": [_adhoc(metric_label, metric_sql)],
        "row_limit": 10000,
        "x_axis": "stat_date",
    }
    return json.dumps(params, ensure_ascii=False)


def _table_params(columns: list[str]) -> str:
    return json.dumps(
        {"adhoc_filters": [], "all_columns": columns, "order_by_cols": [], "row_limit": 1000},
        ensure_ascii=False,
    )


def _big_number_params(label: str, sql: str) -> str:
    return json.dumps({"adhoc_filters": [], "metric": _adhoc(label, sql)}, ensure_ascii=False)


def _pie_params() -> str:
    return json.dumps(
        {
            "adhoc_filters": [],
            "groupby": ["status_norm"],
            "metrics": [_adhoc("订单数", "COUNT(*)")],
            "row_limit": 100,
            "show_legend": True,
        },
        ensure_ascii=False,
    )


CHARTS: list[dict[str, Any]] = [
    # ---- 店铺日报看板 ----
    {
        "slice_name": "GMV 趋势",
        "dataset": "ads_shop_overview",
        "viz_type": "echarts_timeseries_line",
        "params": _timeseries_params("GMV(分)", "SUM(gmv_cents)", bar=False),
        "dashboard": "店铺日报",
        "size": (12, 40),
    },
    {
        "slice_name": "订单量趋势",
        "dataset": "ads_shop_overview",
        "viz_type": "echarts_timeseries_bar",
        "params": _timeseries_params("订单量", "SUM(order_cnt)", bar=True),
        "dashboard": "店铺日报",
        "size": (12, 36),
    },
    {
        "slice_name": "退款率趋势",
        "dataset": "ads_shop_overview",
        "viz_type": "echarts_timeseries_line",
        "params": _timeseries_params("退款率", "AVG(refund_rate)", bar=False),
        "dashboard": "店铺日报",
        "size": (12, 36),
    },
    {
        "slice_name": "客单价趋势",
        "dataset": "ads_shop_overview",
        "viz_type": "echarts_timeseries_line",
        "params": _timeseries_params("客单价(分)", "AVG(avg_order_cents)", bar=False),
        "dashboard": "店铺日报",
        "size": (12, 36),
    },
    {
        "slice_name": "店铺日报明细",
        "dataset": "ads_shop_overview",
        "viz_type": "table",
        "params": _table_params(
            [
                "stat_date",
                "gmv_cents",
                "order_cnt",
                "buyer_cnt",
                "refund_cents",
                "avg_order_cents",
                "refund_rate",
                "gmv_dod_pct",
                "active_product_cnt",
                "sell_through_rate",
            ]
        ),
        "dashboard": "店铺日报",
        "size": (12, 50),
    },
    # ---- KPI 看板 ----
    {
        "slice_name": "GMV 大数卡",
        "dataset": "ads_daily_kpi",
        "viz_type": "big_number_total",
        "params": _big_number_params("GMV(分)", "SUM(gmv_cents)"),
        "dashboard": "全局 KPI",
        "size": (6, 40),
    },
    {
        "slice_name": "订单量大数卡",
        "dataset": "ads_daily_kpi",
        "viz_type": "big_number_total",
        "params": _big_number_params("订单量", "SUM(order_cnt)"),
        "dashboard": "全局 KPI",
        "size": (6, 40),
    },
    {
        "slice_name": "GMV 趋势(全局)",
        "dataset": "ads_daily_kpi",
        "viz_type": "echarts_timeseries_line",
        "params": _timeseries_params("GMV(分)", "SUM(gmv_cents)", bar=False),
        "dashboard": "全局 KPI",
        "size": (12, 36),
    },
    {
        "slice_name": "订单量趋势(全局)",
        "dataset": "ads_daily_kpi",
        "viz_type": "echarts_timeseries_bar",
        "params": _timeseries_params("订单量", "SUM(order_cnt)", bar=True),
        "dashboard": "全局 KPI",
        "size": (12, 36),
    },
    # ---- 订单明细看板 ----
    {
        "slice_name": "订单状态分布",
        "dataset": "dwd_orders",
        "viz_type": "echarts_pie",
        "params": _pie_params(),
        "dashboard": "订单明细",
        "size": (5, 40),
    },
    {
        "slice_name": "每日订单量(明细)",
        "dataset": "dwd_orders",
        "viz_type": "echarts_timeseries_bar",
        "params": _timeseries_params("订单数", "COUNT(*)", bar=True),
        "dashboard": "订单明细",
        "size": (7, 40),
    },
    {
        "slice_name": "订单明细表",
        "dataset": "dwd_orders",
        "viz_type": "table",
        "params": _table_params(
            [
                "stat_date",
                "order_id",
                "status_norm",
                "buyer_nick",
                "payment_amount_cents",
                "refund_amount_cents",
                "created_at",
            ]
        ),
        "dashboard": "订单明细",
        "size": (12, 50),
    },
]

DASHBOARDS = ["店铺日报", "全局 KPI", "订单明细"]


class SupersetClient:
    """最小 API 客户端: 登录 + 按名称查重创建/删除(HTTP 400/404 视为可恢复)。"""

    def __init__(self, url: str, user: str, password: str) -> None:
        self.base = url.rstrip("/")
        self.client = httpx.Client(base_url=self.base, timeout=60.0)
        resp = self.client.post(
            "/api/v1/security/login",
            json={"username": user, "password": password, "provider": "db", "refresh": True},
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Superset 登录失败({resp.status_code}): {resp.text[:300]}")
        token = resp.json()["access_token"]
        self.client.headers["Authorization"] = f"Bearer {token}"

    def _get(self, path: str) -> list[dict[str, Any]]:
        resp = self.client.get(path)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        result = resp.json().get("result", [])
        return list(result) if isinstance(result, list) else []

    def _find(self, endpoint: str, field: str, name: str) -> dict[str, Any] | None:
        q = quote(f"(filters:!((col:{field},opr:eq,value:'{name}')))", safe="")
        rows = self._get(f"/api/v1/{endpoint}/?q={q}")
        return rows[0] if rows else None

    def _post(self, endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = self.client.post(f"/api/v1/{endpoint}/", json=body)
        if resp.status_code == 422:
            raise RuntimeError(f"POST /{endpoint} 422: {resp.text[:500]}\nbody={body}")
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return data
        raise RuntimeError(f"POST /{endpoint} 返回非对象: {data!r}")

    def _put(self, endpoint: str, id_: int, body: dict[str, Any]) -> None:
        resp = self.client.put(f"/api/v1/{endpoint}/{id_}", json=body)
        if resp.status_code in (400, 422):  # 字段集随版本变化,尽力而为
            print(f"[superset] 警告: PUT /{endpoint}/{id_} 被拒绝({resp.status_code}),跳过")
            return
        resp.raise_for_status()

    def _delete(self, endpoint: str, id_: int) -> None:
        self.client.delete(f"/api/v1/{endpoint}/{id_}").raise_for_status()

    def ensure_database(self, name: str, uri: str) -> int:
        existing = self._find("database", "database_name", name)
        if existing:
            return int(existing["id"])
        return int(
            self._post("database", {"database_name": name, "sqlalchemy_uri": uri, "expose_in_sqllab": True})["id"]
        )

    def ensure_dataset(self, database_id: int, schema: str, table: str) -> int:
        existing = self._find("dataset", "table_name", table)
        if existing:
            return int(existing["id"])
        return int(self._post("dataset", {"database": database_id, "schema": schema, "table_name": table})["id"])

    def ensure_chart(self, chart: dict[str, Any], dataset_id: int) -> int:
        existing = self._find("chart", "slice_name", chart["slice_name"])
        if existing:
            return int(existing["id"])
        return int(
            self._post(
                "chart",
                {
                    "slice_name": chart["slice_name"],
                    "viz_type": chart["viz_type"],
                    "params": chart["params"],
                    "datasource_id": dataset_id,
                    "datasource_type": "table",
                },
            )["id"]
        )

    def ensure_dashboard(self, title: str, chart_positions: dict[str, Any]) -> int:
        existing = self._find("dashboard", "dashboard_title", title)
        if existing:
            dash_id = int(existing["id"])
            # 位置元数据以 PUT 为准(允许增量追加图表)
            self._put("dashboard", dash_id, {"json_metadata": json.dumps(chart_positions, ensure_ascii=False)})
            return dash_id
        return int(
            self._post(
                "dashboard",
                {
                    "dashboard_title": title,
                    "json_metadata": json.dumps(chart_positions, ensure_ascii=False),
                    "owners": [1],  # 新装环境 admin 为 1 号用户
                    "published": True,
                },
            )["id"]
        )

    def reset(self) -> None:
        """按名称删除本脚本产物(看板 → 图表 → 数据集 → 数据源),供 --reset 重建。"""
        for title in DASHBOARDS:
            d = self._find("dashboard", "dashboard_title", title)
            if d:
                self._delete("dashboard", int(d["id"]))
                print(f"[superset] 删除看板 {title}")
        for chart in CHARTS:
            c = self._find("chart", "slice_name", chart["slice_name"])
            if c:
                self._delete("chart", int(c["id"]))
                print(f"[superset] 删除图表 {chart['slice_name']}")
        for table in DATASET_TABLES:
            ds = self._find("dataset", "table_name", table)
            if ds:
                self._delete("dataset", int(ds["id"]))
                print(f"[superset] 删除数据集 {table}")
        db = self._find("database", "database_name", DATABASE_NAME)
        if db:
            self._delete("database", int(db["id"]))
            print(f"[superset] 删除数据源 {DATABASE_NAME}")


def build_positions(charts: list[tuple[str, int, tuple[int, int]]]) -> dict[str, Any]:
    """图表 → 12 列网格布局的 dashboard json_metadata(纵向堆叠)。"""
    positions: dict[str, Any] = {
        "ROOT_ID": {"children": [], "id": "ROOT_ID", "type": "ROOT"},
    }
    for name, chart_id, (width, height) in charts:
        key = f"CHART-{chart_id}"
        positions["ROOT_ID"]["children"].append(key)
        positions[key] = {
            "children": [],
            "id": key,
            "meta": {
                "chartId": chart_id,
                "height": height,
                "sliceName": name,
                "uuid": uuid.uuid4().hex,
                "width": width,
            },
            "parents": ["ROOT_ID"],
            "type": "CHART",
        }
    return {"positions": positions, "native_filter_configuration": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化 Superset 数据源/图表/看板(幂等)")
    parser.add_argument("--url", default=SUPERSET_URL, help="Superset 地址")
    parser.add_argument("--user", default=SUPERSET_USER)
    parser.add_argument("--password", default=SUPERSET_PASSWORD)
    parser.add_argument("--db-uri", default=WAREHOUSE_URI, help="数仓连接串(Superset 容器内视角)")
    parser.add_argument("--reset", action="store_true", help="按名称删除本脚本产物后重建")
    args = parser.parse_args()

    client = SupersetClient(args.url, args.user, args.password)
    if args.reset:
        client.reset()

    database_id = client.ensure_database(DATABASE_NAME, args.db_uri)
    dataset_ids = {t: client.ensure_dataset(database_id, SCHEMA, t) for t in DATASET_TABLES}
    print(f"[superset] 数据源 {DATABASE_NAME}(id={database_id}),数据集 {len(dataset_ids)} 个就绪")

    dashboard_charts: dict[str, list[tuple[str, int, tuple[int, int]]]] = {d: [] for d in DASHBOARDS}
    for chart in CHARTS:
        chart_id = client.ensure_chart(chart, dataset_ids[chart["dataset"]])
        dashboard_charts[chart["dashboard"]].append((chart["slice_name"], chart_id, chart["size"]))

    for title, charts in dashboard_charts.items():
        dash_id = client.ensure_dashboard(title, build_positions(charts))
        print(f"[superset] 看板 {title}(id={dash_id})含 {len(charts)} 张图表")

    print(f"[superset] 完成: 3 张看板,共 {len(CHARTS)} 张图表;访问 {args.url}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as exc:
        print(f"[superset] 失败: {exc}", file=sys.stderr)
        sys.exit(1)
