"""Airflow 3 provider 注册: entry_point `apache_airflow_provider` → get_provider_info。

Airflow 3 的插件机制统一为 provider(不再有 AirflowPlugin 类);
sdk/pyproject.toml 的 [project.entry-points] 声明本函数。
"""

from __future__ import annotations

from typing import Any


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "etl-sdk",  # 必须等于发行包名(pyproject name),Airflow 校验一致性
        "name": "ETL SDK Airflow Provider",
        "description": (
            "ETL 业务编排算子: 增量抽取(水位两阶段提交)、数仓构建(ODS→ADS)、DQ 扫描;"
            "业务逻辑全部沉淀在 etl_sdk,算子只做装配。"
        ),
        "connection-types": [
            {"connection-type": "etl_meta", "hook-class-name": "etl_sdk.airflow.hooks.etl_meta.EtlMetaHook"},
        ],
        "config": {},
        "version": ["0.1.0"],
    }
