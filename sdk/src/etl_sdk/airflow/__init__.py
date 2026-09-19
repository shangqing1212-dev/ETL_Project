"""Airflow 3 provider 插件(仅当 Airflow 环境 import 本包时加载;本地测试环境不依赖 Airflow)。

- EtlMetaHook: etl_meta 元数据库连接(Connection 运行时解析)
- EtlTableOperator: 单店铺实体增量抽取(水位两阶段提交)
- DwBuildOperator: 数仓构建(ODS → DWD → DWS → ADS)
- DqScanOperator: 周期 DQ 扫描(守护抽取健康)
"""
