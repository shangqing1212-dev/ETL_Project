"""EtlMetaHook: 连 etl_meta 元数据库(Connection 凭据,运行时解析 —— 遵守 DAG 解析期禁 DB 访问的 Airflow 3 硬约束)。"""

from __future__ import annotations

import sqlalchemy as sa
from airflow.sdk.bases.hook import BaseHook


class EtlMetaHook(BaseHook):
    """把 Airflow Connection(conn_id=etl_meta)解析成 SQLAlchemy engine。

    Connection 语义: host/port/login/password 为 MySQL 凭据,schema 为目标库
    (etl_meta 或 dw;引擎层面对两者均可读写,水表/批次在 etl_meta,业务表在 dw)。
    """

    conn_name_attr = "etl_meta_conn_id"
    default_conn_name = "etl_meta"
    conn_type = "etl_meta"
    hook_name = "ETL Meta MySQL"

    def __init__(self, etl_meta_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.etl_meta_conn_id = etl_meta_conn_id

    def get_engine(self, schema: str = "etl_meta") -> sa.Engine:
        conn = self.get_connection(self.etl_meta_conn_id)
        url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{schema}?charset=utf8mb4"
        return sa.create_engine(url)
