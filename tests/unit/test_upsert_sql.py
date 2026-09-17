"""upsert SQL 生成器单元测试。"""

from __future__ import annotations

import pytest
from etl_sdk.loaders.mysql import build_upsert_sql


def test_basic_upsert_sql_structure() -> None:
    sql = build_upsert_sql(
        "dw.ods_orders",
        ["shop_id", "order_id", "raw_json", "updated_at"],
        ["shop_id", "order_id"],
    )
    assert sql.startswith("INSERT INTO dw.ods_orders (`shop_id`, `order_id`, `raw_json`, `updated_at`)")
    assert "VALUES (:shop_id, :order_id, :raw_json, :updated_at) AS new" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql


def test_monotonic_col_uses_if_guard() -> None:
    sql = build_upsert_sql(
        "t",
        ["id", "status", "updated_at"],
        ["id"],
        monotonic_col="updated_at",
    )
    # 单调列门控: 所有非主键列的新值都受 new.updated_at > 旧值 保护
    assert "`updated_at` = IF(new.`updated_at` > `t`.`updated_at`, new.`updated_at`, `t`.`updated_at`)" in sql
    assert "`status` = IF(new.`updated_at` > `t`.`updated_at`, new.`status`, `t`.`status`)" in sql
    # 主键列不参与更新
    assert "`id` = new.`id`" not in sql


def test_no_monotonic_col_means_plain_overwrite() -> None:
    sql = build_upsert_sql("t", ["id", "status"], ["id"], monotonic_col=None)
    assert "`status` = new.`status`" in sql
    assert "IF(" not in sql


def test_all_columns_are_pk_raises() -> None:
    with pytest.raises(ValueError, match="非主键"):
        build_upsert_sql("t", ["a", "b"], ["a", "b"])
