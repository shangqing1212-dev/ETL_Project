"""pandera 装载前契约: 行集合的类型/值约束检查,坏行分离进死信(schema_mismatch)。

与 DQ 规则的区别: 契约在装载前逐行拦截(行级,一次性全部收集),DQ 规则在
装载后批级聚合检查。两者配合: 契约保证"落库的行形状正确",规则保证"落库后
的数据语义正确"。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pandera.polars as pa  # pandera 0.33 起按后端分模块(项目仅用 polars 后端)
import polars as pl
from pandera.dtypes import DateTime


@dataclass(frozen=True)
class ContractResult:
    """契约检查结果: 好行原样通过,坏行携带逐行错误消息(供死信记录)。"""

    good_rows: list[dict[str, Any]]
    bad_rows: list[tuple[dict[str, Any], str]]

    @property
    def bad_count(self) -> int:
        return len(self.bad_rows)


class SchemaContract:
    """DataFrameSchema 包装: validate 不抛异常,返回好坏行分离结果。

    lazy 模式一次性收集全部失败行;错误消息取 failure_cases 的
    column/check/failure_case 拼接,足够定位问题。
    """

    def __init__(self, schema: pa.DataFrameSchema) -> None:
        self.schema = schema

    def validate(self, rows: list[dict[str, Any]]) -> ContractResult:
        if not rows:
            return ContractResult([], [])
        df = pl.DataFrame(rows, infer_schema_length=None)
        try:
            self.schema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as exc:
            cases = cast("pl.DataFrame", exc.failure_cases)
            per_row: dict[int, list[str]] = {}
            global_msgs: list[str] = []
            for case in cases.to_dicts():
                msg = f"列 {case.get('column')} 检查 {case.get('check')} 失败: {case.get('failure_case')}"
                # 行级失败携带 index;缺列等 schema 级失败无 index,作用于全部行
                if case.get("index") is not None:
                    per_row.setdefault(int(case["index"]), []).append(msg)
                else:
                    global_msgs.append(msg)
            bad: list[tuple[dict[str, Any], str]] = []
            good: list[dict[str, Any]] = []
            for i, row in enumerate(rows):
                msgs = [*per_row.get(i, []), *global_msgs]
                if msgs:
                    bad.append((row, "; ".join(msgs)))
                else:
                    good.append(row)
            return ContractResult(good, bad)
        return ContractResult(rows, [])


def orders_ods_contract() -> SchemaContract:
    """ods_orders 装载前契约: 关键列非空、金额非负、状态码非空字符串。

    只约束通用形状(金额/主键),不绑定具体平台的枚举值 —— 平台枚举差异
    属于 DWD 清洗层职责。
    """
    return SchemaContract(
        pa.DataFrameSchema(
            {
                "order_id": pa.Column(str, pa.Check.str_length(min_value=1)),
                "shop_id": pa.Column(int),
                "platform": pa.Column(str),
                "order_status": pa.Column(str),
                "buyer_nick": pa.Column(str),
                "order_amount_cents": pa.Column(int, pa.Check.ge(0)),
                "payment_amount_cents": pa.Column(int, pa.Check.ge(0)),
                "refund_amount_cents": pa.Column(int, pa.Check.ge(0)),
                "raw_json": pa.Column(str),
                "is_deleted": pa.Column(int, pa.Check.isin([0, 1])),
                "created_at": pa.Column(DateTime),
                "updated_at": pa.Column(DateTime),
                "etl_batch_id": pa.Column(str),
            },
            strict=True,
        )
    )


def order_items_ods_contract() -> SchemaContract:
    """ods_order_items 装载前契约: 主键非空、数量为正、单价非负。"""
    return SchemaContract(
        pa.DataFrameSchema(
            {
                "order_id": pa.Column(str, pa.Check.str_length(min_value=1)),
                "shop_id": pa.Column(int),
                "item_id": pa.Column(str, pa.Check.str_length(min_value=1)),
                "platform": pa.Column(str),
                "product_id": pa.Column(str),
                "product_name": pa.Column(str),
                "quantity": pa.Column(int, pa.Check.gt(0)),
                "price_cents": pa.Column(int, pa.Check.ge(0)),
                "raw_json": pa.Column(str),
                "updated_at": pa.Column(DateTime),
                "etl_batch_id": pa.Column(str),
            },
            strict=True,
        )
    )
