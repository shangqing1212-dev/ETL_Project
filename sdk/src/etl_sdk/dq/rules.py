"""DQ 规则实现: 每条规则为 (conn, RuleDef, DQContext) -> RuleVerdict 的函数。

执行时机: 批装载完成后(数据已 commit,全表规则可见本批数据)。
检查范围: null_rate/range/freshness 默认窗口级(本批窗口内落库的行,
按 shop_id+platform 过滤,天然兼容重试);unique/referential 全表级;
row_count_delta 读 etl_batch 历史。
约定:
- 空窗口/无历史等"无意义检查"情形返回 passed=True 并注明 detail(跳过),不误报
- 规则实现抛异常由引擎捕获并判 failed —— block 规则因此中止批次(安全方向)
- 列名/表名来自配置,仍做标识符白名单校验(配置可能来自手工编辑)
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from etl_sdk.dq.defs import RuleDef

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


@dataclass(frozen=True)
class DQContext:
    """规则执行上下文(批级)。"""

    table_name: str
    batch_id: str
    shop_id: int
    platform: str
    run_type: str
    rows_read: int
    now: datetime
    window_start: datetime | None = None
    window_end: datetime | None = None


@dataclass(frozen=True)
class RuleVerdict:
    """规则执行结论(不含定义信息,由引擎拼装成 RuleOutcome)。"""

    passed: bool
    actual: str
    expected: str
    detail: str | None = None


def _col(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"非法列名 {name!r}")
    return f"`{name}`"


def _table(name: str) -> str:
    if not _TABLE_RE.match(name):
        raise ValueError(f"非法表名 {name!r}")
    return ".".join(f"`{p}`" for p in name.split("."))


def _param(params: Mapping[str, Any], key: str) -> Any:
    if key not in params:
        raise ValueError(f"规则缺少参数 {key!r}")
    return params[key]


def _window_scope(rule: RuleDef, ctx: DQContext) -> str:
    """null_rate/range/freshness 默认按窗口检查(本批窗口内落库的行)。

    不用 etl_batch_id: 幂等 upsert 的单调门控会让未变更行保留旧 batch_id,
    重跑批次按 batch_id 过滤会漏检;窗口 + 租户过滤与抽取语义一致且天然兼容重试。
    scope=table 时全表检查。
    """
    scope = rule.params.get("scope", "window")
    if scope == "window":
        if ctx.window_start is None or ctx.window_end is None:
            raise ValueError("窗口级规则需要 window_start/window_end")
        return (
            "WHERE shop_id = :shop_id AND platform = :platform "
            "AND updated_at >= :window_start AND updated_at < :window_end"
        )
    if scope == "table":
        return ""
    raise ValueError(f"非法 scope {scope!r}(可选: window/table)")


def _window_binds(ctx: DQContext) -> dict[str, Any]:
    return {
        "shop_id": ctx.shop_id,
        "platform": ctx.platform,
        "window_start": ctx.window_start,
        "window_end": ctx.window_end,
    }


def rule_null_rate(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    col = _col(_param(rule.params, "column"))
    max_rate = float(_param(rule.params, "max_rate"))
    row = conn.execute(
        sa.text(f"SELECT COUNT(*), SUM({col} IS NULL) FROM {_table(ctx.table_name)} {_window_scope(rule, ctx)}"),
        _window_binds(ctx),
    ).one()
    total, nulls = int(row[0]), int(row[1] or 0)
    if total == 0:
        return RuleVerdict(True, "-", f"null_rate <= {max_rate}", "空批次,跳过")
    rate = nulls / total
    passed = rate <= max_rate
    return RuleVerdict(passed, f"{rate:.4f}", f"<= {max_rate}", None if passed else f"空值 {nulls}/{total}")


def rule_unique(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    cols = _param(rule.params, "columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError("unique 规则参数 columns 必须为非空列表")
    group = ", ".join(_col(c) for c in cols)
    dup_groups = int(
        conn.execute(
            sa.text(
                f"SELECT COUNT(*) FROM (SELECT 1 FROM {_table(ctx.table_name)} "
                f"GROUP BY {group} HAVING COUNT(*) > 1) AS dup"
            )
        ).scalar_one()
    )
    return RuleVerdict(dup_groups == 0, f"重复组 {dup_groups}", "0")


def rule_range(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    col = _col(_param(rule.params, "column"))
    lo, hi = _param(rule.params, "min"), _param(rule.params, "max")
    row = conn.execute(
        sa.text(f"SELECT MIN({col}), MAX({col}) FROM {_table(ctx.table_name)} {_window_scope(rule, ctx)}"),
        _window_binds(ctx),
    ).one()
    actual_lo, actual_hi = row[0], row[1]
    if actual_lo is None and actual_hi is None:
        return RuleVerdict(True, "-", f"[{lo}, {hi}]", "列为全 NULL(空值由 null_rate 规则负责),跳过")
    passed = actual_lo >= lo and actual_hi <= hi
    return RuleVerdict(passed, f"[{actual_lo}, {actual_hi}]", f"[{lo}, {hi}]")


def rule_freshness(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    col = _col(str(rule.params.get("column", "updated_at")))
    max_age = int(_param(rule.params, "max_age_minutes"))
    max_ts = conn.execute(
        sa.text(f"SELECT MAX({col}) FROM {_table(ctx.table_name)} {_window_scope(rule, ctx)}"),
        _window_binds(ctx),
    ).scalar_one()
    if max_ts is None:
        return RuleVerdict(True, "-", f"新鲜度 <= {max_age} 分钟", "空批次,跳过")
    age_minutes = (ctx.now - max_ts).total_seconds() / 60
    passed = age_minutes <= max_age
    return RuleVerdict(passed, f"{age_minutes:.0f} 分钟", f"<= {max_age} 分钟")


def rule_row_count_delta(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    # 回填/全量的行数与增量不可比(窗口语义不同),直接跳过
    if ctx.run_type != "incremental":
        return RuleVerdict(True, "-", "-", f"run_type={ctx.run_type} 不适用行数波动")
    max_dev = float(_param(rule.params, "max_deviation"))
    lookback = int(rule.params.get("lookback_batches", 10))
    history = [
        int(r[0])
        for r in conn.execute(
            sa.text(
                "SELECT rows_read FROM etl_meta.etl_batch "
                "WHERE table_name = :t AND shop_id = :s AND platform = :p "
                "AND run_type = 'incremental' AND status = 'success' AND rows_read > 0 "
                "ORDER BY started_at DESC LIMIT :n"
            ),
            {"t": ctx.table_name, "s": ctx.shop_id, "p": ctx.platform, "n": lookback},
        )
    ]
    if len(history) < 2:
        return RuleVerdict(True, str(ctx.rows_read), f"偏差 <= {max_dev:.0%}", "历史成功批次数不足,跳过")
    avg = sum(history) / len(history)
    deviation = abs(ctx.rows_read - avg) / avg
    passed = deviation <= max_dev
    detail = None if passed else f"本批 {ctx.rows_read} 行,历史均值 {avg:.0f} 行(偏差 {deviation:.0%})"
    return RuleVerdict(passed, f"本批 {ctx.rows_read} / 均值 {avg:.0f}", f"偏差 <= {max_dev:.0%}", detail)


def rule_referential(conn: Connection, rule: RuleDef, ctx: DQContext) -> RuleVerdict:
    """子表外键引用完整性: 子表(规则所在表)的指定列必须全部命中父表。

    shop 列成对给出时按店铺配对 join(多租户下 order_id 可能跨店重复)。
    """
    child_col = _col(_param(rule.params, "child_column"))
    parent_table = _table(_param(rule.params, "parent_table"))
    parent_col = _col(_param(rule.params, "parent_column"))
    join_conds = [f"p.{parent_col} = c.{child_col}"]
    child_shop = rule.params.get("child_shop_column")
    parent_shop = rule.params.get("parent_shop_column")
    if (child_shop is None) != (parent_shop is None):
        raise ValueError("referential 的 shop 列需成对提供(child_shop_column/parent_shop_column)")
    if child_shop is not None and parent_shop is not None:
        join_conds.append(f"p.{_col(str(parent_shop))} = c.{_col(str(child_shop))}")
    orphans = int(
        conn.execute(
            sa.text(
                f"SELECT COUNT(*) FROM {_table(ctx.table_name)} AS c "
                f"LEFT JOIN {parent_table} AS p ON {' AND '.join(join_conds)} "
                f"WHERE p.{parent_col} IS NULL"
            )
        ).scalar_one()
    )
    return RuleVerdict(orphans == 0, f"孤儿行 {orphans}", "0")


RULES: dict[str, Callable[[Connection, RuleDef, DQContext], RuleVerdict]] = {
    "null_rate": rule_null_rate,
    "unique": rule_unique,
    "range": rule_range,
    "freshness": rule_freshness,
    "row_count_delta": rule_row_count_delta,
    "referential": rule_referential,
}
