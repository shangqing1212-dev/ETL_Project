"""DQ 规则定义: YAML 配置加载/校验 + 幂等同步进 etl_meta.dq_check_def(规则即配置)。

运行时事实来源是 dq_check_def 表(DQEngine 只读表执行);YAML 是代码库内的
可审计配置源,由 scripts/sync_dq_rules.py 同步。同步为增量插入(按
table_name + rule_type + params 判定),修改 YAML 后重跑即可新增;
删除规则需手工禁用(enabled=0)或 DELETE 对应行 —— 避免变更 check_id 使
dq_check_result 历史失去指向。
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import sqlalchemy as sa
import yaml

VALID_RULE_TYPES = frozenset({"null_rate", "unique", "range", "freshness", "row_count_delta", "referential"})
VALID_SEVERITIES = frozenset({"warn", "block"})
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


@dataclass(frozen=True)
class RuleSpec:
    """YAML 中的一条规则定义(check_id 由表自增分配)。"""

    table_name: str
    rule_type: str
    params: dict[str, Any]
    severity: str = "warn"


@dataclass(frozen=True)
class RuleDef:
    """表内已注册的规则(dq_check_def 行)。"""

    check_id: int
    table_name: str
    rule_type: str
    params: dict[str, Any]
    severity: str


def validate_spec(spec: RuleSpec) -> None:
    """校验单条规则定义(表名/规则类型/级别);参数键由各规则实现自行校验。"""
    if not _TABLE_RE.match(spec.table_name):
        raise ValueError(f"非法表名 {spec.table_name!r}")
    if spec.rule_type not in VALID_RULE_TYPES:
        raise ValueError(f"未知规则类型 {spec.rule_type!r}(可选: {sorted(VALID_RULE_TYPES)})")
    if spec.severity not in VALID_SEVERITIES:
        raise ValueError(f"非法级别 {spec.severity!r}(可选: warn/block)")


def load_rule_specs(path: str | Path) -> list[RuleSpec]:
    """加载并校验 YAML 规则文件(顶层为规则列表)。"""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or []
    if not isinstance(raw, list):
        raise ValueError(f"规则文件 {path} 顶层必须是列表")
    specs: list[RuleSpec] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"规则 #{i} 必须是映射,实际为 {type(item).__name__}")
        try:
            specs.append(RuleSpec(**cast("dict[str, Any]", item)))
        except TypeError as exc:
            raise ValueError(f"规则 #{i} 字段缺失或未知: {exc}") from exc
    for spec in specs:
        validate_spec(spec)
    return specs


def sync_rules(engine: sa.Engine, specs: Sequence[RuleSpec]) -> dict[str, int]:
    """幂等同步进 dq_check_def,返回 {"inserted": n, "skipped": n}。"""
    inserted = skipped = 0
    with engine.begin() as conn:
        for spec in specs:
            params_json = json.dumps(spec.params, sort_keys=True)
            exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM etl_meta.dq_check_def "
                    "WHERE table_name = :t AND rule_type = :rt AND params = CAST(:p AS JSON) LIMIT 1"
                ),
                {"t": spec.table_name, "rt": spec.rule_type, "p": params_json},
            ).first()
            if exists:
                skipped += 1
                continue
            conn.execute(
                sa.text(
                    "INSERT INTO etl_meta.dq_check_def (table_name, rule_type, params, severity) "
                    "VALUES (:t, :rt, CAST(:p AS JSON), :s)"
                ),
                {"t": spec.table_name, "rt": spec.rule_type, "p": params_json, "s": spec.severity},
            )
            inserted += 1
    return {"inserted": inserted, "skipped": skipped}
