"""DQ 规则定义单测: YAML 加载与校验(表同步部分见集成测试)。"""

from __future__ import annotations

from pathlib import Path

import pytest
from etl_sdk.dq.defs import RuleSpec, load_rule_specs, validate_spec

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def test_load_rule_specs(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text(
        "- table_name: dw.ods_orders\n"
        "  rule_type: null_rate\n"
        "  params: {column: buyer_nick, max_rate: 0.0}\n"
        "  severity: block\n",
        encoding="utf-8",
    )
    assert load_rule_specs(p) == [
        RuleSpec(
            table_name="dw.ods_orders",
            rule_type="null_rate",
            params={"column": "buyer_nick", "max_rate": 0.0},
            severity="block",
        )
    ]


def test_reject_unknown_rule_type(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text("- table_name: dw.t\n  rule_type: wat\n  params: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="未知规则类型"):
        load_rule_specs(p)


def test_reject_bad_severity(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text("- table_name: dw.t\n  rule_type: null_rate\n  params: {}\n  severity: explode\n", encoding="utf-8")
    with pytest.raises(ValueError, match="非法级别"):
        load_rule_specs(p)


def test_reject_missing_required_field(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text("- table_name: dw.t\n  severity: warn\n", encoding="utf-8")
    with pytest.raises(ValueError, match="字段缺失或未知"):
        load_rule_specs(p)


def test_reject_non_mapping_entry(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text("- just-a-string\n", encoding="utf-8")
    with pytest.raises(ValueError, match="必须是映射"):
        load_rule_specs(p)


@pytest.mark.parametrize(
    "table_name",
    ["dw.t; DROP TABLE x", "dw..t", "dw.t t2", "dw.t-1"],
)
def test_reject_illegal_table_name(table_name: str) -> None:
    with pytest.raises(ValueError, match="非法表名"):
        validate_spec(RuleSpec(table_name, "null_rate", {}, "warn"))


def test_production_rules_yaml_is_valid() -> None:
    """生产规则源随单测校验: 新增规则类型/级别拼写错误在 CI 即失败。"""
    specs = load_rule_specs(_PROJECT_ROOT / "sql" / "dq_rules.yaml")
    assert len(specs) >= 6
    expected = {"null_rate", "unique", "range", "freshness", "row_count_delta", "referential"}
    assert {s.rule_type for s in specs} >= expected
