"""数据质量模块: 规则引擎(规则即配置)+ pandera 装载前契约。"""

from etl_sdk.dq.contracts import ContractResult, SchemaContract, order_items_ods_contract, orders_ods_contract
from etl_sdk.dq.defs import VALID_RULE_TYPES, RuleDef, RuleSpec, load_rule_specs, sync_rules, validate_spec
from etl_sdk.dq.engine import DQBlockedError, DQEngine, DQReport, RuleOutcome
from etl_sdk.dq.rules import RULES, DQContext, RuleVerdict

__all__ = [
    "ContractResult",
    "DQBlockedError",
    "DQContext",
    "DQEngine",
    "DQReport",
    "RULES",
    "RuleDef",
    "RuleOutcome",
    "RuleSpec",
    "RuleVerdict",
    "SchemaContract",
    "VALID_RULE_TYPES",
    "load_rule_specs",
    "order_items_ods_contract",
    "orders_ods_contract",
    "sync_rules",
    "validate_spec",
]
