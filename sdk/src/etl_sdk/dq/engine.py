"""DQ 引擎: 读 dq_check_def(规则即配置)-> 逐条执行 -> 结果落 dq_check_result。

block/warn 语义: 引擎只产出报告;block 失败的处置(中止批次、不推进水位)
由调用方(抽取编排层)决定,引擎不越权控制流程。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import sqlalchemy as sa
from structlog import get_logger

from etl_sdk.dq.defs import RuleDef
from etl_sdk.dq.rules import RULES, DQContext, RuleVerdict

logger = get_logger(__name__)


@dataclass(frozen=True)
class RuleOutcome:
    """一条规则的最终结果(含定义信息,已落库 dq_check_result)。"""

    check_id: int
    rule_type: str
    severity: str
    passed: bool
    actual: str
    expected: str
    detail: str | None = None


@dataclass(frozen=True)
class DQReport:
    """一轮 DQ 的报告: 按严重级别筛选失败项。"""

    outcomes: list[RuleOutcome]

    def block_failures(self) -> list[RuleOutcome]:
        return [o for o in self.outcomes if not o.passed and o.severity == "block"]

    def warn_failures(self) -> list[RuleOutcome]:
        return [o for o in self.outcomes if not o.passed and o.severity == "warn"]

    @property
    def blocked(self) -> bool:
        return bool(self.block_failures())


class DQBlockedError(Exception):
    """block 级 DQ 规则失败: 批次必须中止(水位不推进,由抽取编排层抛出)。"""

    def __init__(self, report: DQReport) -> None:
        self.report = report
        failed = "; ".join(f"{o.rule_type}(actual={o.actual}, expected={o.expected})" for o in report.block_failures())
        super().__init__(f"DQ block 规则失败 {len(report.block_failures())} 条: {failed}")


class DQEngine:
    """规则执行器: 只依赖 SQLAlchemy engine(与抽取器同库,可独立测试)。"""

    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine

    def run(
        self,
        *,
        table_name: str,
        batch_id: str,
        shop_id: int,
        platform: str,
        run_type: str,
        rows_read: int,
        now: datetime | None = None,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> DQReport:
        """执行该表全部启用规则,结果逐条落库,返回报告(规则异常判 failed,安全方向)。"""
        ctx = DQContext(
            table_name=table_name,
            batch_id=batch_id,
            shop_id=shop_id,
            platform=platform,
            run_type=run_type,
            rows_read=rows_read,
            now=now or datetime.now(),
            window_start=window_start,
            window_end=window_end,
        )
        outcomes: list[RuleOutcome] = []
        with self.engine.connect() as conn:
            for rule in self._load_defs(table_name):
                try:
                    verdict = RULES[rule.rule_type](conn, rule, ctx)
                except Exception as exc:
                    verdict = RuleVerdict(False, "error", "-", f"规则执行异常: {exc}")
                    logger.exception("dq.rule_error", table=table_name, rule=rule.rule_type, check_id=rule.check_id)
                outcomes.append(
                    RuleOutcome(
                        check_id=rule.check_id,
                        rule_type=rule.rule_type,
                        severity=rule.severity,
                        passed=verdict.passed,
                        actual=verdict.actual,
                        expected=verdict.expected,
                        detail=verdict.detail,
                    )
                )
                self._record(conn, rule, verdict, ctx)
        logger.info(
            "dq.run.done",
            table=table_name,
            batch_id=batch_id,
            total=len(outcomes),
            failed=sum(1 for o in outcomes if not o.passed),
        )
        return DQReport(outcomes)

    def _load_defs(self, table_name: str) -> list[RuleDef]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT check_id, table_name, rule_type, params, severity "
                    "FROM etl_meta.dq_check_def WHERE table_name = :t AND enabled = 1"
                ),
                {"t": table_name},
            )
            defs = []
            for row in rows:
                params = json.loads(row[3]) if isinstance(row[3], str) else cast("dict[str, Any]", row[3])
                defs.append(
                    RuleDef(
                        check_id=int(row[0]),
                        table_name=str(row[1]),
                        rule_type=str(row[2]),
                        params=params,
                        severity=str(row[4]),
                    )
                )
        return defs

    @staticmethod
    def _record(conn: sa.engine.Connection, rule: RuleDef, verdict: RuleVerdict, ctx: DQContext) -> None:
        # 规则 SELECT 已在该连接上 autobegin,不能再 conn.begin();逐条 execute+commit 落库
        conn.execute(
            sa.text(
                "INSERT INTO etl_meta.dq_check_result "
                "(check_id, batch_id, stat_date, actual_value, expected_value, passed, detail) "
                "VALUES (:c, :b, :d, :a, :e, :p, :det)"
            ),
            {
                "c": rule.check_id,
                "b": ctx.batch_id,
                "d": ctx.now.date(),
                "a": verdict.actual[:255],
                "e": verdict.expected[:255],
                "p": 1 if verdict.passed else 0,
                "det": (verdict.detail or "")[:2000],
            },
        )
        conn.commit()
