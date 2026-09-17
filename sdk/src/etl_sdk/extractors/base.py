"""增量抽取编排: 窗口固定 -> 翻页抽取 -> 批内去重 -> 映射 -> 契约 -> 分批装载 -> DQ -> 两阶段推进水位。

核心不变量: 重跑同一窗口得到相同结果(幂等)。
- 窗口在批开始固定 [wm - overlap, now - delay),批内不取 "now"
- 全窗口成功且无异常才推进水位;失败水位不动,重跑自动重读同窗口
- 回填/全量模式不推进水位(窗口显式给定,upsert 保证幂等)

容错链(M3):
- 映射失败/契约失败的坏行进死信(etl_load_error),不中断整批
- 死信率超阈值(dead_letter_limit)中止批次 —— 数据源系统性变质时宁可失败告警
- 装载后 DQ: block 规则失败中止批次(不推进水位),warn 失败仅告警
- 批次失败(含 DQ block)统一发 error 级告警
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from structlog import get_logger

from etl_sdk.alerts.base import AlertManager
from etl_sdk.dq.contracts import SchemaContract
from etl_sdk.dq.engine import DQBlockedError, DQEngine, DQReport
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import Paginator
from etl_sdk.extractors.state import WatermarkState
from etl_sdk.loaders.dead_letter import DeadLetterLimitExceeded, DeadLetterRecorder, DeadLetterRow
from etl_sdk.loaders.mysql import MySQLBatchLoader

if TYPE_CHECKING:
    # 仅注解使用;运行时导入会与 adapters.base 形成循环依赖
    from etl_sdk.adapters.base import BasePlatformAdapter

logger = get_logger(__name__)

DEFAULT_HISTORY_START = datetime(2020, 1, 1)


@dataclass(frozen=True)
class TimeWindow:
    start: datetime  # 含
    end: datetime  # 不含


@dataclass
class ExtractionResult:
    table_name: str
    batch_id: str
    run_type: str
    window: TimeWindow
    rows_read: int
    rows_written: int
    watermark_advanced: bool
    dead_letters: int = 0


def compute_window(
    watermark: datetime | None,
    now: datetime,
    *,
    overlap: timedelta,
    delay: timedelta,
    history_start: datetime = DEFAULT_HISTORY_START,
) -> TimeWindow:
    """计算本次抽取窗口: [wm - overlap, now - delay)。无水位时从 history_start 全量起步。"""
    end = now - delay
    start = (watermark - overlap) if watermark is not None else history_start
    return TimeWindow(start=start, end=end)


def dedup_by_pk(
    rows: Sequence[Mapping[str, Any]], pk_cols: Sequence[str], ts_col: str = "updated_at"
) -> list[dict[str, Any]]:
    """批内去重: 同一业务主键保留 updated_at 最新的一条(重叠窗口/游标故障会造成同批多版本)。"""
    best: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(c) for c in pk_cols)
        current = best.get(key)
        if current is None or str(row.get(ts_col) or "") > str(current.get(ts_col) or ""):
            best[key] = dict(row)
    return list(best.values())


@dataclass
class EntitySpec:
    """一个实体(表)的装载配置: 目标表 + 列 + 主键 + 行映射 + 装载前契约。"""

    table_name: str
    columns: Sequence[str]
    pk_columns: Sequence[str]
    # 行映射: (源行, batch_id=..., platform=...) -> 目标行
    mapper: Callable[..., dict[str, Any]]
    # items 型子实体: 父行 -> 子行列表(如订单明细)
    children_mapper: Callable[..., list[dict[str, Any]]] | None = None
    # 装载前契约(可选): 坏行进死信,不阻断好行
    contract: SchemaContract | None = None


class BaseExtractor:
    """水位线驱动的增量抽取器(与 Airflow 无关,可独立运行/测试)。"""

    def __init__(
        self,
        *,
        adapter: BasePlatformAdapter,
        paginator: Paginator,
        entity: EntitySpec,
        loader: MySQLBatchLoader,
        watermark: WatermarkState,
        recorder: BatchRecorder,
        shop_id: int,
        platform: str,
        overlap: timedelta = timedelta(hours=1),
        delay: timedelta = timedelta(minutes=5),
        history_start: datetime = DEFAULT_HISTORY_START,
        now_fn: Callable[[], datetime] = datetime.now,
        dead_letter: DeadLetterRecorder | None = None,
        dead_letter_limit: float = 0.0,
        dq_engine: DQEngine | None = None,
        alert_manager: AlertManager | None = None,
    ) -> None:
        self._adapter = adapter
        self._paginator = paginator
        self._entity = entity
        self._loader = loader
        self._watermark = watermark
        self._recorder = recorder
        self._shop_id = shop_id
        self._platform = platform
        self._overlap = overlap
        self._delay = delay
        self._history_start = history_start
        self._now_fn = now_fn
        self._children_spec: EntitySpec | None = None
        self._children_loader: MySQLBatchLoader | None = None
        self._dead_letter = dead_letter
        self._dead_letter_limit = dead_letter_limit
        self._dq_engine = dq_engine
        self._alert_manager = alert_manager

    def add_child(self, spec: EntitySpec, loader: MySQLBatchLoader) -> None:
        """注册子实体(如订单明细),随主实体同批装载。"""
        self._children_spec = spec
        self._children_loader = loader

    def extract(self, *, run_type: str = "incremental", window_override: TimeWindow | None = None) -> ExtractionResult:
        """执行一轮抽取。失败时记录 failed 批次 + error 级告警后重新抛出(水位不动)。"""
        now = self._now_fn()
        watermark_value = (
            None if run_type == "full" else self._watermark.read(self._entity.table_name, self._shop_id, self._platform)
        )
        window = window_override or compute_window(
            watermark_value,
            now,
            overlap=self._overlap,
            delay=self._delay,
            history_start=self._history_start,
        )
        batch_id = self._recorder.start_batch(
            table_name=self._entity.table_name,
            shop_id=self._shop_id,
            platform=self._platform,
            run_type=run_type,
            window_start=window.start,
            window_end=window.end,
        )
        rows_read = 0
        dead_letters = 0
        try:
            rows = self._fetch_all(window)
            rows_read = len(rows)
            deduped = dedup_by_pk(rows, self._entity.pk_columns)

            mapped, dead = self._map_with_dead_letter(deduped, self._entity, batch_id)
            dead_letters += dead
            mapped, bad = self._apply_contract(mapped, self._entity, batch_id)
            dead_letters += bad
            self._check_dead_letter_limit(dead_letters, rows_read)
            rows_written = self._loader.upsert(mapped)

            if self._children_spec is not None and self._children_loader is not None:
                children, child_dead = self._map_children_with_dead_letter(deduped, batch_id)
                dead_letters += child_dead
                children, child_bad = self._apply_contract(children, self._children_spec, batch_id)
                dead_letters += child_bad
                self._check_dead_letter_limit(dead_letters, rows_read)
                self._children_loader.upsert(children)

            # 装载后 DQ(数据已 commit,全表规则可见本批数据;子实体同批同窗口,一并检查)
            if self._dq_engine is not None:
                report = self._dq_engine.run(
                    table_name=self._entity.table_name,
                    batch_id=batch_id,
                    shop_id=self._shop_id,
                    platform=self._platform,
                    run_type=run_type,
                    rows_read=rows_read,
                    now=now,
                    window_start=window.start,
                    window_end=window.end,
                )
                if self._children_spec is not None:
                    child_report = self._dq_engine.run(
                        table_name=self._children_spec.table_name,
                        batch_id=batch_id,
                        shop_id=self._shop_id,
                        platform=self._platform,
                        run_type=run_type,
                        rows_read=rows_read,
                        now=now,
                        window_start=window.start,
                        window_end=window.end,
                    )
                    report = DQReport(outcomes=[*report.outcomes, *child_report.outcomes])
                self._alert_dq_warnings(report)
                if report.blocked:
                    raise DQBlockedError(report)
        except Exception as exc:
            self._recorder.finish_batch(batch_id, status="failed", rows_read=rows_read, error_msg=str(exc))
            self._alert(f"ETL 任务失败: {self._entity.table_name}", str(exc), level="error")
            raise

        # 两阶段提交: 全部成功才推进水位;回填/全量模式不推进(窗口显式给定)
        watermark_advanced = False
        if run_type == "incremental":
            watermark_advanced = self._watermark.advance(
                self._entity.table_name, self._shop_id, self._platform, window.end
            )

        self._recorder.finish_batch(
            batch_id,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
        )
        logger.info(
            "extract.done",
            table=self._entity.table_name,
            shop_id=self._shop_id,
            run_type=run_type,
            window_start=window.start.isoformat(),
            window_end=window.end.isoformat(),
            rows_read=rows_read,
            rows_written=rows_written,
            dead_letters=dead_letters,
            watermark_advanced=watermark_advanced,
        )
        return ExtractionResult(
            table_name=self._entity.table_name,
            batch_id=batch_id,
            run_type=run_type,
            window=window,
            rows_read=rows_read,
            rows_written=rows_written,
            watermark_advanced=watermark_advanced,
            dead_letters=dead_letters,
        )

    def _fetch_all(self, window: TimeWindow) -> list[dict[str, Any]]:
        """按分页策略完整拉取窗口内全部订单。"""
        rows: list[dict[str, Any]] = []
        params: dict[str, Any] | None = self._paginator.initial_params()
        page_count = 0
        while params is not None:
            page = self._adapter.fetch_orders_page(window.start, window.end, params)
            rows.extend(page["orders"])
            params = self._paginator.next_params(page)
            page_count += 1
        logger.info(
            "extract.fetch_pages",
            table=self._entity.table_name,
            pages=page_count,
            rows=len(rows),
        )
        return rows

    # ---- 死信/契约 ----

    def _map_with_dead_letter(
        self, rows: Sequence[dict[str, Any]], spec: EntitySpec, batch_id: str
    ) -> tuple[list[dict[str, Any]], int]:
        """逐行映射,失败行进死信(不中断);返回 (好行, 死信数)。"""
        mapped: list[dict[str, Any]] = []
        dead = 0
        for row in rows:
            try:
                mapped.append(spec.mapper(row, batch_id=batch_id, platform=self._platform))
            except Exception as exc:
                self._record_dead_letters(
                    batch_id,
                    spec.table_name,
                    [DeadLetterRow(raw_row=row, error_type="mapper_error", error_msg=str(exc))],
                )
                dead += 1
                logger.warning(
                    "extract.mapper_error",
                    table=spec.table_name,
                    error=str(exc),
                )
        return mapped, dead

    def _map_children_with_dead_letter(
        self, rows: Sequence[dict[str, Any]], batch_id: str
    ) -> tuple[list[dict[str, Any]], int]:
        """子实体映射(父行 -> 子行列表),失败以父行落死信。"""
        spec = self._children_spec
        assert spec is not None
        children: list[dict[str, Any]] = []
        dead = 0
        for row in rows:
            try:
                child_rows = spec.mapper(row, batch_id=batch_id, platform=self._platform)
                children.extend(cast("list[dict[str, Any]]", child_rows))
            except Exception as exc:
                self._record_dead_letters(
                    batch_id,
                    spec.table_name,
                    [DeadLetterRow(raw_row=row, error_type="mapper_error", error_msg=str(exc))],
                )
                dead += 1
        return children, dead

    def _apply_contract(
        self, rows: list[dict[str, Any]], spec: EntitySpec, batch_id: str
    ) -> tuple[list[dict[str, Any]], int]:
        """装载前契约检查: 坏行进死信(schema_mismatch),好行继续。"""
        if spec.contract is None:
            return rows, 0
        result = spec.contract.validate(rows)
        if result.bad_rows:
            self._record_dead_letters(
                batch_id,
                spec.table_name,
                [
                    DeadLetterRow(raw_row=row, error_type="schema_mismatch", error_msg=msg)
                    for row, msg in result.bad_rows
                ],
            )
        return result.good_rows, result.bad_count

    def _record_dead_letters(self, batch_id: str, table_name: str, rows: Sequence[DeadLetterRow]) -> None:
        if self._dead_letter is not None:
            self._dead_letter.record_many(batch_id=batch_id, table_name=table_name, rows=rows)

    def _check_dead_letter_limit(self, dead: int, rows_read: int) -> None:
        if self._dead_letter_limit > 0 and rows_read > 0 and dead / rows_read > self._dead_letter_limit:
            raise DeadLetterLimitExceeded(
                f"死信 {dead}/{rows_read}({dead / rows_read:.2%}) 超过阈值 {self._dead_letter_limit:.2%}"
            )

    # ---- DQ / 告警 ----

    def _alert_dq_warnings(self, report: DQReport) -> None:
        for outcome in report.warn_failures():
            text = f"规则 {outcome.rule_type} 未通过: actual={outcome.actual}, expected={outcome.expected}" + (
                f"; {outcome.detail}" if outcome.detail else ""
            )
            self._alert(f"DQ 警告: {self._entity.table_name}", text, level="warning")

    def _alert(self, title: str, text: str, *, level: str) -> None:
        if self._alert_manager is not None:
            self._alert_manager.send(f"[{self._platform}/shop={self._shop_id}] {title}", text, level=level)
