"""增量抽取编排: 窗口固定 -> 翻页抽取 -> 批内去重 -> 映射 -> 分批装载 -> 两阶段推进水位。

核心不变量: 重跑同一窗口得到相同结果(幂等)。
- 窗口在批开始固定 [wm - overlap, now - delay),批内不取 "now"
- 全窗口成功且无异常才推进水位;失败水位不动,重跑自动重读同窗口
- 回填/全量模式不推进水位(窗口显式给定,upsert 保证幂等)
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from structlog import get_logger

from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import Paginator
from etl_sdk.extractors.state import WatermarkState
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
    """一个实体(表)的装载配置: 目标表 + 列 + 主键 + 行映射。"""

    table_name: str
    columns: Sequence[str]
    pk_columns: Sequence[str]
    # 行映射: (源行, batch_id=..., platform=...) -> 目标行
    mapper: Callable[..., dict[str, Any]]
    # items 型子实体: 父行 -> 子行列表(如订单明细)
    children_mapper: Callable[..., list[dict[str, Any]]] | None = None


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

    def add_child(self, spec: EntitySpec, loader: MySQLBatchLoader) -> None:
        """注册子实体(如订单明细),随主实体同批装载。"""
        self._children_spec = spec
        self._children_loader = loader

    def extract(self, *, run_type: str = "incremental", window_override: TimeWindow | None = None) -> ExtractionResult:
        """执行一轮抽取。失败时记录 failed 批次后重新抛出(水位不动)。"""
        watermark_value = (
            None if run_type == "full" else self._watermark.read(self._entity.table_name, self._shop_id, self._platform)
        )
        window = window_override or compute_window(
            watermark_value,
            self._now_fn(),
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
        try:
            rows = self._fetch_all(window)
            rows_read = len(rows)
            deduped = dedup_by_pk(rows, self._entity.pk_columns)
            mapped = [self._entity.mapper(r, batch_id=batch_id, platform=self._platform) for r in deduped]
            rows_written = self._loader.upsert(mapped)

            if self._children_spec is not None and self._children_loader is not None:
                children: list[dict[str, Any]] = []
                for r in deduped:
                    child_rows = self._children_spec.mapper(r, batch_id=batch_id, platform=self._platform)
                    # 子实体规格的 mapper 约定返回子行列表(见 EntitySpec 注释)
                    children.extend(cast("list[dict[str, Any]]", child_rows))
                self._children_loader.upsert(children)
        except Exception as exc:
            self._recorder.finish_batch(batch_id, status="failed", rows_read=rows_read, error_msg=str(exc))
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
