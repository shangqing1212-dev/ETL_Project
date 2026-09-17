"""抽取层: 水位线增量流程、分页策略、令牌桶限流、批次记录。"""

from etl_sdk.extractors.base import BaseExtractor, EntitySpec, ExtractionResult, TimeWindow, compute_window, dedup_by_pk
from etl_sdk.extractors.batches import BatchRecorder
from etl_sdk.extractors.pagination import CursorPaginator, PagePaginator, Paginator
from etl_sdk.extractors.rate_limit import AdaptiveRateLimiter, TokenBucket
from etl_sdk.extractors.state import WatermarkState

__all__ = [
    "BaseExtractor",
    "EntitySpec",
    "ExtractionResult",
    "TimeWindow",
    "compute_window",
    "dedup_by_pk",
    "BatchRecorder",
    "CursorPaginator",
    "PagePaginator",
    "Paginator",
    "AdaptiveRateLimiter",
    "TokenBucket",
    "WatermarkState",
]
