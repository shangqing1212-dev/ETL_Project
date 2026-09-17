"""structlog 配置: dev 用 console 彩色输出,prod 用 JSON(可进 Loki/filebeat)。"""

from __future__ import annotations

import logging

import structlog
from structlog.types import Processor


def setup_logging(level: str = "INFO", *, json_output: bool = False) -> None:
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        # utc=False: 与数仓本地时区存储(ADR-001)保持一致,便于对照排查
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]
    renderer = structlog.processors.JSONRenderer(ensure_ascii=False) if json_output else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[*shared_processors, structlog.processors.StackInfoRenderer(), renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )
