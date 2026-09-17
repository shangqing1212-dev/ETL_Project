"""告警通道抽象与 AlertManager: 多通道分发,单通道失败隔离。

级别(info < warning < error < critical)在 AlertManager 统一过滤,
低于 min_level 的告警静默丢弃 —— 避免 DQ warn 类低级别事件刷屏。
"""

from __future__ import annotations

import abc
from collections.abc import Sequence

from structlog import get_logger

logger = get_logger(__name__)

_LEVEL_ORDER = {"info": 0, "warning": 1, "error": 2, "critical": 3}


class AlertChannel(abc.ABC):
    """单条告警通道。send 为阻塞式(生产量级下告警频率低,无需异步)。"""

    name: str = "channel"

    @abc.abstractmethod
    def send(self, title: str, text: str, *, level: str) -> None:
        """发送一条告警;失败由 AlertManager 隔离(记日志,不影响其他通道)。"""


class LogChannel(AlertChannel):
    """开发环境兜底通道: 告警写入 structlog(error 级,dev console / prod JSON 均可见)。

    生产环境应配置真实 webhook/SMTP;全部未配置时自动挂载本通道保证可观测。
    """

    name = "log"

    def send(self, title: str, text: str, *, level: str) -> None:
        logger.error("alert.log_channel", title=title, text=text, level=level)


class AlertManager:
    """按级别过滤后分发到所有启用的通道;单通道异常不阻断其余通道。"""

    def __init__(
        self,
        channels: Sequence[AlertChannel],
        *,
        enabled: bool = True,
        min_level: str = "warning",
    ) -> None:
        self._channels = list(channels)
        self._enabled = enabled
        self._min_level = min_level

    @property
    def channels(self) -> list[AlertChannel]:
        """当前启用的通道列表(副本)。"""
        return list(self._channels)

    def send(self, title: str, text: str, *, level: str = "error") -> None:
        if not self._enabled:
            return
        if _LEVEL_ORDER.get(level, 0) < _LEVEL_ORDER.get(self._min_level, 0):
            return
        for channel in self._channels:
            try:
                channel.send(title, text, level=level)
            except Exception:
                logger.exception("alert.channel_failed", channel=channel.name)
