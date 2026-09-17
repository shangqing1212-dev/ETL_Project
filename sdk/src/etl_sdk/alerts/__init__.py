"""告警模块: 三通道(钉钉加签/企业微信/邮件)+ 开发兜底日志通道。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from etl_sdk.alerts.base import AlertChannel, AlertManager, LogChannel
from etl_sdk.alerts.dingtalk import DingTalkChannel
from etl_sdk.alerts.email import EmailChannel
from etl_sdk.alerts.wecom import WeComChannel

if TYPE_CHECKING:
    from etl_sdk.config import AlertSettings

__all__ = [
    "AlertChannel",
    "AlertManager",
    "DingTalkChannel",
    "EmailChannel",
    "LogChannel",
    "WeComChannel",
    "build_alert_manager",
]


def build_alert_manager(settings: AlertSettings) -> AlertManager:
    """按配置组装通道: webhook/SMTP 未配置的通道自动跳过。

    全部通道均未配置时挂载 LogChannel 兜底 —— dev 环境告警进日志,
    生产环境配置任一真实通道即自动替换。
    """
    channels: list[AlertChannel] = []
    if settings.dingtalk_webhook:
        channels.append(DingTalkChannel(settings.dingtalk_webhook, settings.dingtalk_secret))
    if settings.wecom_webhook:
        channels.append(WeComChannel(settings.wecom_webhook))
    if settings.smtp_host and settings.smtp_from and settings.smtp_to:
        channels.append(
            EmailChannel(
                host=settings.smtp_host,
                port=settings.smtp_port,
                user=settings.smtp_user,
                password=settings.smtp_password,
                sender=settings.smtp_from,
                recipients=[addr.strip() for addr in settings.smtp_to.split(",") if addr.strip()],
            )
        )
    if not channels:
        channels.append(LogChannel())
    return AlertManager(channels, enabled=settings.enabled, min_level=settings.min_level)
