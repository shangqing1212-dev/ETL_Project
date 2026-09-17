"""告警模块单测: 钉钉加签、级别过滤、通道故障隔离、按配置组装通道。"""

from __future__ import annotations

import base64
import hashlib
import hmac
import urllib.parse

import httpx
import pytest
from etl_sdk.alerts import AlertManager, DingTalkChannel, build_alert_manager
from etl_sdk.alerts.base import AlertChannel
from etl_sdk.alerts.dingtalk import build_signed_url
from etl_sdk.config import AlertSettings


class StubChannel(AlertChannel):
    """记录收到的告警供断言。"""

    name = "stub"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def send(self, title: str, text: str, *, level: str) -> None:
        self.calls.append((title, text, level))


class FailingChannel(AlertChannel):
    name = "failing"

    def send(self, title: str, text: str, *, level: str) -> None:
        raise RuntimeError("boom")


def test_dingtalk_signed_url_matches_official_algorithm() -> None:
    secret = "SEC000000000000000000000"
    ts = 1700000000000
    url = build_signed_url("https://oapi.dingtalk.com/robot/send?access_token=abc", secret, timestamp_ms=ts)
    string_to_sign = f"{ts}\n{secret}"
    expected_sign = urllib.parse.quote_plus(
        base64.b64encode(hmac.new(secret.encode(), string_to_sign.encode(), hashlib.sha256).digest())
    )
    assert url == f"https://oapi.dingtalk.com/robot/send?access_token=abc&timestamp={ts}&sign={expected_sign}"


def test_dingtalk_url_unchanged_without_secret() -> None:
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=abc"
    assert build_signed_url(webhook, "") == webhook


def test_dingtalk_channel_business_error_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "timestamp=" in str(request.url)  # 请求必须走加签 URL
        return httpx.Response(200, json={"errcode": 1, "errmsg": "bad"})

    channel = DingTalkChannel("https://x", "s", client=httpx.Client(transport=httpx.MockTransport(handler)))
    with pytest.raises(RuntimeError, match="errcode=1"):
        channel.send("t", "text", level="error")
    channel.close()


def test_dingtalk_channel_success() -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"errcode": 0}))
    channel = DingTalkChannel("https://x", "s", client=httpx.Client(transport=transport))
    channel.send("t", "text", level="error")  # 不抛异常即成功
    channel.close()


def test_manager_level_filtering() -> None:
    stub = StubChannel()
    mgr = AlertManager([stub], min_level="error")
    mgr.send("t", "text", level="warning")
    assert stub.calls == []
    mgr.send("t", "text", level="error")
    assert [(t, lv) for t, _, lv in stub.calls] == [("t", "error")]


def test_manager_disabled_is_silent() -> None:
    stub = StubChannel()
    AlertManager([stub], enabled=False).send("t", "text", level="critical")
    assert stub.calls == []


def test_channel_failure_isolation() -> None:
    stub = StubChannel()
    mgr = AlertManager([FailingChannel(), stub])
    mgr.send("t", "text", level="error")  # 失败通道不阻断其他通道
    assert len(stub.calls) == 1


def test_build_alert_manager_selects_configured_channels() -> None:
    settings = AlertSettings(_env_file=None, wecom_webhook="https://wecom/x")
    assert [c.name for c in build_alert_manager(settings).channels] == ["wecom"]


def test_build_alert_manager_falls_back_to_log_channel() -> None:
    settings = AlertSettings(_env_file=None)
    assert [c.name for c in build_alert_manager(settings).channels] == ["log"]


def test_build_alert_manager_orders_channels() -> None:
    settings = AlertSettings(
        _env_file=None,
        dingtalk_webhook="https://ding/x",
        wecom_webhook="https://wecom/x",
        smtp_host="smtp.x",
        smtp_from="a@x",
        smtp_to="b@x",
    )
    assert [c.name for c in build_alert_manager(settings).channels] == ["dingtalk", "wecom", "email"]
