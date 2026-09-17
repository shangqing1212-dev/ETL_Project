"""钉钉自定义机器人通道(安全设置: 加签)。

加签算法(官方): sign = urlencode(base64(hmac_sha256(secret, f"{timestamp}\\n{secret}"))),
timestamp 为毫秒时间戳,拼接在 webhook URL 上。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import urllib.parse

import httpx

from etl_sdk.alerts.base import AlertChannel


def build_signed_url(webhook: str, secret: str, *, timestamp_ms: int | None = None) -> str:
    """按钉钉官方算法生成加签 URL(secret 为空时原样返回,便于单测注入固定时间戳)。"""
    if not secret:
        return webhook
    ts = str(timestamp_ms if timestamp_ms is not None else round(time.time() * 1000))
    string_to_sign = f"{ts}\n{secret}"
    digest = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(digest))
    sep = "&" if "?" in webhook else "?"
    return f"{webhook}{sep}timestamp={ts}&sign={sign}"


class DingTalkChannel(AlertChannel):
    """钉钉群机器人 webhook,消息类型 markdown。"""

    name = "dingtalk"

    def __init__(
        self,
        webhook: str,
        secret: str = "",
        *,
        client: httpx.Client | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._webhook = webhook
        self._secret = secret
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def send(self, title: str, text: str, *, level: str) -> None:
        payload = {"msgtype": "markdown", "markdown": {"title": title, "text": text}}
        resp = self._client.post(build_signed_url(self._webhook, self._secret), json=payload)
        resp.raise_for_status()
        data = resp.json()
        # 钉钉 API 约定: HTTP 200 但 errcode != 0 仍为业务失败
        if data.get("errcode") != 0:
            raise RuntimeError(f"dingtalk errcode={data.get('errcode')} errmsg={data.get('errmsg')}")

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
