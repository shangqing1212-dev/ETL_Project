"""企业微信群机器人通道(无需加签,webhook key 即凭据)。"""

from __future__ import annotations

import httpx

from etl_sdk.alerts.base import AlertChannel


class WeComChannel(AlertChannel):
    name = "wecom"

    def __init__(self, webhook: str, *, client: httpx.Client | None = None, timeout: float = 10.0) -> None:
        self._webhook = webhook
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def send(self, title: str, text: str, *, level: str) -> None:
        payload = {"msgtype": "markdown", "markdown": {"content": f"**{title}**\n{text}"}}
        resp = self._client.post(self._webhook, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"wecom errcode={data.get('errcode')} errmsg={data.get('errmsg')}")

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
