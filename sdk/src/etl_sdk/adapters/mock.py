"""mock 平台适配器: 对接本地模拟电商 API(协议与真实平台一致,含 OAuth2 鉴权)。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from etl_sdk.adapters.base import BaseHTTPAdapter


class MockPlatformAdapter(BaseHTTPAdapter):
    platform = "mock"

    def get_token(self) -> tuple[str, int]:
        response = self._client.post(
            "/token",
            json={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["access_token"], int(data["expires_in"])

    def fetch_orders_page(
        self,
        window_start: datetime,
        window_end: datetime,
        page_params: dict[str, Any],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "updated_start": window_start.isoformat(),
            "updated_end": window_end.isoformat(),
            **page_params,
        }
        return self._request("/orders", params=params)
