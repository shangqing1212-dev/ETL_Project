"""故障注入: 中间件 + /admin/faults 运行时开关。

模式(仅作用于 /orders):
- none            不注入
- http_500        每 2 个请求失败 1 次(500)
- rate_limit_429  每 2 个请求返回 1 次 429(带 Retry-After)
- timeout         每 2 个请求有 1 次延迟 40s(超出客户端超时)
- malformed_json  每 2 个请求有 1 次返回非法 JSON
- cursor_reset    游标分页翻到第 3 页时把 next_cursor 重置回起点(一次性,模拟平台游标故障)

单 worker 进程内状态,仅供开发/测试锻炼 SDK 的重试与幂等路径。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

from fastapi import APIRouter, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

VALID_MODES = {"none", "http_500", "rate_limit_429", "timeout", "malformed_json", "cursor_reset"}

# timeout 模式注入的延迟秒数;生产默认 40s(超出客户端超时),E2E 缩短以控制测试时长
FAULT_TIMEOUT_SECONDS = 40.0

_state: dict[str, str] = {"mode": "none"}
_counter = 0
_cursor_reset_done = False


def set_fault(mode: str) -> None:
    if mode not in VALID_MODES:
        raise ValueError(f"unknown fault mode {mode!r}")
    global _counter, _cursor_reset_done
    _state["mode"] = mode
    _counter = 0
    _cursor_reset_done = False


def get_fault() -> str:
    return _state["mode"]


class FaultInjectorMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, mode: str = "none") -> None:
        super().__init__(app)
        set_fault(mode)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not request.url.path.startswith("/orders"):
            return await call_next(request)
        mode = _state["mode"]
        if mode == "none":
            return await call_next(request)

        global _counter, _cursor_reset_done
        _counter += 1

        if mode == "http_500" and _counter % 2 == 0:
            return JSONResponse(status_code=500, content={"code": 500, "msg": "injected fault: http_500"})
        if mode == "rate_limit_429" and _counter % 2 == 0:
            return JSONResponse(
                status_code=429,
                content={"code": 429, "msg": "injected fault: rate_limit_429"},
                headers={"Retry-After": "1"},
            )
        if mode == "timeout" and _counter % 2 == 1:
            await asyncio.sleep(FAULT_TIMEOUT_SECONDS)
            return await call_next(request)
        if mode == "malformed_json" and _counter % 2 == 0:
            return Response(content=b"{broken json", media_type="application/json", status_code=200)

        if mode == "cursor_reset":
            response = await call_next(request)
            if "page_cursor" in request.query_params and not _cursor_reset_done and _counter >= 3:
                # 中间件拿到的实际类型是 _StreamingResponse(body_iterator),类型标注里没有
                streaming = cast("Any", response)
                raw = b"".join([chunk async for chunk in streaming.body_iterator])
                body = json.loads(raw.decode("utf-8"))
                body["next_cursor"] = "0"  # 重置回起点,制造重复页
                _cursor_reset_done = True
                return JSONResponse(body)
            return response
        return await call_next(request)


router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/faults")
def read_fault() -> dict[str, str]:
    return {"mode": get_fault()}


@router.post("/faults")
def update_fault(mode: str) -> dict[str, str]:
    try:
        set_fault(mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"mode": get_fault()}
