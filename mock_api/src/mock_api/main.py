"""模拟电商平台 API 入口。已含: 鉴权、双分页、真实限流、故障注入开关。"""

from __future__ import annotations

from fastapi import FastAPI

from mock_api.auth import router as auth_router
from mock_api.config import get_settings
from mock_api.faults import FaultInjectorMiddleware
from mock_api.faults import router as admin_router
from mock_api.rate_limiter import RateLimitMiddleware
from mock_api.routers.orders import router as orders_router


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Mock E-commerce Platform API", version="0.2.0")
    app.add_middleware(
        RateLimitMiddleware,
        rate_per_min=settings.rate_limit_orders,
        burst=settings.rate_limit_burst,
    )
    app.add_middleware(FaultInjectorMiddleware, mode=settings.fault_mode)
    app.include_router(auth_router)
    app.include_router(orders_router)
    app.include_router(admin_router)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
