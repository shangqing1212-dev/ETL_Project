"""模拟电商平台 API 入口。M2/M3 将加入: 游标分页、故障注入中间件、店铺/商品接口。"""

from __future__ import annotations

from fastapi import FastAPI

from mock_api.auth import router as auth_router
from mock_api.config import get_settings
from mock_api.rate_limiter import RateLimitMiddleware
from mock_api.routers.orders import router as orders_router


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Mock E-commerce Platform API", version="0.1.0")
    app.add_middleware(
        RateLimitMiddleware,
        rate_per_min=settings.rate_limit_orders,
        burst=settings.rate_limit_burst,
    )
    app.include_router(auth_router)
    app.include_router(orders_router)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
