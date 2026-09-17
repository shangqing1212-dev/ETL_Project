"""模拟 OAuth2 client_credentials: 签发 2h 过期 token(进程内存储,单 worker 专用)。"""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel

from mock_api.config import get_settings

router = APIRouter(tags=["auth"])

_tokens: dict[str, float] = {}  # token -> expires_at(epoch 秒)


class TokenRequest(BaseModel):
    grant_type: str = "client_credentials"
    client_id: str
    client_secret: str


@router.post("/token")
def issue_token(req: TokenRequest) -> dict[str, str | int]:
    settings = get_settings()
    if req.grant_type != "client_credentials":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "unsupported grant_type")
    if req.client_id != settings.client_id or req.client_secret != settings.client_secret:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid client credentials")
    token = uuid.uuid4().hex
    _tokens[token] = time.time() + settings.token_ttl_seconds
    return {"access_token": token, "token_type": "bearer", "expires_in": settings.token_ttl_seconds}


def verify_token(authorization: str = Header(...)) -> None:
    """FastAPI 依赖: 校验 Authorization: Bearer <token>。"""
    token = authorization.removeprefix("Bearer ").strip()
    expires_at = _tokens.get(token)
    if expires_at is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token")
    if time.time() > expires_at:
        _tokens.pop(token, None)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token expired")
