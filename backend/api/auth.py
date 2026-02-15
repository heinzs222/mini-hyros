"""Simple fixed-credential auth (login only, no registration)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket
from pydantic import BaseModel

router = APIRouter()

AUTH_COOKIE_NAME = "hyros_auth"
TOKEN_TTL_SECONDS_DEFAULT = 12 * 60 * 60


class LoginRequest(BaseModel):
    username: str
    password: str


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _auth_username() -> str:
    return os.environ.get("AUTH_USERNAME", "").strip()


def _auth_password() -> str:
    return os.environ.get("AUTH_PASSWORD", "")


def _auth_secret() -> str:
    return os.environ.get("AUTH_SECRET_KEY", "").strip()


def _token_ttl_seconds() -> int:
    try:
        hours = int(os.environ.get("AUTH_SESSION_TTL_HOURS", "12"))
        if hours <= 0:
            return TOKEN_TTL_SECONDS_DEFAULT
        return hours * 60 * 60
    except ValueError:
        return TOKEN_TTL_SECONDS_DEFAULT


def is_auth_enabled() -> bool:
    return _bool_env("AUTH_ENABLED", default=False)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _sign(payload_part: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def issue_token(username: str) -> str:
    secret = _auth_secret()
    if not secret:
        raise HTTPException(status_code=500, detail="AUTH_SECRET_KEY is required when AUTH_ENABLED=true")

    now = int(time.time())
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + _token_ttl_seconds(),
    }
    payload_part = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = _sign(payload_part, secret)
    return f"{payload_part}.{signature}"


def validate_token(token: str) -> dict[str, Any] | None:
    token = str(token or "").strip()
    if not token or "." not in token:
        return None

    payload_part, sig_part = token.split(".", 1)
    secret = _auth_secret()
    if not secret:
        return None

    expected_sig = _sign(payload_part, secret)
    if not hmac.compare_digest(sig_part, expected_sig):
        return None

    try:
        payload_raw = _b64url_decode(payload_part)
        payload = json.loads(payload_raw.decode("utf-8"))
    except Exception:
        return None

    now = int(time.time())
    exp = int(payload.get("exp", 0) or 0)
    if exp <= now:
        return None

    subject = str(payload.get("sub", "")).strip()
    if not subject:
        return None

    return payload


def extract_request_token(request: Request) -> str:
    auth_header = str(request.headers.get("authorization", "")).strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    cookie_token = request.cookies.get(AUTH_COOKIE_NAME)
    if cookie_token:
        return str(cookie_token).strip()

    return ""


def extract_websocket_token(ws: WebSocket) -> str:
    auth_header = str(ws.headers.get("authorization", "")).strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    qs_token = ws.query_params.get("token")
    if qs_token:
        return str(qs_token).strip()

    cookie_header = str(ws.headers.get("cookie", ""))
    if cookie_header:
        pieces = [p.strip() for p in cookie_header.split(";") if p.strip()]
        for p in pieces:
            if p.startswith(f"{AUTH_COOKIE_NAME}="):
                return p.split("=", 1)[1].strip()

    return ""


def _set_auth_cookie(response: Response, token: str) -> None:
    secure = _bool_env("AUTH_COOKIE_SECURE", default=False)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=secure,
        samesite="lax",
        max_age=_token_ttl_seconds(),
        path="/",
    )


def _clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(key=AUTH_COOKIE_NAME, path="/")


@router.post("/login")
async def login(body: LoginRequest, response: Response):
    if not is_auth_enabled():
        raise HTTPException(status_code=400, detail="Auth is disabled")

    expected_username = _auth_username()
    expected_password = _auth_password()
    if not expected_username or not expected_password:
        raise HTTPException(status_code=500, detail="AUTH_USERNAME and AUTH_PASSWORD are required when AUTH_ENABLED=true")

    username = str(body.username or "").strip()
    password = str(body.password or "")

    username_ok = hmac.compare_digest(username, expected_username)
    password_ok = hmac.compare_digest(password, expected_password)
    if not (username_ok and password_ok):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = issue_token(username)
    _set_auth_cookie(response, token)
    return {
        "ok": True,
        "auth_enabled": True,
        "user": {"username": username},
        "token": token,
        "expires_in": _token_ttl_seconds(),
    }


@router.post("/logout")
async def logout(response: Response):
    _clear_auth_cookie(response)
    return {"ok": True}


@router.get("/me")
async def me(request: Request):
    if not is_auth_enabled():
        return {"authenticated": True, "auth_enabled": False, "user": None}

    token = extract_request_token(request)
    payload = validate_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {
        "authenticated": True,
        "auth_enabled": True,
        "user": {"username": payload.get("sub", "")},
        "exp": payload.get("exp"),
    }
