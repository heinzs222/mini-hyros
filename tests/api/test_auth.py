"""Tests for the auth module: token issue/validate plus the middleware + routes."""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

import api.auth as auth

SECRET = "s3cret-key"


def _make_token(secret: str, *, sub: str = "u", exp: int) -> str:
    payload = {"sub": sub, "iat": 0, "exp": exp}
    part = auth._b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return f"{part}.{auth._sign(part, secret)}"


# ── token issue / validate ────────────────────────────────────────────────────
def test_issue_and_validate_roundtrip(monkeypatch):
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    token = auth.issue_token("alice")
    payload = auth.validate_token(token)
    assert payload is not None
    assert payload["sub"] == "alice"


def test_issue_token_requires_secret(monkeypatch):
    monkeypatch.delenv("AUTH_SECRET_KEY", raising=False)
    with pytest.raises(HTTPException) as exc:
        auth.issue_token("alice")
    assert exc.value.status_code == 500


def test_validate_rejects_tampered_signature(monkeypatch):
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    token = auth.issue_token("alice")
    part, sig = token.split(".", 1)
    tampered = f"{part}.{sig[:-1]}{'A' if sig[-1] != 'A' else 'B'}"
    assert auth.validate_token(tampered) is None


def test_validate_rejects_expired(monkeypatch):
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    assert auth.validate_token(_make_token(SECRET, exp=1)) is None  # exp in the past


def test_validate_accepts_future_expiry(monkeypatch):
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    assert auth.validate_token(_make_token(SECRET, exp=9_999_999_999)) is not None


def test_validate_rejects_wrong_secret(monkeypatch):
    token = _make_token("other-secret", exp=9_999_999_999)
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    assert auth.validate_token(token) is None


@pytest.mark.parametrize("bad", ["", "no-dot", "   "])
def test_validate_rejects_malformed(monkeypatch, bad):
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)
    assert auth.validate_token(bad) is None


# ── auth routes (auth disabled by default) ────────────────────────────────────
def test_login_returns_400_when_auth_disabled(client):
    r = client.post("/api/auth/login", json={"username": "x", "password": "y"})
    assert r.status_code == 400


def test_me_reports_auth_disabled(client):
    r = client.get("/api/auth/me")
    assert r.status_code == 200
    assert r.json() == {"authenticated": True, "auth_enabled": False, "user": None}


def test_logout_ok(client):
    assert client.post("/api/auth/logout").json() == {"ok": True}


# ── auth enabled: login + middleware enforcement ──────────────────────────────
@pytest.fixture
def auth_enabled(monkeypatch):
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_USERNAME", "admin")
    monkeypatch.setenv("AUTH_PASSWORD", "hunter2")
    monkeypatch.setenv("AUTH_SECRET_KEY", SECRET)


def test_login_rejects_bad_credentials(client, auth_enabled):
    r = client.post("/api/auth/login", json={"username": "admin", "password": "wrong"})
    assert r.status_code == 401


def test_login_success_returns_token(client, auth_enabled):
    r = client.post("/api/auth/login", json={"username": "admin", "password": "hunter2"})
    assert r.status_code == 200
    assert r.json()["token"]


def test_protected_route_blocked_without_token(client, auth_enabled):
    # /api/health is public, but /api/ltv/summary is protected.
    assert client.get("/api/health").status_code == 200
    assert client.get("/api/ltv/summary").status_code == 401


def test_protected_route_allowed_with_bearer_token(client, auth_enabled):
    token = client.post("/api/auth/login", json={"username": "admin", "password": "hunter2"}).json()["token"]
    r = client.get("/api/ltv/summary", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
