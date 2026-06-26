"""Fixtures for FastAPI backend tests.

The backend imports its routers as top-level ``api.*`` modules, so ``backend/``
must be importable. Each test gets its own temporary SQLite warehouse wired in
via the ``ATTRIBUTIONOPS_DB_PATH`` env var (read per-request by the app), and
auth is disabled by default so endpoints are reachable without a token.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
for _p in (str(ROOT), str(BACKEND)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _init_schema(db_path: str) -> None:
    spec = importlib.util.spec_from_file_location("_init_empty_db_api", ROOT / "scripts" / "init_empty_db.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.init_db(db_path)


@pytest.fixture
def api_db(tmp_path, monkeypatch) -> str:
    db_path = tmp_path / "api.sqlite"
    _init_schema(str(db_path))
    monkeypatch.setenv("ATTRIBUTIONOPS_DB_PATH", str(db_path))
    monkeypatch.setenv("AUTH_ENABLED", "false")  # default: auth off, even if .env enables it
    return str(db_path)


@pytest.fixture
def client(api_db):
    from fastapi.testclient import TestClient
    import main

    return TestClient(main.app)
