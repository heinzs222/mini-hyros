"""Shared pytest fixtures.

The whole application reads from a single SQLite database addressed by path, so
the core fixture here builds a *fresh, schema-correct* temporary database for
each test. Tests then seed exactly the rows they need (see ``tests.helpers``),
which keeps golden-value assertions deterministic and easy to reason about.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

# Pin the reporting timezone to UTC for tests so golden-value date-range
# assertions stay deterministic regardless of the host zone. Production defaults
# to America/Toronto (see attributionops.util.report_timezone).
os.environ.setdefault("REPORT_TIMEZONE", "UTC")

# Disable the short-lived report cache in tests so each request recomputes
# against the test's freshly-seeded DB (no cross-test or within-test staleness).
os.environ.setdefault("REPORT_CACHE_TTL", "0")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_INIT_SCRIPT = ROOT / "scripts" / "init_empty_db.py"


def _load_init_db():
    spec = importlib.util.spec_from_file_location("_init_empty_db", _INIT_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.init_db


init_schema = _load_init_db()


@pytest.fixture
def empty_db(tmp_path) -> str:
    """Path to a fresh SQLite DB with the production schema and no rows."""
    db_path = tmp_path / "attributionops_test.sqlite"
    init_schema(str(db_path))
    return str(db_path)
