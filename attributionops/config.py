from __future__ import annotations

import os
from pathlib import Path

from attributionops.db import is_postgres_dsn


DEFAULT_SQLITE_DB_PATH = str(Path("data/dummy/attributionops_demo.sqlite"))


def default_db_path() -> str:
    for key in ("SUPABASE_DB_URL", "DATABASE_URL"):
        value = os.environ.get(key, "").strip()
        if is_postgres_dsn(value):
            return value
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", "").strip() or DEFAULT_SQLITE_DB_PATH
