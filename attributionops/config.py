from __future__ import annotations

import os
from pathlib import Path


def default_db_path() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", str(Path("data/dummy/attributionops_demo.sqlite")))

