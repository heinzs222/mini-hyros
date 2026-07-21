from __future__ import annotations

import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


def remove_file(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def create_sqlite_snapshot(source_path: str) -> str:
    source = Path(source_path).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)

    fd, snapshot_path = tempfile.mkstemp(prefix="mini-hyros-", suffix=".sqlite")
    os.close(fd)
    try:
        with closing(sqlite3.connect(str(source))) as source_db, closing(
            sqlite3.connect(snapshot_path)
        ) as snapshot_db:
            source_db.backup(snapshot_db)

        with closing(sqlite3.connect(snapshot_path)) as check_db:
            result = check_db.execute("PRAGMA integrity_check").fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError("SQLite snapshot integrity check failed")
        return snapshot_path
    except Exception:
        remove_file(snapshot_path)
        raise
