import os
import sqlite3
import tempfile
import unittest
from contextlib import closing

from backend.database_export import create_sqlite_snapshot, remove_file


class DatabaseExportTest(unittest.TestCase):
    def test_snapshot_is_consistent_and_independent(self) -> None:
        fd, source_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        snapshot_path = ""
        try:
            with closing(sqlite3.connect(source_path)) as db:
                db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                db.execute("INSERT INTO events (name) VALUES ('purchase')")
                db.commit()

            snapshot_path = create_sqlite_snapshot(source_path)

            with closing(sqlite3.connect(source_path)) as source_db:
                source_db.execute("INSERT INTO events (name) VALUES ('lead')")
                source_db.commit()
            with closing(sqlite3.connect(snapshot_path)) as snapshot_db:
                rows = snapshot_db.execute("SELECT name FROM events ORDER BY id").fetchall()

            self.assertEqual(rows, [("purchase",)])
        finally:
            remove_file(snapshot_path)
            remove_file(source_path)


if __name__ == "__main__":
    unittest.main()
