"""Copy the Vigil SQLite warehouse into Supabase Postgres exactly once.

The migration is transactional and verifies every table's row count before
commit. It intentionally requires --replace when the target contains data.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlsplit, urlunsplit


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "supabase" / "migrations" / "202607210001_initial_schema.sql"
TABLES = (
    "spend",
    "sessions",
    "touchpoints",
    "orders",
    "conversions",
    "reported_value",
    "ad_names",
    "video_metrics",
    "campaign_settings",
    "refund_log",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-path", required=True, type=Path)
    parser.add_argument(
        "--database-url",
        default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"),
        help="Supabase Postgres URI (or set SUPABASE_DB_URL).",
    )
    parser.add_argument("--schema-file", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace all target warehouse rows in the same transaction.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect the SQLite source without connecting to Supabase.",
    )
    return parser.parse_args()


def quote_identifier(value: str) -> str:
    if value not in TABLES:
        raise ValueError(f"Unexpected table name: {value}")
    return f'"{value}"'


def source_tables(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def source_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table)})").fetchall()
    return [str(row[1]) for row in rows]


def source_count(connection: sqlite3.Connection, table: str) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(table)}"
    ).fetchone()
    return int(row[0])


def source_rows(
    connection: sqlite3.Connection, table: str, batch_size: int
) -> Iterable[list[tuple[object, ...]]]:
    cursor = connection.execute(f"SELECT * FROM {quote_identifier(table)}")
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            return
        yield [tuple(row) for row in batch]


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    index = 0
    while index < len(sql_text):
        char = sql_text[index]
        if char == "'":
            current.append(char)
            if in_single_quote and index + 1 < len(sql_text) and sql_text[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif char == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        index += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def redacted_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        username = parsed.username or "postgres"
        return urlunsplit((parsed.scheme, f"{username}:***@{hostname}{port}", parsed.path, "", ""))
    except ValueError:
        return "<redacted database URL>"


def validate_source(connection: sqlite3.Connection) -> dict[str, int]:
    available = source_tables(connection)
    missing = [table for table in TABLES if table not in available]
    if missing:
        raise RuntimeError(f"SQLite source is missing tables: {', '.join(missing)}")
    return {table: source_count(connection, table) for table in TABLES}


def execute_schema(postgres_connection: object, schema_file: Path) -> None:
    schema_text = schema_file.read_text(encoding="utf-8")
    for statement in split_sql_statements(schema_text):
        postgres_connection.execute(statement)


def target_counts(postgres_connection: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in TABLES:
        row = postgres_connection.execute(
            f"SELECT COUNT(*) FROM public.{quote_identifier(table)}"
        ).fetchone()
        counts[table] = int(row[0])
    return counts


def target_columns(postgres_connection: object, table: str) -> set[str]:
    rows = postgres_connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    ).fetchall()
    return {str(row[0]) for row in rows}


def validate_target_schema(
    sqlite_connection: sqlite3.Connection,
    postgres_connection: object,
) -> None:
    mismatches: list[str] = []
    for table in TABLES:
        source = set(source_columns(sqlite_connection, table))
        target = target_columns(postgres_connection, table)
        missing = sorted(source - target)
        if missing:
            mismatches.append(f"{table}: {', '.join(missing)}")
    if mismatches:
        raise RuntimeError(
            "Supabase schema is missing SQLite source columns: " + "; ".join(mismatches)
        )


def insert_table(
    sqlite_connection: sqlite3.Connection,
    postgres_connection: object,
    table: str,
    batch_size: int,
) -> int:
    columns = source_columns(sqlite_connection, table)
    if not columns:
        raise RuntimeError(f"SQLite table {table} has no columns")
    column_sql = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO public.{quote_identifier(table)} ({column_sql}) "
        f"VALUES ({placeholders})"
    )
    inserted = 0
    with postgres_connection.cursor() as cursor:
        for batch in source_rows(sqlite_connection, table, batch_size):
            cursor.executemany(insert_sql, batch)
            inserted += len(batch)
    return inserted


def print_counts(counts: dict[str, int], label: str) -> None:
    print(label)
    for table in TABLES:
        print(f"  {table:20} {counts[table]:>10}")


def main() -> int:
    args = parse_args()
    if not args.sqlite_path.is_file():
        print(f"SQLite source not found: {args.sqlite_path}", file=sys.stderr)
        return 2
    if not args.schema_file.is_file():
        print(f"Schema file not found: {args.schema_file}", file=sys.stderr)
        return 2
    if args.batch_size < 1:
        print("--batch-size must be at least 1", file=sys.stderr)
        return 2

    sqlite_connection = sqlite3.connect(str(args.sqlite_path))
    try:
        source = validate_source(sqlite_connection)
        print_counts(source, f"SQLite source: {args.sqlite_path.resolve()}")
        if args.dry_run:
            print("Dry run complete; Supabase was not contacted.")
            return 0

        if not args.database_url:
            print(
                "Missing Supabase database URI. Set SUPABASE_DB_URL or pass --database-url.",
                file=sys.stderr,
            )
            return 2
        if not args.database_url.startswith(("postgres://", "postgresql://")):
            print("Supabase database URI must start with postgres:// or postgresql://", file=sys.stderr)
            return 2

        try:
            import psycopg
        except ImportError:
            print(
                "Missing psycopg. Install it with: pip install -r requirements-migration.txt",
                file=sys.stderr,
            )
            return 2

        print(f"Connecting to {redacted_url(args.database_url)}")
        with psycopg.connect(args.database_url) as postgres_connection:
            execute_schema(postgres_connection, args.schema_file)
            validate_target_schema(sqlite_connection, postgres_connection)
            before = target_counts(postgres_connection)
            if any(before.values()) and not args.replace:
                populated = ", ".join(
                    f"{table}={count}" for table, count in before.items() if count
                )
                raise RuntimeError(
                    "Supabase target is not empty. Re-run with --replace only after "
                    f"confirming replacement is intended. Existing rows: {populated}"
                )

            if args.replace:
                table_sql = ", ".join(
                    f"public.{quote_identifier(table)}" for table in TABLES
                )
                postgres_connection.execute(f"TRUNCATE TABLE {table_sql}")

            inserted: dict[str, int] = {}
            for table in TABLES:
                inserted[table] = insert_table(
                    sqlite_connection, postgres_connection, table, args.batch_size
                )
                print(f"Imported {table}: {inserted[table]} rows")

            after = target_counts(postgres_connection)
            if source != inserted or source != after:
                raise RuntimeError(
                    "Row-count verification failed; transaction will be rolled back. "
                    f"source={source}, inserted={inserted}, target={after}"
                )
            print_counts(after, "Verified Supabase target")
            print("Migration committed successfully.")
        return 0
    finally:
        sqlite_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
