"""SQLite→Postgres SQL translation for the dual-backend data layer.

The AttributionOps warehouse was written against SQLite and its dialect is used
verbatim across ~30 modules. Rather than hand-porting every query, all SQL flows
through :func:`translate` when the process is configured to talk to Postgres
(Supabase). Local development and the test suite still run on SQLite, so the
original SQL remains the source of truth and stays continuously exercised.

What this translator handles (everything else is left untouched):

* Parameter placeholders: ``?`` → ``%s`` and ``:name`` → ``%(name)s`` (psycopg
  format), with literal ``%`` escaped to ``%%`` so psycopg's own interpolation
  never trips on ``LIKE '%x%'`` or similar.
* Idempotent upserts: ``INSERT OR IGNORE`` → ``... ON CONFLICT DO NOTHING`` and
  ``INSERT OR REPLACE`` / ``REPLACE INTO`` → ``... ON CONFLICT (<keys>) DO
  UPDATE SET ...`` using a small per-table conflict-key registry.
* Lenient numeric casts: SQLite's ``CAST(x AS REAL/INTEGER)`` silently coerces
  empty/garbage text to 0; Postgres raises. These become ``sqlite_real(x)`` /
  ``sqlite_int(x)`` helper functions (installed by the schema migration) that
  reproduce SQLite's forgiving behaviour.
* ``rowid`` → ``ctid`` for the handful of ORDER-BY tiebreak / dedupe queries.

The tokenizer is string-literal aware: nothing inside ``'...'`` single-quoted
literals (SQLite doubles the quote to escape) or ``"..."`` identifiers is
rewritten, so a literal ``?``, ``:``, ``%`` or the word ``rowid`` in data is
preserved.
"""

from __future__ import annotations

import re

# Conflict-target columns for the tables written with REPLACE semantics
# (INSERT OR REPLACE / REPLACE INTO). Derived from each table's PRIMARY KEY in
# the Postgres schema. INSERT OR IGNORE does not need an explicit target
# (Postgres accepts a bare ``ON CONFLICT DO NOTHING``), so those tables are
# intentionally absent here.
REPLACE_CONFLICT_KEYS: dict[str, tuple[str, ...]] = {
    "ad_names": ("platform", "entity_type", "entity_id"),
    "platform_tokens": ("platform",),
    "capi_log": ("id",),
}


def _split_top_level_columns(collist: str) -> list[str]:
    """Split a ``(a, b, c)`` column list on top-level commas."""
    out: list[str] = []
    depth = 0
    cur = []
    for ch in collist:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        out.append("".join(cur).strip())
    return [c.strip().strip('"') for c in out if c.strip()]


_REPLACE_RE = re.compile(
    r"^\s*(?:INSERT\s+OR\s+REPLACE\s+INTO|REPLACE\s+INTO)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
_INSERT_OR_IGNORE_RE = re.compile(
    r"^\s*INSERT\s+OR\s+IGNORE\s+INTO", re.IGNORECASE
)


def _rewrite_upsert(sql: str) -> str:
    """Rewrite SQLite REPLACE/INSERT OR IGNORE upserts to ON CONFLICT."""
    if _INSERT_OR_IGNORE_RE.match(sql):
        sql = re.sub(
            r"^\s*INSERT\s+OR\s+IGNORE\s+INTO",
            "INSERT INTO",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        # Already has an explicit ON CONFLICT? leave it. Otherwise append the
        # bare DO NOTHING (valid in Postgres for any unique/pk violation).
        if not re.search(r"ON\s+CONFLICT", sql, re.IGNORECASE):
            sql = sql.rstrip().rstrip(";") + "\nON CONFLICT DO NOTHING"
        return sql

    m = _REPLACE_RE.match(sql)
    if not m:
        return sql
    table = m.group(1)
    columns = _split_top_level_columns(m.group(2))
    keys = REPLACE_CONFLICT_KEYS.get(table.lower())
    if not keys:
        # Unknown table: fall back to DO NOTHING rather than emit invalid SQL.
        # (Registry should cover every REPLACE target; this is a safety net.)
        conflict = "ON CONFLICT DO NOTHING"
    else:
        updates = [c for c in columns if c.lower() not in {k.lower() for k in keys}]
        set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
        if set_sql:
            conflict = f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {set_sql}"
        else:
            conflict = f"ON CONFLICT ({', '.join(keys)}) DO NOTHING"
    # Replace the leading REPLACE/INSERT OR REPLACE with plain INSERT ...
    head = _REPLACE_RE.sub(
        lambda mm: f"INSERT INTO {mm.group(1)} ({mm.group(2)})", sql, count=1
    )
    return head.rstrip().rstrip(";") + "\n" + conflict


def _rewrite_casts(sql: str) -> str:
    """Rewrite ``CAST(<expr> AS REAL|INTEGER)`` → lenient helper functions.

    Balanced-paren aware so nested ``CAST(CAST(x AS REAL) ...)`` is handled from
    the innermost cast outward.
    """
    lowered = sql
    while True:
        # Find the *last* CAST( so we process innermost-first when nested.
        idx = _rfind_cast(lowered)
        if idx < 0:
            break
        open_paren = idx + 4  # position of '('
        depth = 0
        end = -1
        for j in range(open_paren, len(lowered)):
            c = lowered[j]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    end = j
                    break
        if end < 0:
            break
        inner = lowered[open_paren + 1 : end]
        m = re.search(r"^(.*)\bAS\s+(REAL|INTEGER|INT)\s*$", inner, re.IGNORECASE | re.DOTALL)
        if not m:
            # Not a numeric cast we translate (e.g. AS TEXT) — mark the whole
            # ``CAST(`` (5 chars) so _rfind_cast skips it; restored at the end.
            lowered = lowered[:idx] + "CAST\x00" + lowered[idx + 5 :]
            continue
        expr = m.group(1).strip()
        kind = m.group(2).upper()
        fn = "sqlite_real" if kind == "REAL" else "sqlite_int"
        replacement = f"{fn}({expr})"
        lowered = lowered[:idx] + replacement + lowered[end + 1 :]
    return lowered.replace("CAST\x00", "CAST(")


def _rfind_cast(sql: str) -> int:
    """Index of the last case-insensitive ``cast(`` occurrence, or -1."""
    return sql.lower().rfind("cast(")


def _tokenize_and_rewrite(sql: str) -> str:
    """Placeholder/`%`/rowid rewriting that must skip string literals."""
    out: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        # Single-quoted string literal (SQLite escapes ' as '').
        if ch == "'":
            out.append(ch)
            i += 1
            while i < n:
                c = sql[i]
                if c == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append("''")
                        i += 2
                        continue
                    out.append("'")
                    i += 1
                    break
                out.append("%%" if c == "%" else c)
                i += 1
            continue
        # Double-quoted identifier.
        if ch == '"':
            out.append(ch)
            i += 1
            while i < n:
                c = sql[i]
                out.append(c)
                i += 1
                if c == '"':
                    break
            continue
        # Line / block comments pass through untouched (but escape %).
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            while i < n and sql[i] != "\n":
                out.append("%%" if sql[i] == "%" else sql[i])
                i += 1
            continue
        # Qmark placeholder.
        if ch == "?":
            out.append("%s")
            i += 1
            continue
        # Named placeholder :name (but not ::cast).
        if ch == ":" and not (i + 1 < n and sql[i + 1] == ":"):
            m = re.match(r":([A-Za-z_][A-Za-z0-9_]*)", sql[i:])
            if m:
                out.append(f"%({m.group(1)})s")
                i += m.end()
                continue
        # Literal percent → escape for psycopg.
        if ch == "%":
            out.append("%%")
            i += 1
            continue
        # rowid → ctid (word-boundary).
        if (ch in "rR") and re.match(r"rowid\b", sql[i:], re.IGNORECASE):
            prev = sql[i - 1] if i > 0 else " "
            if not (prev.isalnum() or prev == "_"):
                out.append("ctid")
                i += 5
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def translate(sql: str) -> str:
    """Translate a SQLite SQL string to its Postgres equivalent."""
    sql = _rewrite_upsert(sql)
    sql = _rewrite_casts(sql)
    sql = _tokenize_and_rewrite(sql)
    return sql
