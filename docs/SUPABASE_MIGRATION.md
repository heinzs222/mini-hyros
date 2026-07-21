# Supabase Database Migration

Vigil currently consists of a Next.js frontend on Vercel and a FastAPI API on
Render. Supabase can replace the persistent SQLite database immediately, but it
does not host an existing FastAPI container. Moving API compute off Render is a
separate API rewrite or deployment change.

The browser publishable key is intentionally not used for warehouse reads or
writes. All warehouse tables have row-level security enabled with no anonymous
policies. The backend must use Supabase's private Postgres connection URI.

## 1. Get the private database URI

In Supabase, open the project and select **Connect**. Copy a Postgres connection
string appropriate for a persistent server. Do not commit it. Set it locally:

```powershell
$env:SUPABASE_DB_URL = "postgresql://..."
```

The URI is different from `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`. A publishable
key cannot create this schema or import trusted attribution data.

## 2. Export the current Render database

Download `/var/data/attributionops.sqlite` from the current Render persistent
disk before changing production. Keep Render serving traffic during validation.

## 3. Inspect and migrate

```powershell
pip install -r requirements-migration.txt
python scripts/migrate_sqlite_to_supabase.py --sqlite-path .\attributionops.sqlite --dry-run
python scripts/migrate_sqlite_to_supabase.py --sqlite-path .\attributionops.sqlite --replace
```

`--replace` imports all ten warehouse tables in one transaction. The command
rolls back if any source and target row count differs. Without `--replace`, it
refuses to write into a populated target.

## 4. Production cutover

Do not point production at Supabase until the backend has a Postgres data-access
adapter and report parity has been tested. The current code contains SQLite SQL
(`PRAGMA`, `rowid`, `strftime`, `julianday`, and SQLite placeholders), so changing
only `ATTRIBUTIONOPS_DB_PATH` or installing `@supabase/supabase-js` would break
reporting.

The recommended sequence is:

1. Import a snapshot and verify row counts.
2. Add and test the FastAPI Postgres adapter against the imported project.
3. Pause ingestion briefly, import a final snapshot, and switch the backend.
4. Move API compute only after database/report parity is confirmed.
