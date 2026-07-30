# Supabase Database Migration

Vigil currently consists of a Next.js frontend on Vercel and a FastAPI API on
Render. Supabase replaces the persistent SQLite database; Render still runs the
FastAPI container.

The browser publishable key is intentionally not used for warehouse reads or
writes. All warehouse tables have row-level security enabled with no anonymous
policies. The backend must use Supabase's private Postgres connection URI.

## Current status (2026-07-22)

A production SQLite snapshot passed `PRAGMA quick_check` and was imported into
Supabase in one committed transaction. Source and target row counts matched
exactly:

| Table | Rows |
| --- | ---: |
| `spend` | 1,211 |
| `sessions` | 267,810 |
| `touchpoints` | 141,547 |
| `orders` | 8,678 |
| `conversions` | 11,806 |
| `reported_value` | 0 |
| `ad_names` | 2,330 |
| `video_metrics` | 0 |
| `campaign_settings` | 3 |
| `refund_log` | 25 |

This is a verified snapshot, not the live runtime database. Render continues
reading and writing SQLite until the Postgres data-access adapter and
fixed-window report parity checks are complete.

## 1. Get the private database URI

In Supabase, open the project and select **Connect**. Copy a Postgres connection
string appropriate for a persistent server. Do not commit it. Set it locally:

```powershell
$env:SUPABASE_DB_URL = "postgresql://..."
```

The URI is different from `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`. A publishable
key cannot create this schema or import trusted attribution data.

## 2. Export the current Render database

Keep Render serving traffic during validation. Deploy the protected export
endpoint, create a long random `DATABASE_EXPORT_TOKEN` in Render, and redeploy.
Never put this token in a URL or commit it.

Download a consistent SQLite snapshot through the API:

```powershell
$env:DATABASE_EXPORT_TOKEN = "your-random-export-token"
$headers = @{"X-Mini-Hyros-Export-Token" = $env:DATABASE_EXPORT_TOKEN}
Invoke-WebRequest `
  -Uri "https://vigil-api.vercel.app/api/admin/database-export" `
  -Headers $headers `
  -OutFile ".\production-attributionops.sqlite"
```

The endpoint uses SQLite's online backup API and runs an integrity check before
returning the file. It does not stop ingestion or expose the Render disk path.
Confirm the downloaded file before importing it:

```powershell
python -c "import sqlite3; db=sqlite3.connect('production-attributionops.sqlite'); print(db.execute('PRAGMA integrity_check').fetchone()[0]); db.close()"
```

The result must be `ok`. Keep this snapshot private because it contains
customer, click, lead, and order data.

## 3. Inspect and migrate

```powershell
pip install -r requirements-migration.txt
python scripts/migrate_sqlite_to_supabase.py --sqlite-path .\production-attributionops.sqlite --dry-run
python scripts/migrate_sqlite_to_supabase.py --sqlite-path .\production-attributionops.sqlite --replace
```

`--replace` imports the warehouse tables plus optional runtime tables
(`platform_tokens`, `stripe_sync_coverage`, `capi_log`, `webhook_log`,
`email_sms_events`) in one transaction. Older SQLite snapshots that do not have
the optional tables still migrate cleanly. The command rolls back if any source
and target row count differs. Without `--replace`, it refuses to write into a
populated target.

## 4. Production cutover

Set this private environment variable on the Render backend:

```powershell
SUPABASE_DB_URL=postgresql://...
```

`SUPABASE_DB_URL` takes precedence over `ATTRIBUTIONOPS_DB_PATH`. Leave the
SQLite path in place as a rollback fallback, but remove or blank
`SUPABASE_DB_URL` if you need to return to the persistent disk.

The recommended sequence is:

1. Import a snapshot and verify row counts.
2. Run the backend locally with `SUPABASE_DB_URL` set and compare `/api/report`
   against the SQLite source for the same date range.
3. Pause ingestion briefly, import a final snapshot with `--replace`, and set
   `SUPABASE_DB_URL` on Render.
4. Hit `/api/health` and a report endpoint before resuming traffic.
