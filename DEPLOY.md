# Deploy Mini Hyros on Vercel + Supabase

Mini Hyros now runs entirely on Vercel:

| Piece | Where | How |
|---|---|---|
| **Dashboard** (`dashboard/`, Next.js) | Vercel project | Static/SSR Next.js |
| **Backend API** (`backend/`, FastAPI) | Vercel Python serverless function (`api/index.py`) | ASGI, routed via `vercel.json` |
| **Database** | Supabase Postgres | `migrations/postgres/0001_schema.sql` |
| **Periodic sync** | Vercel Cron → `/api/cron/sync` | replaces the old always-on loop |
| **Live feed** | `/api/live/recent` polling | replaces the `/ws` WebSocket |

> Local development is unchanged and still uses SQLite — the Postgres backend
> only turns on when a Postgres connection string env var is present.

---

## What You Need
- A **GitHub** account (the repo)
- A **Vercel** account (free) — hosts both the backend and the dashboard
- A **Supabase** account (free) — the Postgres database

---

## Step 1: Create the Supabase database

1. Go to https://supabase.com → **New project**. Pick a name, region, and a
   strong database password (save it).
2. Once the project is ready, open **SQL Editor → New query**, paste the entire
   contents of [`migrations/postgres/0001_schema.sql`](migrations/postgres/0001_schema.sql),
   and **Run**. This creates every table, index, the lenient-cast/`strftime`
   helper functions, and the recurring-order attribution triggers.
3. Get the connection string: **Project Settings → Database → Connection string
   → "Transaction pooler" (port 6543)**. It looks like:
   ```
   postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres
   ```
   Use the **transaction pooler** (6543), not the direct connection — serverless
   functions open many short-lived connections. (The app disables prepared
   statements so it is pooler-safe.)

---

## Step 2: Deploy the Backend API on Vercel

1. Go to https://vercel.com → **Add New → Project** and import the repo.
2. Configure:
   - **Root Directory**: leave as the repository root (`.`)
   - **Framework Preset**: **Other** (Vercel auto-detects the Python function
     from `api/index.py` + `requirements.txt` and the `vercel.json` routing)
3. Add **Environment Variables** (see the full reference below). At minimum:
   - `DATABASE_URL` = the Supabase transaction-pooler URI from Step 1
   - `TRACKING_DOMAIN` = your backend URL (e.g. `https://mini-hyros-api.vercel.app`)
   - `SITE_TOKEN` = any string you choose
   - `CRON_SECRET` = a random string (Vercel Cron sends it as a bearer token)
   - `REPORT_TIMEZONE`, `REPORT_CURRENCY`, plus any ad-platform/webhook secrets
4. **Deploy.** Your backend URL will be something like
   `https://mini-hyros-api.vercel.app`.
5. The `crons` entry in `vercel.json` registers an hourly call to
   `/api/cron/sync`. Adjust the schedule there if you want it more/less frequent
   (minute-level cron requires a Vercel Pro plan).

**Test it:** visit `https://<your-backend>.vercel.app/api/health` — you should
see `{"status":"ok", ... ,"backend":"postgres"}`.

---

## Step 3: Deploy the Dashboard on Vercel

1. **Add New → Project**, import the **same repo** again (a second Vercel
   project for the frontend).
2. Configure:
   - **Framework Preset**: **Next.js**
   - **Root Directory**: `dashboard`
3. Add this **Environment Variable**:
   - `NEXT_PUBLIC_API_URL` = your backend URL from Step 2
     (e.g. `https://mini-hyros-api.vercel.app`)
4. **Deploy.** Your dashboard URL will be something like
   `https://mini-hyros.vercel.app`.

> Set `CORS_ALLOW_ORIGINS` on the backend to your dashboard origin if you want
> to lock CORS down (it defaults to `*` so public tracking scripts work).

---

## Step 4: Install the Tracking Pixel

Visit `https://<your-backend>.vercel.app/t/setup` for copy-paste snippets. The
main script is:

```html
<script src="https://<your-backend>.vercel.app/t/hyros.js"
        data-token="your-site-token"
        data-endpoint="https://<your-backend>.vercel.app"></script>
```

---

## Environment Variable Reference

Set these on the **backend** Vercel project (secrets marked 🔒):

| Variable | Example / default | Notes |
|---|---|---|
| `DATABASE_URL` 🔒 | `postgresql://…pooler.supabase.com:6543/postgres` | Supabase transaction pooler. Also accepts `ATTRIBUTIONOPS_DATABASE_URL` / `POSTGRES_URL` / `SUPABASE_DB_URL`. |
| `CRON_SECRET` 🔒 | random string | Auth for `/api/cron/sync` (Vercel Cron bearer token). |
| `TRACKING_DOMAIN` | `https://mini-hyros-api.vercel.app` | Used by `/t/setup` script URLs. |
| `SITE_TOKEN` | `your-site-token` | Pixel token. |
| `REPORT_TIMEZONE` | `Etc/GMT+6` | Match the source Hyros account. |
| `REPORT_CURRENCY` | `CAD` | Reporting currency. |
| `SPEND_FX_RATES` 🔒 | `USD:1.40` | Read-time FX conversion. |
| `SYNC_INTERVAL_MINUTES` | `30` | Cadence hint (the actual cadence is the Vercel Cron schedule). |
| `META_ACCESS_TOKEN` 🔒 / `META_AD_ACCOUNT_ID` 🔒 / `META_PIXEL_ID` 🔒 | | Meta Ads API. |
| `META_INCLUDE_CAMPAIGN_ADJUSTMENTS` | `true` | |
| `GOOGLE_ADS_DEVELOPER_TOKEN` 🔒 (+ OAuth vars) | | Google Ads API. |
| `GOOGLE_INCLUDE_CAMPAIGN_ADJUSTMENTS` | `true` | |
| `TIKTOK_ACCESS_TOKEN` 🔒 / `TIKTOK_ACCOUNT_CURRENCY` | | TikTok Ads API. |
| `STRIPE_RECURRING_AMOUNT_KEYS` | `CAD:5000` | |
| `STRIPE_HISTORY_BACKFILL_DAYS` | `365` | |
| `SHOPIFY_WEBHOOK_SECRET` 🔒 / `STRIPE_WEBHOOK_SECRET` 🔒 | | Webhook verification. |
| `AUTH_ENABLED` | `false` | Set `true` + `AUTH_USERNAME`/`AUTH_PASSWORD`/`AUTH_SECRET_KEY` to require login. |
| `CORS_ALLOW_ORIGINS` | `*` | Comma-separated origins to lock CORS down. |

Set on the **dashboard** project: `NEXT_PUBLIC_API_URL` (your backend URL), and
optionally `NEXT_PUBLIC_LIVE_POLL_MS` (live-feed poll interval, default 4000).

---

## Your Final URLs

| What | URL |
|---|---|
| Dashboard | `https://<your-dashboard>.vercel.app` |
| Backend API | `https://<your-backend>.vercel.app` |
| Health check | `https://<your-backend>.vercel.app/api/health` |
| Tracking Script | `https://<your-backend>.vercel.app/t/hyros.js` |
| Setup Guide | `https://<your-backend>.vercel.app/t/setup` |
| Shopify Webhook | `https://<your-backend>.vercel.app/api/webhooks/shopify` |
| Stripe Webhook | `https://<your-backend>.vercel.app/api/webhooks/stripe` |
| Cron sync (Vercel Cron) | `https://<your-backend>.vercel.app/api/cron/sync` |

---

## Notes

- **Migrations**: `migrations/postgres/0001_schema.sql` is the single source of
  truth for the Postgres schema. The app never runs SQLite-style
  `CREATE`/`ALTER`/`PRAGMA`/trigger DDL against Postgres — apply the migration
  once (Step 1) and re-apply it (it is idempotent) when the schema changes.
- **Periodic sync**: driven by Vercel Cron hitting `/api/cron/sync`. The old
  in-process 30-minute loop is disabled on serverless.
- **Live feed**: the dashboard polls `/api/live/recent` (every 4s by default);
  there is no long-lived WebSocket on serverless.
- **Cold starts**: serverless functions cold-start after inactivity; the first
  request may be slower while the report cache warms lazily.
- **Local development** is unchanged: without a Postgres URL the app uses SQLite
  and `uvicorn main:app --app-dir backend` (see `README.md`).
