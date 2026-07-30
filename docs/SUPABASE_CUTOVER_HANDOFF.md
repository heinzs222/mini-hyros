# Vigil Supabase Cutover Handoff

## Objective

Move Vigil (formerly Mini Hyros) away from Render and its SQLite database without losing tracking data or interrupting inbound webhooks.

## Current Architecture

- Frontend: Vercel at `https://mini-hyros.vercel.app`
- API: Vercel Python service at `https://vigil-api.vercel.app`
- Database: Supabase PostgreSQL project `orwohzzwrxngztiaduoq`
- Legacy API: Render at `https://mini-hyros.onrender.com` (temporary fallback only)
- Server-side tagging: Stape sGTM

Supabase is the persistent PostgreSQL database. The FastAPI service still needs a Python runtime, so it runs as a Vercel Python service rather than inside Supabase.

## Completed Work

- Added a PostgreSQL database adapter while preserving the existing application query API.
- Configured the backend to use `SUPABASE_DB_URL` or `DATABASE_URL`.
- Prevented SQLite schema bootstrap when PostgreSQL is active.
- Added the Vercel ASGI entry point at `api/index.py`.
- Added Vercel Python packaging and dependency metadata.
- Configured the API project environment without committing credentials.
- Deployed the API and assigned the stable alias `https://vigil-api.vercel.app`.
- Verified the new API against Supabase:
  - `/health` returns HTTP 200.
  - `/t/hyros.js` returns HTTP 200.
  - An authenticated report request returns HTTP 200.
- Verified the migrated database contains sessions, touchpoints, orders, conversions, and spend rows.
- Updated application defaults and integration examples to target the Vercel API.
- Made OAuth callback URL generation configurable through `PUBLIC_API_URL` / `API_PUBLIC_URL`.

## Relevant Commits

- `6733249` - Add Supabase PostgreSQL backend deployment
- `536f8f2` - Configure Vercel API service
- `5cefc0f` - Declare Vercel Python API entrypoint
- `446c4be` - Fix Vercel Python package metadata

## Required Production Environment

The Vercel API project must contain the existing backend credentials plus:

```text
DATABASE_URL=<Supabase PostgreSQL connection string>
SUPABASE_DB_URL=<Supabase PostgreSQL connection string>
PUBLIC_API_URL=https://vigil-api.vercel.app
API_PUBLIC_URL=https://vigil-api.vercel.app
BACKEND_URL=https://vigil-api.vercel.app
DASHBOARD_URL=https://mini-hyros.vercel.app
FRONTEND_URL=https://mini-hyros.vercel.app
TRACKING_DOMAIN=https://vigil-api.vercel.app
CORS_ORIGINS=https://mini-hyros.vercel.app,https://formation.xguard.ca
```

Keep all existing authentication, Stripe, Meta, TikTok, Google Ads, GHL, and site-token variables configured. Never commit their values.

## External Cutover Checklist

These systems are configured outside the repository and must all point to the stable Vercel API before Render is disabled:

1. Website tracking scripts on `formation.xguard.ca`
   - Replace every `https://mini-hyros.onrender.com` script URL and `data-endpoint` value with `https://vigil-api.vercel.app`.
   - Keep the Stape endpoint unchanged.
2. Stape server GTM
   - Change any Vigil/Mini Hyros webhook or API endpoint from Render to `https://vigil-api.vercel.app`.
3. Google Ads Script
   - Set `MINI_HYROS_ENDPOINT` to `https://vigil-api.vercel.app`.
   - Run once and confirm the response is HTTP 200 with inserted spend rows.
4. TikTok developer app
   - Set the OAuth redirect URI to `https://vigil-api.vercel.app/api/platform-auth/tiktok/callback`.
   - Reconnect TikTok if its existing token was issued for the old redirect or lacks reporting scopes.
5. Meta and GHL
   - Replace any configured callback/webhook URL that still uses the Render hostname.
6. Stripe webhooks
   - Replace the Render webhook destination with the equivalent endpoint under `https://vigil-api.vercel.app`.

## Verification Before Disabling Render

Perform one real page visit, one lead submission, and one controlled purchase/test conversion. Confirm each request reaches `vigil-api.vercel.app` and appears in Vigil. Then confirm:

- Google, Meta, and TikTok spend syncs do not call Render.
- Google Ads Script posts to the Vercel API.
- Stape receives and forwards purchase and lead events.
- OAuth reconnect links use the Vercel callback domain.
- The dashboard and reports load the selected date range.
- No browser network request contains `mini-hyros.onrender.com`.

Only after all checks pass should the Render web service be suspended. Keep the Render service available during cutover, but do not send new traffic to it.

## Rollback

If a production integration fails during cutover, restore only that integration's endpoint to Render while fixing it. The Supabase database remains the source of truth; do not restore or overwrite it with an old SQLite file.
