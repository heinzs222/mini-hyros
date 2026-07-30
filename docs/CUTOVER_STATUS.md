# Vigil Supabase Cutover Status

Last updated: 2026-07-22

## Completed

- Migrated the canonical Vigil dataset from SQLite to Supabase PostgreSQL.
- Verified the PostgreSQL adapter against the migrated production-sized data.
- Added a Vercel-compatible FastAPI entrypoint at `api/index.py`.
- Added root deployment configuration and Python dependencies for a standalone API project.
- Kept SQLite support for local development and existing tests.
- Made OAuth callback generation use `PUBLIC_API_URL` or `API_PUBLIC_URL` instead of assuming Render.

Verified Supabase row counts at migration time:

| Table | Rows |
| --- | ---: |
| sessions | 267,810 |
| touchpoints | 141,547 |
| orders | 8,678 |
| conversions | 11,806 |
| spend | 1,211 |

## Deployment Architecture

- Frontend: Vercel (`dashboard` project)
- API: standalone Vercel Python project from the repository root
- Database: Supabase PostgreSQL
- Server-side tagging: Stape
- Google Ads reporting: Google Ads script posts spend to the API

Supabase supplies PostgreSQL. It does not run the FastAPI process, so the API still needs a compute runtime. The root Vercel project provides that runtime without Render.

## API Environment Variables

Configure these on the standalone Vercel API project. Values must stay in deployment secrets and must never be committed.

- `SUPABASE_DB_URL`
- `PUBLIC_API_URL`
- `FRONTEND_URL`
- `AUTH_USERNAME`
- `AUTH_PASSWORD`
- `GOOGLE_ADS_SCRIPT_TOKEN`
- Existing Meta, TikTok, Stripe, GoHighLevel, Stape, and OAuth secrets currently used by the API

Set `PUBLIC_API_URL` to the final API domain after the first deployment. Set `FRONTEND_URL` to the production dashboard URL.

## Cutover Checklist

Do not disable Render until every item below succeeds against the replacement API.

1. Deploy the repository root as a separate Vercel project.
2. Confirm `/api/health` and a dated `/api/report` request return successfully.
3. Set `NEXT_PUBLIC_API_URL` on the dashboard Vercel project to the replacement API URL and redeploy.
4. Update the website tracking scripts' `data-endpoint` value.
5. Update Stape destinations and server-side requests that still target Render.
6. Update the Google Ads script `MINI_HYROS_ENDPOINT` and run one custom-date push.
7. Update Meta and TikTok OAuth redirect URLs, then reconnect only if their providers require it.
8. Verify tracking, purchases, leads, spend, names, and dashboard reports.
9. Disable the Render service only after the old hostname receives no required traffic.

## Data and Product Direction

- Actual Hyros is comparison-only. Never modify that account.
- Vigil must report source data and calculated metrics without invented values.
- Revenue is based on historical Stripe state, including refund timestamps, so past date ranges do not change because of later refunds.
- Google spend comes from the Google Ads script when direct reporting API access is unavailable.
- Stape handles event ingestion and conversion forwarding; it is not the source for Google Ads reporting metrics.
- Attribution quality depends on click identifiers and the tracking script being present across funnel and checkout pages.

## Remaining Work

- Create and verify the standalone Vercel API deployment.
- Switch all external callers to its final URL.
- Observe the cutover, then retire Render.
