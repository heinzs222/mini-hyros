# Vigil Infrastructure Migration Handoff
Last updated: 2026-07-22

## Current architecture

- Frontend: Vercel at `https://mini-hyros.vercel.app`
- API: Vercel Python service at `https://vigil-api.vercel.app`
- Database: Supabase PostgreSQL project `orwohzzwrxngztiaduoq`
- Tracking collection: the Vigil API plus Stape server-side GTM
- Ad reporting: platform APIs where authorized; Google Ads Script push remains the fallback for Google spend

Render is no longer the target API or database host. Keep the Render service available only until every external sender listed below has been changed and a real event has been verified through the Vercel API.

## Completed work

- Added a PostgreSQL database adapter while retaining SQLite support for local tests.
- Migrated the production dataset into Supabase PostgreSQL.
- Added the Vercel Python ASGI entry point and service configuration.
- Deployed the FastAPI backend to Vercel and assigned `vigil-api.vercel.app`.
- Configured the API deployment with Supabase, authentication, Stripe, Meta, TikTok, Google, and application environment variables.
- Verified the replacement API health endpoint, tracking script, and authenticated report endpoint.
- Updated application defaults and documentation to use `https://vigil-api.vercel.app`.

## Required external endpoint changes

These systems are configured outside this repository and must use the replacement API URL:

1. Website tracking scripts on `formation.xguard.ca`
   - `data-endpoint="https://vigil-api.vercel.app"`
   - Script source: `https://vigil-api.vercel.app/t/hyros.js`
   - GHL helper: `https://vigil-api.vercel.app/t/hyros-ghl.js`
2. Stape server GTM
   - Replace any `https://mini-hyros.onrender.com` destination with `https://vigil-api.vercel.app`.
3. Google Ads Script
   - Set `MINI_HYROS_ENDPOINT` to `https://vigil-api.vercel.app`.
4. TikTok developer app
   - Redirect URI: `https://vigil-api.vercel.app/api/platform-auth/tiktok/callback`
5. Meta and GoHighLevel developer settings
   - Replace old Render callback URLs with the matching path under `https://vigil-api.vercel.app`.

Do not remove Render until a real page view, lead, purchase, and Google spend push have all reached the replacement API.

## Verification checklist

- `GET https://vigil-api.vercel.app/health` returns HTTP 200.
- `GET https://vigil-api.vercel.app/t/hyros.js` returns JavaScript.
- The production dashboard loads report data from `vigil-api.vercel.app`.
- Browser Network shows tracking and conversion requests to `vigil-api.vercel.app`, not Render.
- A GHL form submission creates a lead in Vigil.
- A purchase creates an order and forwards the configured conversion events.
- The Google Ads Script returns HTTP 200 from `/api/spend/google-ads-script`.
- TikTok OAuth callback and reporting permissions remain valid after the redirect change.

## Decommission Render

After the verification checklist passes:

1. Take a final Render disk/database export for rollback only.
2. Remove or suspend the Render web service.
3. Remove old Render callback URLs from platform settings.
4. Revoke Render-only secrets.
5. Keep Supabase backups and Vercel deployment protection enabled.

## Security

No database passwords, API tokens, application passwords, or export tokens belong in Git. Production secrets are stored only in deployment environment variables. Rotate any secret that was pasted into chat or otherwise shared outside the deployment secret store.
