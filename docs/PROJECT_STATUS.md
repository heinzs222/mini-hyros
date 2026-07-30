# Vigil / Mini Hyros Project Status

Last updated: 2026-07-22

## Objective

Vigil (formerly Mini Hyros) is an attribution and marketing reporting system for
XGuard. It must report real collected data and approach Hyros parity without
inventing attribution, leads, revenue, or spend. The existing Hyros account is a
read-only reference: never create, edit, delete, or reconfigure anything there.

## Current architecture

- Frontend: Vercel at `https://mini-hyros.vercel.app/`.
- API and ingestion: FastAPI on Render at `https://vigil-api.vercel.app/`.
- Current production database: SQLite on the Render persistent disk at
  `/var/data/attributionops.sqlite`.
- Target database: Supabase Postgres. A production snapshot has been imported
  and verified with exact source/target row-count parity, but production has
  not been switched to Postgres.
- Server-side tagging: Stape-hosted Google Tag Manager server container.

## Data flows

- `hyros.js` records browser visits, click IDs, UTMs, visitor/session IDs, and
  conversions in the Vigil API.
- `hyros-ghl.js` supplements tracking on GoHighLevel pages.
- GoHighLevel supplies lead/contact data for lead reporting and identity
  matching.
- Stripe supplies orders, revenue, and refunds.
- Google Ads spend is imported by the Google Ads account script through the
  protected `/api/spend/google-ads-script` endpoint.
- Meta and TikTok spend and names use their Marketing APIs when credentials and
  scopes are valid.
- Purchase conversion events are forwarded through Stape/sGTM; preview testing
  showed the Data Client claiming the request and the Google Ads purchase tag
  firing successfully.

## Verified work

- Cross-origin browser tracking and conversion requests return successfully.
- The Stape server container receives purchase events and sends successful
  outbound Google requests in GTM preview.
- Google Ads script imports support explicit and rolling date ranges and replace
  only the dates included in each batch.
- Render has a persistent disk and the application points SQLite at `/var/data`.
- Dashboard date controls, spend imports, Stripe sync, and lead sync have been
  repaired incrementally; timeout handling no longer leaves the main sync stuck.
- Supabase schema migration `202607210001_initial_schema.sql` has been applied to
  the target project.
- `scripts/migrate_sqlite_to_supabase.py` performs dry runs and transactional,
  row-count-verified imports.
- A token-protected production SQLite snapshot endpoint is implemented at
  `/api/admin/database-export`.

## Known gaps and risks

- Supabase is not the live runtime database. FastAPI still contains substantial
  SQLite-specific SQL (`PRAGMA`, `rowid`, SQLite date functions, and `?`
  placeholders). Changing one environment variable is not a valid cutover.
- Supabase contains a verified production snapshot, but it can lag behind the
  live SQLite database while Render continues ingesting into SQLite. A final
  delta import is required immediately before runtime cutover.
- Attribution coverage is incomplete. Unattributed orders must remain clearly
  reported rather than assigned to a fabricated source.
- Cost per lead can differ from Hyros when Vigil and Hyros use different lead
  definitions or historical GHL coverage. Exact parity requires comparing the
  same GHL contact set, creation window, deduplication rule, and source model.
- Historical Stripe refund timing was identified as a source of retroactive
  revenue drift. Refunds must be evaluated as of the selected report end date;
  this behavior needs a final production parity check after data migration.
- TikTok reporting depends on approved advertiser information, campaign, and
  reporting scopes. Reauthorization may still be required when the token lacks
  an approved scope.
- Lead conversion forwarding to every platform dataset/pixel needs an end-to-end
  production test. A purchase test alone does not prove lead forwarding.
- Reports, filters, dropdowns, and action buttons require a final workflow pass
  after the database cutover. Disabled or unsupported actions must say so rather
  than appear to work.

## Supabase migration status

Completed:

1. Created a Postgres-compatible warehouse schema with row-level security.
2. Added a SQLite-to-Postgres migration utility and migration dependencies.
3. Confirmed the Supabase connection and applied the schema.
4. Added a safe, authenticated SQLite snapshot endpoint and focused test.
5. Exported the production SQLite database and passed `PRAGMA quick_check`.
6. Imported the snapshot in one committed transaction and verified exact row
   counts in all ten warehouse tables: 1,211 spend rows, 267,810 sessions,
   141,547 touchpoints, 8,678 orders, 11,806 conversions, 0 reported values,
   2,330 ad names, 0 video metrics, 3 campaign settings, and 25 refunds.

Current stopping point:

1. Implement a Postgres data-access adapter behind the existing FastAPI API.
2. Run fixed-window SQLite/Postgres reports side by side and resolve every
   difference in orders, net revenue, refunds, leads, spend, clicks, and
   attribution.
3. Pause ingestion briefly, import the final SQLite delta, and switch reads and
   writes only after parity passes.
4. Keep the FastAPI service on Render during this database cutover. Moving API
   compute is a separate project.

## Direction and next work

1. Add a Postgres data-access adapter behind the existing FastAPI API.
2. Run SQLite and Postgres reports side by side for fixed date windows and
   compare orders, net revenue, refunds, leads, spend, clicks, and attribution.
3. Perform a short final ingestion pause, import the final delta, and switch the
   API only after parity checks pass.
4. Reconcile GHL lead definitions and identity stitching to reduce the reporting
   gap and match Hyros CPL using the same source records.
5. Verify browser lead and purchase events reach Vigil, Stape, Google, Meta, and
   TikTok with stable event IDs and deduplication.
6. Finish reports and CRM workflow testing across desktop and mobile.
7. Retire Render SQLite only after the Supabase deployment has stable ingestion,
   reporting, backups, and rollback coverage. Moving API compute off Render is a
   separate deployment project.

## Deployment and collaboration

- Work through reviewed commits on `main`; Vercel and Render currently deploy
  from the GitHub repository.
- Do not commit `.env`, database snapshots, access tokens, API keys, passwords,
  or Supabase connection strings.
- Rotate credentials that were shared in chat or screenshots.
- Before pushing, preserve collaborators' changes, run focused tests, and check
  the diff for secrets and unrelated file churn.

## Reference documents

- `docs/SUPABASE_MIGRATION.md`: production export and Supabase import procedure.
- `supabase/migrations/202607210001_initial_schema.sql`: target schema.
- `scripts/migrate_sqlite_to_supabase.py`: verified migration utility.
- `.env.example`: environment variable names only; never place values there.
