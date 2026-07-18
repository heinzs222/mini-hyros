-- Mini Hyros — Postgres (Supabase) schema
-- =========================================================================
-- This is the authoritative schema for the Postgres/Supabase backend. It is the
-- hand-ported equivalent of the SQLite schema that scripts/init_empty_db.py +
-- attributionops/schema.py build for local development. Apply it ONCE to a fresh
-- Supabase database (see DEPLOY.md). The application never runs SQLite-style
-- PRAGMA/migration DDL against Postgres — every ensure_* path is a no-op when a
-- Postgres DSN is configured — so this file is the single source of truth for
-- the production schema.
--
-- Column types intentionally mirror SQLite: numeric-ish values are stored as
-- text (the ingestion layer writes them as strings) and read with the lenient
-- sqlite_real()/sqlite_int() helpers below, reproducing SQLite's forgiving CAST.

-- ── Lenient numeric casts (mimic SQLite CAST(x AS REAL/INTEGER)) ─────────────
-- SQLite coerces empty/garbage text to 0 and parses a leading numeric prefix;
-- Postgres raises. The dialect translator rewrites CAST(x AS REAL) -> sqlite_real(x)
-- and CAST(x AS INTEGER) -> sqlite_int(x) so those queries keep their semantics.
CREATE OR REPLACE FUNCTION sqlite_real(v anyelement)
RETURNS double precision
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    m text;
BEGIN
    IF v IS NULL THEN
        RETURN NULL;
    END IF;
    -- Leading signed decimal / exponent prefix, like SQLite's numeric parse.
    m := (regexp_match(v::text, '^[[:space:]]*[-+]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][-+]?[0-9]+)?'))[1];
    IF m IS NULL THEN
        RETURN 0;
    END IF;
    RETURN m::double precision;
EXCEPTION WHEN others THEN
    RETURN 0;
END;
$$;

CREATE OR REPLACE FUNCTION sqlite_int(v anyelement)
RETURNS bigint
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN trunc(sqlite_real(v))::bigint;
END;
$$;

-- ── SQLite strftime() emulation ─────────────────────────────────────────────
-- The warehouse uses strftime('%Y-%m-%dT%H:%M:%SZ', ts, '-N days') for the
-- tracking source-lookback window. Reproduce it: convert the SQLite format
-- string to a to_char() pattern (quoting literal letters like the T/Z) and
-- apply the optional relative modifier as an interval.
CREATE OR REPLACE FUNCTION _mh_strftime_fmt(fmt text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    out text := '';
    i int := 1;
    n int := length(fmt);
    ch text;
    code text;
BEGIN
    WHILE i <= n LOOP
        ch := substr(fmt, i, 1);
        IF ch = '%' AND i < n THEN
            code := substr(fmt, i + 1, 1);
            out := out || CASE code
                WHEN 'Y' THEN 'YYYY'
                WHEN 'm' THEN 'MM'
                WHEN 'd' THEN 'DD'
                WHEN 'H' THEN 'HH24'
                WHEN 'M' THEN 'MI'
                WHEN 'S' THEN 'SS'
                WHEN 'j' THEN 'DDD'
                WHEN 'W' THEN 'IW'
                WHEN 'w' THEN 'D'
                WHEN '%' THEN '"%"'
                ELSE code
            END;
            i := i + 2;
        ELSIF ch ~ '[A-Za-z]' THEN
            -- Literal letter: quote so to_char treats it verbatim.
            out := out || '"' || ch || '"';
            i := i + 1;
        ELSE
            out := out || ch;
            i := i + 1;
        END IF;
    END LOOP;
    RETURN out;
END;
$$;

CREATE OR REPLACE FUNCTION strftime(fmt text, ts text, modifier text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    tsv timestamptz;
BEGIN
    IF ts IS NULL OR ts = '' THEN
        RETURN NULL;
    END IF;
    BEGIN
        tsv := ts::timestamptz;
    EXCEPTION WHEN others THEN
        RETURN NULL;
    END;
    IF modifier IS NOT NULL AND modifier <> '' THEN
        tsv := tsv + modifier::interval;
    END IF;
    RETURN to_char(tsv AT TIME ZONE 'UTC', _mh_strftime_fmt(fmt));
END;
$$;

CREATE OR REPLACE FUNCTION strftime(fmt text, ts text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN strftime(fmt, ts, NULL);
END;
$$;

-- ── Warehouse tables ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS spend (
    platform text, date text, account_id text, campaign_id text,
    adset_id text, ad_id text, creative_id text,
    clicks text, cost text, impressions text, metadata text,
    currency text DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id text, visitor_id text, ts text, event_name text, page_title text,
    utm_source text, utm_medium text,
    utm_campaign text, utm_content text, utm_term text,
    referrer text, landing_page text, device text,
    gclid text, fbclid text, ttclid text, customer_key text,
    custom_data_json text
);

CREATE TABLE IF NOT EXISTS touchpoints (
    ts text, channel text, platform text, campaign_id text,
    adset_id text, ad_id text, creative_id text,
    gclid text, fbclid text, ttclid text,
    customer_key text, session_id text, visitor_id text DEFAULT ''
);

CREATE TABLE IF NOT EXISTS orders (
    order_id text, ts text, gross text, net text,
    refunds text, chargebacks text, cogs text, fees text,
    customer_key text, subscription_id text,
    session_id text DEFAULT '', visitor_id text DEFAULT '',
    channel text DEFAULT '', platform text DEFAULT '',
    campaign_id text DEFAULT '', adset_id text DEFAULT '',
    ad_id text DEFAULT '', creative_id text DEFAULT '',
    gclid text DEFAULT '', fbclid text DEFAULT '', ttclid text DEFAULT '',
    currency text DEFAULT '', processor text DEFAULT '',
    processor_customer_id text DEFAULT '', payment_fingerprint text DEFAULT '',
    product_key text DEFAULT '', is_recurring integer DEFAULT 0,
    sale_group_id text DEFAULT ''
);

CREATE TABLE IF NOT EXISTS conversions (
    conversion_id text, ts text, type text, value text,
    order_id text, customer_key text,
    session_id text DEFAULT '', visitor_id text DEFAULT ''
);

CREATE TABLE IF NOT EXISTS reported_value (
    platform text, date text, account_id text, campaign_id text,
    adset_id text, ad_id text, conversion_type text, reported_value text
);

CREATE TABLE IF NOT EXISTS ad_names (
    platform text,
    entity_type text,
    entity_id text,
    name text,
    parent_id text,
    source text DEFAULT 'manual',
    updated_at text,
    thumbnail_url text,
    creative_type text,
    video_id text,
    creative_id text,
    PRIMARY KEY (platform, entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS video_metrics (
    platform text, date text, campaign_id text, adset_id text,
    ad_id text, creative_id text, video_id text, video_name text,
    views text, views_3s text, views_25 text, views_50 text,
    views_75 text, views_100 text, avg_watch_time_sec text,
    thumb_stop_rate text, hook_rate text, hold_rate text,
    ctr text, cost text, cost_per_view text
);

CREATE TABLE IF NOT EXISTS campaign_settings (
    platform    text NOT NULL,
    campaign_id text NOT NULL,
    tracked     integer NOT NULL DEFAULT 1,
    note        text DEFAULT '',
    updated_at  text,
    PRIMARY KEY (platform, campaign_id)
);

CREATE TABLE IF NOT EXISTS refund_log (
    id text PRIMARY KEY,
    ts text,
    order_id text,
    customer_key text,
    type text,
    amount text,
    reason text,
    source text
);

CREATE TABLE IF NOT EXISTS platform_tokens (
    platform text PRIMARY KEY,
    access_token text,
    refresh_token text,
    advertiser_id text,
    expires_at text,
    updated_at text
);

CREATE TABLE IF NOT EXISTS email_sms_events (
    id text PRIMARY KEY,
    ts text,
    customer_key text,
    channel text,
    event_type text,
    sequence_name text,
    step_number text,
    subject_line text,
    link_url text,
    source text
);

CREATE TABLE IF NOT EXISTS capi_log (
    id text PRIMARY KEY,
    ts text,
    platform text,
    event_name text,
    customer_key text,
    order_id text,
    value text,
    status text,
    response text,
    error text
);

CREATE TABLE IF NOT EXISTS stripe_sync_coverage (
    start_date text NOT NULL,
    end_date text NOT NULL,
    updated_at text NOT NULL,
    PRIMARY KEY (start_date, end_date)
);

CREATE TABLE IF NOT EXISTS webhook_log (
    id text PRIMARY KEY,
    ts text,
    source text,
    event_type text,
    payload text,
    result text
);

-- ── Indexes ─────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_visitor_id ON sessions(visitor_id);
CREATE INDEX IF NOT EXISTS idx_sessions_customer_key ON sessions(customer_key);
CREATE INDEX IF NOT EXISTS idx_sessions_ts ON sessions(ts);
CREATE INDEX IF NOT EXISTS idx_touchpoints_customer_key_ts ON touchpoints(customer_key, ts);
CREATE INDEX IF NOT EXISTS idx_touchpoints_session_id ON touchpoints(session_id);
CREATE INDEX IF NOT EXISTS idx_touchpoints_visitor_id ON touchpoints(visitor_id);
CREATE INDEX IF NOT EXISTS idx_touchpoints_customer_key ON touchpoints(customer_key);
CREATE INDEX IF NOT EXISTS idx_touchpoints_ts ON touchpoints(ts);
CREATE INDEX IF NOT EXISTS idx_touchpoints_campaign_id ON touchpoints(campaign_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_key ON orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_orders_session_id ON orders(session_id);
CREATE INDEX IF NOT EXISTS idx_orders_visitor_id ON orders(visitor_id);
CREATE INDEX IF NOT EXISTS idx_orders_ts ON orders(ts);
CREATE INDEX IF NOT EXISTS idx_orders_sale_group_id ON orders(sale_group_id);
CREATE INDEX IF NOT EXISTS idx_orders_processor_customer_id ON orders(processor_customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_payment_fingerprint ON orders(payment_fingerprint);
CREATE INDEX IF NOT EXISTS idx_orders_is_recurring ON orders(is_recurring);
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_order_id ON orders(order_id);
CREATE INDEX IF NOT EXISTS idx_conversions_customer_key ON conversions(customer_key);
CREATE INDEX IF NOT EXISTS idx_conversions_ts ON conversions(ts);
CREATE INDEX IF NOT EXISTS idx_conversions_order_id ON conversions(order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_conversions_conversion_id ON conversions(conversion_id);
CREATE INDEX IF NOT EXISTS idx_spend_date ON spend(date);
CREATE INDEX IF NOT EXISTS idx_spend_platform_date ON spend(platform, date);
CREATE INDEX IF NOT EXISTS idx_reported_value_date ON reported_value(date);
CREATE INDEX IF NOT EXISTS idx_reported_value_platform_date ON reported_value(platform, date);
CREATE INDEX IF NOT EXISTS idx_refund_log_order_id_ts ON refund_log(order_id, ts);
CREATE INDEX IF NOT EXISTS idx_refund_log_ts ON refund_log(ts);

-- ── Parity default campaign exclusions (mirrors DEFAULT_CAMPAIGN_SETTINGS) ────
INSERT INTO campaign_settings (platform, campaign_id, tracked, note, updated_at)
VALUES
    ('google', '21892266666', 0, 'Default Hyros parity exclusion', now()::text),
    ('google', '23351447961', 0, 'Default Hyros parity exclusion', now()::text),
    ('meta',   '120237922106660149', 0, 'Default Hyros parity exclusion', now()::text)
ON CONFLICT (platform, campaign_id) DO NOTHING;

-- ── Recurring-order attribution triggers (ported from the SQLite triggers) ───
-- Latest qualifying touchpoint for a recurring order's buyer at/before its ts.
-- The SQLite version filled each attribution column from an identical correlated
-- subquery, so every column resolves to the same touchpoint row — captured once
-- here. rowid tiebreak becomes ctid.
CREATE OR REPLACE FUNCTION _mh_backfill_recurring(o orders)
RETURNS orders
LANGUAGE plpgsql AS $$
DECLARE
    tp touchpoints%ROWTYPE;
BEGIN
    IF COALESCE(o.is_recurring, 0) <> 1 OR COALESCE(o.customer_key, '') = '' THEN
        RETURN o;
    END IF;
    SELECT t.* INTO tp
    FROM touchpoints t
    WHERE COALESCE(t.customer_key, '') = COALESCE(o.customer_key, '')
      AND t.ts <= o.ts
      AND (
            LOWER(COALESCE(t.channel, '')) NOT IN ('', 'organic', 'direct')
            OR COALESCE(t.platform, '') <> ''
            OR COALESCE(t.campaign_id, '') <> ''
            OR COALESCE(t.adset_id, '') <> ''
            OR COALESCE(t.ad_id, '') <> ''
            OR COALESCE(t.creative_id, '') <> ''
            OR COALESCE(t.gclid, '') <> ''
            OR COALESCE(t.fbclid, '') <> ''
            OR COALESCE(t.ttclid, '') <> ''
      )
    ORDER BY t.ts DESC, t.ctid DESC
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN o;
    END IF;
    IF COALESCE(o.session_id, '') = '' THEN o.session_id := COALESCE(tp.session_id, ''); END IF;
    IF COALESCE(o.visitor_id, '') = '' THEN o.visitor_id := COALESCE(tp.visitor_id, ''); END IF;
    IF COALESCE(o.channel, '')     = '' THEN o.channel     := COALESCE(tp.channel, ''); END IF;
    IF COALESCE(o.platform, '')    = '' THEN o.platform    := COALESCE(tp.platform, ''); END IF;
    IF COALESCE(o.campaign_id, '') = '' THEN o.campaign_id := COALESCE(tp.campaign_id, ''); END IF;
    IF COALESCE(o.adset_id, '')    = '' THEN o.adset_id    := COALESCE(tp.adset_id, ''); END IF;
    IF COALESCE(o.ad_id, '')       = '' THEN o.ad_id       := COALESCE(tp.ad_id, ''); END IF;
    IF COALESCE(o.creative_id, '') = '' THEN o.creative_id := COALESCE(tp.creative_id, ''); END IF;
    IF COALESCE(o.gclid, '')       = '' THEN o.gclid       := COALESCE(tp.gclid, ''); END IF;
    IF COALESCE(o.fbclid, '')      = '' THEN o.fbclid      := COALESCE(tp.fbclid, ''); END IF;
    IF COALESCE(o.ttclid, '')      = '' THEN o.ttclid      := COALESCE(tp.ttclid, ''); END IF;
    RETURN o;
END;
$$;

CREATE OR REPLACE FUNCTION trg_orders_backfill_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW := _mh_backfill_recurring(NEW);
    RETURN NEW;
END;
$$;

-- On UPDATE: first preserve any previously-populated attribution that the new
-- row would blank (sparse re-syncs must never erase captured attribution), then
-- backfill recurring orders from touchpoints.
CREATE OR REPLACE FUNCTION trg_orders_preserve_and_backfill_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF COALESCE(NEW.session_id, '') = '' THEN NEW.session_id := OLD.session_id; END IF;
    IF COALESCE(NEW.visitor_id, '') = '' THEN NEW.visitor_id := OLD.visitor_id; END IF;
    IF COALESCE(NEW.channel, '')     = '' THEN NEW.channel     := OLD.channel; END IF;
    IF COALESCE(NEW.platform, '')    = '' THEN NEW.platform    := OLD.platform; END IF;
    IF COALESCE(NEW.campaign_id, '') = '' THEN NEW.campaign_id := OLD.campaign_id; END IF;
    IF COALESCE(NEW.adset_id, '')    = '' THEN NEW.adset_id    := OLD.adset_id; END IF;
    IF COALESCE(NEW.ad_id, '')       = '' THEN NEW.ad_id       := OLD.ad_id; END IF;
    IF COALESCE(NEW.creative_id, '') = '' THEN NEW.creative_id := OLD.creative_id; END IF;
    IF COALESCE(NEW.gclid, '')       = '' THEN NEW.gclid       := OLD.gclid; END IF;
    IF COALESCE(NEW.fbclid, '')      = '' THEN NEW.fbclid      := OLD.fbclid; END IF;
    IF COALESCE(NEW.ttclid, '')      = '' THEN NEW.ttclid      := OLD.ttclid; END IF;
    NEW := _mh_backfill_recurring(NEW);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_orders_backfill_insert ON orders;
CREATE TRIGGER trg_orders_backfill_insert
    BEFORE INSERT ON orders
    FOR EACH ROW EXECUTE FUNCTION trg_orders_backfill_insert();

DROP TRIGGER IF EXISTS trg_orders_preserve_and_backfill_update ON orders;
CREATE TRIGGER trg_orders_preserve_and_backfill_update
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION trg_orders_preserve_and_backfill_update();
