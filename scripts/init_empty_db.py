#!/usr/bin/env python3
"""Create an empty SQLite database with the correct schema (no dummy data)."""

import argparse
import sqlite3
from pathlib import Path


def init_db(db_path: str) -> None:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        conn.execute("""CREATE TABLE spend (
            platform TEXT, date TEXT, account_id TEXT, campaign_id TEXT,
            adset_id TEXT, ad_id TEXT, creative_id TEXT,
            clicks TEXT, cost TEXT, impressions TEXT, metadata TEXT
        );""")

        conn.execute("""CREATE TABLE sessions (
            session_id TEXT, visitor_id TEXT, ts TEXT, event_name TEXT, page_title TEXT,
            utm_source TEXT, utm_medium TEXT,
            utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
            referrer TEXT, landing_page TEXT, device TEXT,
            gclid TEXT, fbclid TEXT, ttclid TEXT, customer_key TEXT,
            custom_data_json TEXT
        );""")

        conn.execute("""CREATE TABLE touchpoints (
            ts TEXT, channel TEXT, platform TEXT, campaign_id TEXT,
            adset_id TEXT, ad_id TEXT, creative_id TEXT,
            gclid TEXT, fbclid TEXT, ttclid TEXT,
            customer_key TEXT, session_id TEXT, visitor_id TEXT DEFAULT ''
        );""")

        conn.execute("""CREATE TABLE orders (
            order_id TEXT, ts TEXT, gross TEXT, net TEXT,
            refunds TEXT, chargebacks TEXT, cogs TEXT, fees TEXT,
            customer_key TEXT, subscription_id TEXT,
            session_id TEXT DEFAULT '', visitor_id TEXT DEFAULT '',
            channel TEXT DEFAULT '', platform TEXT DEFAULT '',
            campaign_id TEXT DEFAULT '', adset_id TEXT DEFAULT '',
            ad_id TEXT DEFAULT '', creative_id TEXT DEFAULT '',
            gclid TEXT DEFAULT '', fbclid TEXT DEFAULT '', ttclid TEXT DEFAULT ''
        );""")

        conn.execute("""CREATE TABLE conversions (
            conversion_id TEXT, ts TEXT, type TEXT, value TEXT,
            order_id TEXT, customer_key TEXT,
            session_id TEXT DEFAULT '', visitor_id TEXT DEFAULT ''
        );""")

        # Indexes for the warehouse access patterns used by the report/
        # attribution engine. The originals only covered customer_key/session_id/
        # visitor_id — there was no index on any ts/date column, so every
        # date-range filter (the dominant pattern) ran as a full table scan. Keep
        # in sync with scripts/generate_dummy_data.py (WAREHOUSE_INDEXES).
        conn.execute("CREATE INDEX idx_sessions_session_id ON sessions(session_id);")
        conn.execute("CREATE INDEX idx_sessions_visitor_id ON sessions(visitor_id);")
        conn.execute("CREATE INDEX idx_sessions_customer_key ON sessions(customer_key);")
        conn.execute("CREATE INDEX idx_sessions_ts ON sessions(ts);")
        # Composite covers the attribution customer+window scan and tracking EXISTS.
        conn.execute("CREATE INDEX idx_touchpoints_customer_key_ts ON touchpoints(customer_key, ts);")
        conn.execute("CREATE INDEX idx_touchpoints_session_id ON touchpoints(session_id);")
        conn.execute("CREATE INDEX idx_touchpoints_visitor_id ON touchpoints(visitor_id);")
        conn.execute("CREATE INDEX idx_touchpoints_customer_key ON touchpoints(customer_key);")
        conn.execute("CREATE INDEX idx_touchpoints_ts ON touchpoints(ts);")
        conn.execute("CREATE INDEX idx_touchpoints_campaign_id ON touchpoints(campaign_id);")
        conn.execute("CREATE INDEX idx_orders_customer_key ON orders(customer_key);")
        conn.execute("CREATE INDEX idx_orders_session_id ON orders(session_id);")
        conn.execute("CREATE INDEX idx_orders_visitor_id ON orders(visitor_id);")
        conn.execute("CREATE INDEX idx_orders_ts ON orders(ts);")
        # UNIQUE so the INSERT OR IGNORE idempotency the ingestion layer relies on
        # actually collapses duplicate webhook/sync deliveries (was a silent no-op).
        conn.execute("CREATE UNIQUE INDEX uq_orders_order_id ON orders(order_id);")
        conn.execute("CREATE INDEX idx_conversions_customer_key ON conversions(customer_key);")
        conn.execute("CREATE INDEX idx_conversions_ts ON conversions(ts);")
        conn.execute("CREATE INDEX idx_conversions_order_id ON conversions(order_id);")
        conn.execute("CREATE UNIQUE INDEX uq_conversions_conversion_id ON conversions(conversion_id);")
        conn.execute("CREATE INDEX idx_spend_date ON spend(date);")
        conn.execute("CREATE INDEX idx_spend_platform_date ON spend(platform, date);")

        conn.execute("""CREATE TABLE reported_value (
            platform TEXT, date TEXT, account_id TEXT, campaign_id TEXT,
            adset_id TEXT, ad_id TEXT, conversion_type TEXT, reported_value TEXT
        );""")

        conn.execute("CREATE INDEX idx_reported_value_date ON reported_value(date);")
        conn.execute("CREATE INDEX idx_reported_value_platform_date ON reported_value(platform, date);")

        conn.execute("""CREATE TABLE IF NOT EXISTS ad_names (
            platform TEXT,
            entity_type TEXT,
            entity_id TEXT,
            name TEXT,
            parent_id TEXT,
            source TEXT DEFAULT 'manual',
            updated_at TEXT,
            PRIMARY KEY (platform, entity_type, entity_id)
        );""")

        conn.execute("""CREATE TABLE IF NOT EXISTS video_metrics (
            platform TEXT, date TEXT, campaign_id TEXT, adset_id TEXT,
            ad_id TEXT, creative_id TEXT, video_id TEXT, video_name TEXT,
            views TEXT, views_3s TEXT, views_25 TEXT, views_50 TEXT,
            views_75 TEXT, views_100 TEXT, avg_watch_time_sec TEXT,
            thumb_stop_rate TEXT, hook_rate TEXT, hold_rate TEXT,
            ctr TEXT, cost TEXT, cost_per_view TEXT
        );""")

        # Per-campaign tracked/excluded flag (absence of a row == tracked).
        conn.execute("""CREATE TABLE IF NOT EXISTS campaign_settings (
            platform    TEXT NOT NULL,
            campaign_id TEXT NOT NULL,
            tracked     INTEGER NOT NULL DEFAULT 1,
            note        TEXT DEFAULT '',
            updated_at  TEXT,
            PRIMARY KEY (platform, campaign_id)
        );""")

        conn.commit()

    print(f"Empty database created at {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-path", default="data/dummy/attributionops_demo.sqlite")
    args = parser.parse_args()
    init_db(args.sqlite_path)
