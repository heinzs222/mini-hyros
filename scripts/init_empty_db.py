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
            session_id TEXT, ts TEXT, utm_source TEXT, utm_medium TEXT,
            utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
            referrer TEXT, landing_page TEXT, device TEXT,
            gclid TEXT, fbclid TEXT, ttclid TEXT, customer_key TEXT
        );""")

        conn.execute("""CREATE TABLE touchpoints (
            ts TEXT, channel TEXT, platform TEXT, campaign_id TEXT,
            adset_id TEXT, ad_id TEXT, creative_id TEXT,
            gclid TEXT, fbclid TEXT, ttclid TEXT,
            customer_key TEXT, session_id TEXT
        );""")

        conn.execute("""CREATE TABLE orders (
            order_id TEXT, ts TEXT, gross TEXT, net TEXT,
            refunds TEXT, chargebacks TEXT, cogs TEXT, fees TEXT,
            customer_key TEXT, subscription_id TEXT
        );""")

        conn.execute("""CREATE TABLE conversions (
            conversion_id TEXT, ts TEXT, type TEXT, value TEXT,
            order_id TEXT, customer_key TEXT
        );""")

        conn.execute("""CREATE TABLE reported_value (
            platform TEXT, date TEXT, account_id TEXT, campaign_id TEXT,
            adset_id TEXT, ad_id TEXT, conversion_type TEXT, reported_value TEXT
        );""")

        conn.execute("""CREATE TABLE IF NOT EXISTS video_metrics (
            platform TEXT, date TEXT, campaign_id TEXT, adset_id TEXT,
            ad_id TEXT, creative_id TEXT, video_id TEXT, video_name TEXT,
            views TEXT, views_3s TEXT, views_25 TEXT, views_50 TEXT,
            views_75 TEXT, views_100 TEXT, avg_watch_time_sec TEXT,
            thumb_stop_rate TEXT, hook_rate TEXT, hold_rate TEXT,
            ctr TEXT, cost TEXT, cost_per_view TEXT
        );""")

        conn.commit()

    print(f"Empty database created at {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-path", default="data/dummy/attributionops_demo.sqlite")
    args = parser.parse_args()
    init_db(args.sqlite_path)
