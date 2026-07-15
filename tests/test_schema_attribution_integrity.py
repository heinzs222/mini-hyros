from __future__ import annotations

import sqlite3

from attributionops.schema import apply_migrations


def _row(db_path: str, sql: str, params: tuple = ()) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        result = conn.execute(sql, params).fetchone()
        assert result is not None
        return result


def test_sparse_order_update_preserves_existing_attribution(empty_db):
    with sqlite3.connect(empty_db) as conn:
        conn.execute(
            """INSERT INTO orders (
                   order_id, ts, gross, net, customer_key, session_id, visitor_id,
                   channel, platform, campaign_id, fbclid, is_recurring, sale_group_id
               ) VALUES (?, ?, '100', '100', ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
            (
                "order-1",
                "2026-07-10T12:00:00Z",
                "customer-1",
                "session-1",
                "visitor-1",
                "paid_social",
                "meta",
                "campaign-1",
                "fb-click-1",
                "order-1",
            ),
        )
        apply_migrations(conn)
        conn.execute(
            """UPDATE orders
                  SET session_id = '', visitor_id = '', channel = '', platform = '',
                      campaign_id = '', fbclid = ''
                WHERE order_id = 'order-1'"""
        )
        conn.commit()

    row = _row(
        empty_db,
        """SELECT session_id, visitor_id, channel, platform, campaign_id, fbclid
             FROM orders WHERE order_id = 'order-1'""",
    )
    assert dict(row) == {
        "session_id": "session-1",
        "visitor_id": "visitor-1",
        "channel": "paid_social",
        "platform": "meta",
        "campaign_id": "campaign-1",
        "fbclid": "fb-click-1",
    }


def test_recurring_order_gets_latest_prior_customer_touchpoint(empty_db):
    with sqlite3.connect(empty_db) as conn:
        conn.execute(
            """INSERT INTO touchpoints (
                   ts, channel, platform, campaign_id, adset_id, ad_id, creative_id,
                   gclid, fbclid, ttclid, customer_key, session_id, visitor_id
               ) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, '', ?, ?, ?)""",
            (
                "2026-01-10T12:00:00Z",
                "paid_social",
                "meta",
                "campaign-old",
                "adset-old",
                "ad-old",
                "creative-old",
                "fb-old",
                "customer-2",
                "session-old",
                "visitor-old",
            ),
        )
        conn.execute(
            """INSERT INTO touchpoints (
                   ts, channel, platform, campaign_id, adset_id, ad_id, creative_id,
                   gclid, fbclid, ttclid, customer_key, session_id, visitor_id
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?)""",
            (
                "2026-05-10T12:00:00Z",
                "paid_search",
                "google",
                "campaign-new",
                "adset-new",
                "ad-new",
                "creative-new",
                "g-new",
                "customer-2",
                "session-new",
                "visitor-new",
            ),
        )
        conn.execute(
            """INSERT INTO orders (
                   order_id, ts, gross, net, customer_key, is_recurring, sale_group_id
               ) VALUES (?, ?, '50', '50', ?, 1, ?)""",
            ("order-recurring", "2026-07-10T12:00:00Z", "customer-2", "order-recurring"),
        )
        apply_migrations(conn)
        conn.commit()

    row = _row(
        empty_db,
        """SELECT session_id, visitor_id, channel, platform, campaign_id,
                  adset_id, ad_id, creative_id, gclid, fbclid
             FROM orders WHERE order_id = 'order-recurring'""",
    )
    assert dict(row) == {
        "session_id": "session-new",
        "visitor_id": "visitor-new",
        "channel": "paid_search",
        "platform": "google",
        "campaign_id": "campaign-new",
        "adset_id": "adset-new",
        "ad_id": "ad-new",
        "creative_id": "creative-new",
        "gclid": "g-new",
        "fbclid": "",
    }


def test_non_recurring_order_does_not_get_lifetime_source(empty_db):
    with sqlite3.connect(empty_db) as conn:
        conn.execute(
            """INSERT INTO touchpoints (
                   ts, channel, platform, campaign_id, customer_key, session_id, visitor_id
               ) VALUES (?, 'paid_social', 'meta', 'campaign-3', ?, 'session-3', 'visitor-3')""",
            ("2026-01-10T12:00:00Z", "customer-3"),
        )
        conn.execute(
            """INSERT INTO orders (
                   order_id, ts, gross, net, customer_key, is_recurring, sale_group_id
               ) VALUES (?, ?, '100', '100', ?, 0, ?)""",
            ("order-new", "2026-07-10T12:00:00Z", "customer-3", "order-new"),
        )
        apply_migrations(conn)
        conn.commit()

    row = _row(
        empty_db,
        "SELECT session_id, platform, campaign_id FROM orders WHERE order_id = 'order-new'",
    )
    assert dict(row) == {"session_id": "", "platform": "", "campaign_id": ""}


def test_order_and_purchase_conversion_share_identity(empty_db):
    with sqlite3.connect(empty_db) as conn:
        conn.execute(
            """INSERT INTO orders (
                   order_id, ts, gross, net, customer_key, sale_group_id
               ) VALUES ('order-linked', '2026-07-10T12:00:00Z', '100', '100', '', 'order-linked')"""
        )
        conn.execute(
            """INSERT INTO conversions (
                   conversion_id, ts, type, value, order_id, customer_key, session_id, visitor_id
               ) VALUES (
                   'conversion-linked', '2026-07-10T12:00:01Z', 'Purchase', '100',
                   'order-linked', 'customer-linked', 'session-linked', 'visitor-linked'
               )"""
        )
        apply_migrations(conn)
        conn.commit()

    order = _row(
        empty_db,
        "SELECT customer_key, session_id, visitor_id FROM orders WHERE order_id = 'order-linked'",
    )
    assert dict(order) == {
        "customer_key": "customer-linked",
        "session_id": "session-linked",
        "visitor_id": "visitor-linked",
    }
