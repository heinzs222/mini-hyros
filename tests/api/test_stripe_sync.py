"""Tests for the Stripe order sync endpoints (/api/stripe).

Covers:
  - POST /api/stripe/sync   — fetch charges (respx-mocked), write orders +
                              conversions, dedupe, pagination, missing key,
                              API-error handling.
  - GET  /api/stripe/status — connection status (respx-mocked) + missing key.

All outbound HTTP to api.stripe.com is mocked with respx; nothing hits the
real network.
"""

from __future__ import annotations

from datetime import datetime, timezone

import respx
from httpx import Response

from attributionops.db import sql_rows


def _charge(charge_id, *, amount=10000, paid=True, refunded=False,
            amount_refunded=0, email="buyer@example.com", created=1735689600,
            currency="usd", subscription=None, refunds=None):
    """Build a minimal Stripe charge object as the code expects to parse it."""
    if refunds is None and amount_refunded:
        refunds = [
            {
                "id": f"re_{charge_id}",
                "amount": amount_refunded,
                "created": created + 3600,
                "status": "succeeded",
            }
        ]
    return {
        "id": charge_id,
        "amount": amount,
        "amount_refunded": amount_refunded,
        "paid": paid,
        "refunded": refunded,
        "currency": currency,
        "created": created,
        "subscription": subscription,
        "billing_details": {"email": email} if email else {},
        "refunds": {"object": "list", "data": refunds or [], "has_more": False},
    }


def _charges_page(charges, has_more=False):
    return {"object": "list", "data": charges, "has_more": has_more}


# ── sync: success ────────────────────────────────────────────────────────────

def test_sync_writes_orders_and_conversions(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")

    payload = _charges_page([
        _charge("ch_1", amount=10000, email="a@example.com"),
        _charge("ch_2", amount=5000, email="b@example.com"),
    ])

    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        r = client.post(
            "/api/stripe/sync",
            params={"start_date": "2025-01-01", "end_date": "2025-01-31"},
        )

    assert r.status_code == 200
    assert route.called
    body = r.json()
    assert body["synced"] == 2
    assert body["updated"] == 0
    assert body["fetched"] == 2
    assert body["start_date"] == "2025-01-01"
    assert body["end_date"] == "2025-01-31"

    orders = sql_rows(api_db, "SELECT * FROM orders")
    assert len(orders) == 2
    # gross is dollars (amount/100), fees use the 2.9% + 0.30 estimate.
    by_gross = sorted(float(o["gross"]) for o in orders)
    assert by_gross == [50.0, 100.0]
    assert all(o["customer_key"] for o in orders)  # email -> hashed customer_key

    convs = sql_rows(api_db, "SELECT * FROM conversions WHERE type = 'Purchase'")
    assert len(convs) == 2
    # Each conversion links to an order that exists.
    order_ids = {o["order_id"] for o in orders}
    assert all(c["order_id"] in order_ids for c in convs)


def test_sync_includes_refunded_filters_unpaid_and_zero(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    payload = _charges_page([
        _charge("ch_ok", amount=2000),
        _charge("ch_refunded", amount=2000, refunded=True, amount_refunded=2000),
        _charge("ch_unpaid", amount=2000, paid=False),
        _charge("ch_zero", amount=0),
    ])
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        body = client.post("/api/stripe/sync").json()

    # Refunded charges are now KEPT (so their refund reduces net revenue); only
    # unpaid and zero-amount charges are excluded.
    assert body["fetched"] == 2
    assert body["synced"] == 2
    assert body["refund_events"] == 1
    orders = sql_rows(api_db, "SELECT * FROM orders")
    assert len(orders) == 2
    refunded = sql_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ["stripe|ch_refunded"])[0]
    assert float(refunded["gross"]) == 20.0
    assert float(refunded["refunds"]) == 20.0
    assert float(refunded["net"]) == 0.0
    refund_events = sql_rows(
        api_db,
        "SELECT id, ts, order_id, amount, source FROM refund_log",
    )
    assert refund_events == [
        {
            "id": "re_ch_refunded",
            "ts": "2025-01-01T01:00:00Z",
            "order_id": "stripe|ch_refunded",
            "amount": "20.0",
            "source": "stripe_sync",
        }
    ]


def test_sync_applies_configured_recurring_amount_rule(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    monkeypatch.setenv("STRIPE_RECURRING_AMOUNT_KEYS", "CAD:5000")
    payload = _charges_page([
        _charge("ch_plan_repeat", amount=5000, currency="cad", email="plan@example.com", created=1769904000),
        _charge("ch_other", amount=5000, currency="usd", email="other@example.com", created=1769904000),
        # Account-specific amount rules allow a shorter repeat cycle, but only
        # when a prior charge has the same canonical identity.
        _charge("ch_plan_first", amount=5000, currency="cad", email="plan@example.com", created=1769299200),
    ])
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        body = client.post("/api/stripe/sync").json()
        second_body = client.post("/api/stripe/sync").json()

    assert body["recurring"] == 1
    assert second_body["recurring"] == 1
    rows = sql_rows(api_db, "SELECT order_id, is_recurring FROM orders ORDER BY order_id")
    assert {row["order_id"]: row["is_recurring"] for row in rows} == {
        "stripe|ch_other": 0,
        "stripe|ch_plan_first": 0,
        "stripe|ch_plan_repeat": 1,
    }


def test_sync_does_not_downgrade_existing_recurring_order(
    client, api_db, monkeypatch
):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    recurring_charge = _charge("ch_recurring", amount=5000)
    recurring_charge["invoice"] = {
        "billing_reason": "subscription_cycle",
        "subscription": "sub_123",
    }
    incomplete_charge = _charge("ch_recurring", amount=5000)

    responses = iter(
        [
            Response(200, json=_charges_page([recurring_charge])),
            Response(200, json=_charges_page([incomplete_charge])),
        ]
    )
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            side_effect=lambda request: next(responses)
        )
        first = client.post("/api/stripe/sync").json()
        second = client.post("/api/stripe/sync").json()

    assert first["recurring"] == 1
    assert second["recurring"] == 1
    row = sql_rows(
        api_db,
        "SELECT is_recurring FROM orders WHERE order_id = ?",
        ["stripe|ch_recurring"],
    )[0]
    assert row["is_recurring"] == 1


def test_sync_dedupes_on_second_run(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    payload = _charges_page([_charge("ch_dup", amount=7000)])

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        first = client.post("/api/stripe/sync").json()
        second = client.post("/api/stripe/sync").json()

    assert first["synced"] == 1 and first["updated"] == 0
    # Second run re-syncs the same charge as an UPDATE (refresh refunds/net),
    # not a new insert.
    assert second["synced"] == 0 and second["updated"] == 1
    # Still exactly one order in the warehouse.
    assert len(sql_rows(api_db, "SELECT * FROM orders WHERE order_id != ''")) == 1


def test_sync_paginates_with_has_more(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    page1 = _charges_page([_charge("ch_p1", amount=1000)], has_more=True)
    page2 = _charges_page([_charge("ch_p2", amount=2000)], has_more=False)

    responses = iter([Response(200, json=page1), Response(200, json=page2)])

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            side_effect=lambda request: next(responses)
        )
        body = client.post("/api/stripe/sync").json()

    assert body["fetched"] == 2
    assert body["synced"] == 2
    assert len(sql_rows(api_db, "SELECT * FROM orders")) == 2


def test_sync_uses_reporting_timezone_for_stripe_query_bounds(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")

    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=_charges_page([]))
        )
        body = client.post(
            "/api/stripe/sync",
            params={"start_date": "2026-06-19", "end_date": "2026-06-25"},
        ).json()

    request = route.calls[0].request
    assert int(request.url.params["created[gte]"]) == 1781848800  # Jun 19 06:00 UTC
    assert int(request.url.params["created[lte]"]) == 1782453599  # Jun 26 05:59:59 UTC
    assert body["query_start_utc"] == "2026-06-19T06:00:00Z"
    assert body["query_end_utc"] == "2026-06-26T05:59:59Z"


def test_sync_backfills_customer_history_once_for_recurring_classification(
    client, api_db, monkeypatch
):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    monkeypatch.setenv("STRIPE_HISTORY_BACKFILL_DAYS", "365")

    history_charge = _charge(
        "ch_history",
        amount=12000,
        email="returning@example.com",
        created=int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()),
    )
    history_charge["description"] = "Guard training"
    selected_charge = _charge(
        "ch_selected",
        amount=12000,
        email="returning@example.com",
        created=int(datetime(2026, 7, 5, tzinfo=timezone.utc).timestamp()),
    )
    selected_charge["description"] = "Guard training"

    responses = iter([
        Response(200, json=_charges_page([history_charge])),
        Response(200, json=_charges_page([selected_charge])),
        Response(200, json=_charges_page([selected_charge])),
    ])
    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            side_effect=lambda request: next(responses)
        )
        first = client.post(
            "/api/stripe/sync",
            params={"start_date": "2026-07-04", "end_date": "2026-07-11"},
        ).json()
        second = client.post(
            "/api/stripe/sync",
            params={"start_date": "2026-07-04", "end_date": "2026-07-11"},
        ).json()

    assert route.call_count == 3
    assert first["history_fetched"] == 1
    assert first["history_synced"] == 1
    assert first["recurring"] == 1
    assert second["history_fetched"] == 0
    assert second["history_ranges"] == []

    selected = sql_rows(
        api_db,
        "SELECT is_recurring FROM orders WHERE order_id = 'stripe|ch_selected'",
    )[0]
    assert selected["is_recurring"] == 1
    coverage = sql_rows(
        api_db,
        "SELECT start_date, end_date FROM stripe_sync_coverage",
    )
    assert [(row["start_date"], row["end_date"]) for row in coverage] == [
        ("2025-07-04", "2026-07-11")
    ]


def test_sync_uses_fallback_secret_key_env(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_API_SECRET_KEY", raising=False)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_fallback")
    payload = _charges_page([_charge("ch_fb", amount=3000)])
    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        body = client.post("/api/stripe/sync").json()
    assert route.called
    assert body["synced"] == 1


def test_sync_extracts_email_from_receipt_and_metadata(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    # No billing_details email -> should fall through to receipt_email.
    ch = _charge("ch_receipt", amount=4000, email=None)
    ch["receipt_email"] = "RECEIPT@Example.com"
    payload = _charges_page([ch])
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        client.post("/api/stripe/sync")
    orders = sql_rows(api_db, "SELECT customer_key FROM orders")
    assert len(orders) == 1
    assert orders[0]["customer_key"] != ""  # email normalized + hashed


# ── sync: error paths ────────────────────────────────────────────────────────

def test_sync_preserves_metadata_attribution_and_stitches_browser_path(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")

    from tests.helpers import insert_rows, touchpoint

    insert_rows(api_db, "sessions", [{
        "session_id": "sess-stripe",
        "visitor_id": "vis-stripe",
        "ts": "2026-06-01T00:00:00Z",
        "customer_key": "",
    }])
    insert_rows(api_db, "touchpoints", [
        touchpoint(
            "2026-06-01T00:00:00Z",
            "",
            platform="google",
            channel="paid_search",
            session_id="sess-stripe",
            visitor_id="vis-stripe",
            gclid="g-click",
        )
    ])

    ch = _charge("ch_attr", amount=12345, email=None)
    ch["metadata"] = {
        "email": "META@Example.com",
        "hyros_session_id": "sess-stripe",
        "hyros_visitor_id": "vis-stripe",
    }
    ch["payment_intent"] = {
        "metadata": {
            "utm_source": "google",
            "gclid": "g-click",
            "campaign_id": "camp-123",
            "ad_id": "ad-123",
            "creative_id": "creative-123",
        }
    }

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=_charges_page([ch]))
        )
        body = client.post("/api/stripe/sync").json()

    assert body["synced"] == 1

    orders = sql_rows(api_db, "SELECT * FROM orders")
    assert len(orders) == 1
    assert orders[0]["session_id"] == "sess-stripe"
    assert orders[0]["visitor_id"] == "vis-stripe"
    assert orders[0]["platform"] == "google"
    assert orders[0]["channel"] == "paid_search"
    assert orders[0]["campaign_id"] == "camp-123"
    assert orders[0]["ad_id"] == "ad-123"
    assert orders[0]["creative_id"] == "creative-123"
    assert orders[0]["gclid"] == "g-click"

    convs = sql_rows(api_db, "SELECT * FROM conversions WHERE type = 'Purchase'")
    assert len(convs) == 1
    assert convs[0]["session_id"] == "sess-stripe"
    assert convs[0]["visitor_id"] == "vis-stripe"
    assert convs[0]["customer_key"] == orders[0]["customer_key"]

    sessions = sql_rows(api_db, "SELECT * FROM sessions WHERE session_id = 'sess-stripe'")
    touchpoints = sql_rows(api_db, "SELECT * FROM touchpoints WHERE session_id = 'sess-stripe'")
    assert sessions[0]["customer_key"] == orders[0]["customer_key"]
    assert touchpoints[0]["customer_key"] == orders[0]["customer_key"]


def test_sync_missing_key(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_API_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    r = client.post("/api/stripe/sync")
    assert r.status_code == 200
    body = r.json()
    assert body["synced"] == 0
    assert "STRIPE_API_SECRET_KEY" in body["error"]


def test_sync_skips_charge_without_id(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    ch_no_id = _charge("", amount=5000)  # empty id -> skipped during write
    ch_ok = _charge("ch_real", amount=5000)
    payload = _charges_page([ch_no_id, ch_ok])
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json=payload)
        )
        body = client.post("/api/stripe/sync").json()
    # Both pass the fetch filter, but the id-less one is dropped on write.
    assert body["fetched"] == 2
    assert body["synced"] == 1
    assert len(sql_rows(api_db, "SELECT * FROM orders")) == 1


def test_sync_handles_generic_exception(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")
    # Non-JSON body makes resp.json() raise -> caught by the generic except.
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, content=b"not json")
        )
        body = client.post("/api/stripe/sync").json()
    assert body["synced"] == 0
    assert "error" in body


def test_sync_handles_stripe_api_error(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_bad")
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(401, json={"error": {"message": "Invalid API Key"}})
        )
        body = client.post("/api/stripe/sync").json()
    assert body["synced"] == 0
    assert "Stripe API error" in body["error"]
    # Nothing written on failure.
    assert sql_rows(api_db, "SELECT * FROM orders") == []


# ── status ───────────────────────────────────────────────────────────────────

def test_status_missing_key(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_API_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    body = client.get("/api/stripe/status").json()
    assert body["connected"] is False
    assert "STRIPE_API_SECRET_KEY not set" in body["error"]


def test_status_connected(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_abcdef123456")
    # Seed an order so orders_in_db / last_order_ts are populated.
    from tests.helpers import insert_rows, order
    insert_rows(api_db, "orders", [order("o1", "2026-01-01T00:00:00Z", "ck", gross="10")])

    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(200, json={"object": "list", "data": [], "has_more": False})
        )
        body = client.get("/api/stripe/status").json()

    assert route.called
    assert body["connected"] is True
    assert body["orders_in_db"] == 1
    assert body["last_order_ts"] == "2026-01-01T00:00:00Z"
    assert body["key_prefix"] == "sk_test_abcd..."


def test_status_reports_not_connected_on_bad_key(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_bad")
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=Response(401, json={"error": {"message": "bad"}})
        )
        body = client.get("/api/stripe/status").json()
    # status_code != 200 -> connected False, but no exception.
    assert body["connected"] is False
    assert body["orders_in_db"] == 0
