"""Deeper coverage of the analytics modules' less-exercised endpoints.

Smoke tests for cohort/analysis, funnel/report, journey/stats, ltv/by-source,
ltv/summary already live in tests/api/test_analytics_endpoints.py. This file
covers the OTHER endpoints and deeper branches:

  - cohort/retention, plus cohort/analysis week-granularity & platform breakdown
  - funnel/by-source (platform / campaign_id / utm_source breakdowns)
  - journey/customer, journey/leads, journey/common-paths
  - ltv/by-customer

All outbound HTTP is mocked defensively with respx (these endpoints make no
network calls, so assert_all_called=False keeps the unused mock from failing).
"""

from __future__ import annotations

import httpx
import respx

from tests.helpers import ad_name, insert_rows, order, touchpoint


def _conv(conversion_id, ts, ctype, customer_key, value="0", order_id=""):
    return {
        "conversion_id": conversion_id,
        "ts": ts,
        "type": ctype,
        "value": str(value),
        "order_id": order_id,
        "customer_key": customer_key,
    }


def _session(session_id, ts, customer_key, **extra):
    row = {"session_id": session_id, "ts": ts, "customer_key": customer_key}
    row.update(extra)
    return row


def _no_net():
    """Context manager: defensive respx guard around an HTTP-free endpoint."""
    return respx.mock(assert_all_mocked=False, assert_all_called=False)


# ===========================================================================
# cohort/retention  +  cohort/analysis deeper branches
# ===========================================================================


def test_cohort_retention_two_cohorts_with_repeat(client, api_db):
    """Two monthly cohorts; one customer repeats in a later period."""
    # Jan cohort: c1 (acquired Jan, buys Jan + Mar → retained period 2) and c2 (Jan only).
    # Feb cohort: c3 (acquired Feb, buys Feb only).
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-05T09:00:00Z", "c1", platform="meta"),
        touchpoint("2026-01-06T09:00:00Z", "c2", platform="meta"),
        touchpoint("2026-02-05T09:00:00Z", "c3", platform="google"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-15T10:00:00Z", "c1", gross=100),
        order("o2", "2026-03-15T10:00:00Z", "c1", gross=120),   # period 2 for c1
        order("o3", "2026-01-20T10:00:00Z", "c2", gross=90),
        order("o4", "2026-02-20T10:00:00Z", "c3", gross=80),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/cohort/retention")

    assert r.status_code == 200
    body = r.json()
    cohorts = {c["cohort"]: c for c in body["cohorts"]}
    assert set(cohorts) == {"2026-01", "2026-02"}
    assert cohorts["2026-01"]["customers"] == 2
    assert cohorts["2026-02"]["customers"] == 1

    # Jan period 0: both c1 and c2 active → 2/2 = 100%.
    jan_ret = {p["period"]: p for p in cohorts["2026-01"]["retention"]}
    assert jan_ret[0]["active_customers"] == 2
    assert jan_ret[0]["retention_rate"] == 100.0
    # Jan period 2: only c1 active → 1/2 = 50%.
    assert jan_ret[2]["active_customers"] == 1
    assert jan_ret[2]["retention_rate"] == 50.0
    # max_period spans to 2 (Jan→Mar).
    assert body["max_period"] == 2
    assert body["granularity"] == "month"


def test_cohort_retention_empty(client, api_db):
    """No touchpoints → empty cohorts list."""
    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/cohort/retention")
    assert r.status_code == 200
    assert r.json() == {"cohorts": []}


def test_cohort_analysis_week_granularity(client, api_db):
    """Week granularity: order one week after first touch → period 1 has revenue."""
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-01T09:00:00Z", "c1", platform="meta")])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-01T10:00:00Z", "c1", gross=100),  # period 0
        order("o2", "2026-01-12T10:00:00Z", "c1", gross=50),   # ~11 days → period 1
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/cohort/analysis", params={"granularity": "week"})

    assert r.status_code == 200
    body = r.json()
    assert body["granularity"] == "week"
    assert len(body["cohorts"]) == 1
    cohort = body["cohorts"][0]
    assert cohort["customers"] == 1
    assert cohort["total_revenue"] == 150.0
    periods = {p["period"]: p for p in cohort["periods"]}
    assert periods[0]["revenue"] == 100.0
    assert periods[1]["revenue"] == 50.0
    # cumulative & ltv_per_customer accumulate.
    assert periods[1]["cumulative_revenue"] == 150.0
    assert periods[1]["ltv_per_customer"] == 150.0


def test_cohort_analysis_platform_breakdown(client, api_db):
    """breakdown=platform splits the same month into per-platform cohort keys."""
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-05T09:00:00Z", "c1", platform="meta"),
        touchpoint("2026-01-06T09:00:00Z", "c2", platform="google"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-15T10:00:00Z", "c1", gross=200),
        order("o2", "2026-01-15T10:00:00Z", "c2", gross=300),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/cohort/analysis", params={"breakdown": "platform"})

    assert r.status_code == 200
    cohorts = {c["cohort"]: c for c in r.json()["cohorts"]}
    assert "2026-01 (meta)" in cohorts
    assert "2026-01 (google)" in cohorts
    assert cohorts["2026-01 (meta)"]["total_revenue"] == 200.0
    assert cohorts["2026-01 (google)"]["total_revenue"] == 300.0


# ===========================================================================
# funnel/by-source
# ===========================================================================


def test_funnel_by_source_platform_breakdown(client, api_db):
    """Per-platform funnel: counts visits, leads, purchases & revenue per source."""
    # meta: c1 (lead+purchase), c2 (lead only). google: c3 (purchase only).
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-05T09:00:00Z", "c1", platform="meta", session_id="s1"),
        touchpoint("2026-01-05T09:00:00Z", "c2", platform="meta", session_id="s2"),
        touchpoint("2026-01-05T09:00:00Z", "c3", platform="google", session_id="s3"),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-06T09:00:00Z", "Lead", "c1"),
        _conv("k2", "2026-01-07T09:00:00Z", "Purchase", "c1", value=500, order_id="o1"),
        _conv("k3", "2026-01-06T09:00:00Z", "Lead", "c2"),
        _conv("k4", "2026-01-07T09:00:00Z", "Purchase", "c3", value=300, order_id="o2"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-07T09:00:00Z", "c1", gross=500),
        order("o2", "2026-01-07T09:00:00Z", "c3", gross=300),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/funnel/by-source", params={"breakdown": "platform"})

    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "platform"
    rows = {row["source"]: row for row in body["rows"]}
    assert set(rows) == {"meta", "google"}

    meta = rows["meta"]
    assert meta["visits"] == 2          # s1, s2
    assert meta["leads"] == 2           # c1, c2
    assert meta["purchases"] == 1       # c1
    assert meta["revenue"] == 500.0
    assert meta["purchase_rate"] == 50.0  # 1 purchase / 2 visits

    google = rows["google"]
    assert google["visits"] == 1
    assert google["leads"] == 0
    assert google["purchases"] == 1
    assert google["revenue"] == 300.0

    # Sorted by visits desc → meta (2) before google (1).
    assert [row["source"] for row in body["rows"]] == ["meta", "google"]


def test_funnel_by_source_campaign_breakdown(client, api_db):
    """breakdown=campaign_id buckets touchpoints by campaign."""
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-05T09:00:00Z", "c1", platform="meta", campaign_id="cmpX", session_id="s1"),
        touchpoint("2026-01-05T09:00:00Z", "c2", platform="meta", campaign_id="cmpY", session_id="s2"),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-06T09:00:00Z", "Booking", "c1"),
        _conv("k2", "2026-01-06T09:00:00Z", "Lead", "c2"),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/funnel/by-source", params={"breakdown": "campaign_id"})

    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "campaign_id"
    rows = {row["source"]: row for row in body["rows"]}
    assert "cmpX" in rows and "cmpY" in rows
    assert rows["cmpX"]["bookings"] == 1
    assert rows["cmpY"]["leads"] == 1


def test_funnel_by_source_utm_source_uses_sessions(client, api_db):
    """breakdown=utm_source reads sessions; empty utm collapses to 'direct'."""
    insert_rows(api_db, "sessions", [
        _session("s1", "2026-01-05T09:00:00Z", "c1", utm_source="newsletter"),
        _session("s2", "2026-01-05T09:00:00Z", "c2", utm_source=""),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-06T09:00:00Z", "Lead", "c1"),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/funnel/by-source", params={"breakdown": "utm_source"})

    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "utm_source"
    rows = {row["source"]: row for row in body["rows"]}
    assert "newsletter" in rows
    assert rows["newsletter"]["visits"] == 1
    assert rows["newsletter"]["leads"] == 1
    # Empty utm_source falls back to label 'direct'.
    assert "direct" in rows
    assert rows["direct"]["leads"] == 0


def test_funnel_by_source_date_filter(client, api_db):
    """start_date/end_date restrict which touchpoints feed the funnel sources."""
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-15T09:00:00Z", "c1", platform="meta", session_id="s1"),
        touchpoint("2026-03-15T09:00:00Z", "c2", platform="google", session_id="s2"),
    ])
    insert_rows(api_db, "conversions", [_conv("k1", "2026-01-16T09:00:00Z", "Lead", "c1")])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get(
            "/api/funnel/by-source",
            params={"breakdown": "platform", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        )

    assert r.status_code == 200
    rows = {row["source"]: row for row in r.json()["rows"]}
    # Only the January meta touchpoint is in range; the March google one is excluded.
    assert "meta" in rows
    assert "google" not in rows
    assert rows["meta"]["leads"] == 1


def test_funnel_by_source_unknown_breakdown_defaults_to_platform(client, api_db):
    """Unrecognized breakdown value falls back to platform."""
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-05T09:00:00Z", "c1", platform="tiktok", session_id="s1"),
    ])
    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/funnel/by-source", params={"breakdown": "garbage"})
    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "platform"
    assert any(row["source"] == "tiktok" for row in body["rows"])


# ===========================================================================
# journey/customer
# ===========================================================================


def test_journey_customer_full_timeline(client, api_db):
    """Full timeline (session+touchpoint+conversion+order) and summary fields."""
    ck = "cust-abc"
    insert_rows(api_db, "sessions", [
        _session("s1", "2026-01-10T08:00:00Z", ck, utm_source="facebook", landing_page="/lp", device="mobile"),
    ])
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T08:05:00Z", ck, platform="meta", campaign_id="cmp1", ad_id="ad1", session_id="s1"),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-11T08:00:00Z", "Purchase", ck, value=250, order_id="o1"),
    ])
    insert_rows(api_db, "orders", [order("o1", "2026-01-11T08:00:00Z", ck, gross=250, net=200, refunds=0)])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/customer", params={"customer_key": ck})

    assert r.status_code == 200
    body = r.json()
    assert body["customer_key"] == ck
    summary = body["summary"]
    assert summary["total_sessions"] == 1
    assert summary["total_touchpoints"] == 1
    assert summary["total_orders"] == 1
    assert summary["total_conversions"] == 1
    assert summary["total_revenue"] == 250.0
    assert summary["first_touch"] == "2026-01-10T08:00:00Z"
    assert summary["first_order"] == "2026-01-11T08:00:00Z"
    # ~24h between first session and first order.
    assert "hours" in summary["time_to_convert"] or "days" in summary["time_to_convert"]

    # Timeline holds all 4 event types, sorted by ts (session first).
    types = [ev["type"] for ev in body["timeline"]]
    assert types[0] == "session"
    assert set(types) == {"session", "touchpoint", "conversion", "order"}


def test_journey_customer_fast_convert_minutes(client, api_db):
    """First touch and first order minutes apart → time_to_convert in 'min'."""
    ck = "fast-cust"
    insert_rows(api_db, "sessions", [_session("s1", "2026-01-10T08:00:00Z", ck, landing_page="/lp")])
    insert_rows(api_db, "orders", [order("o1", "2026-01-10T08:20:00Z", ck, gross=75, net=75)])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/customer", params={"customer_key": ck})

    assert r.status_code == 200
    summary = r.json()["summary"]
    assert summary["total_revenue"] == 75.0
    assert "min" in summary["time_to_convert"]


def test_journey_customer_unknown_returns_empty_summary(client, api_db):
    """Unknown customer_key → zeroed summary, empty timeline, 200."""
    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/customer", params={"customer_key": "nope"})
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["total_sessions"] == 0
    assert body["summary"]["total_revenue"] == 0
    assert body["timeline"] == []


# ===========================================================================
# journey/leads
# ===========================================================================


def test_journey_leads_builds_paths_with_ad_names(client, api_db):
    """Lead + purchase conversions produce rows with timeline + compact path."""
    ck = "lead-cust-key-1234567890"
    # Ad name enrichment so campaign_name resolves in the timeline.
    insert_rows(api_db, "ad_names", [
        ad_name(platform="meta", entity_type="campaign", entity_id="cmp1", name="Summer Sale"),
    ])
    insert_rows(api_db, "sessions", [
        _session("s1", "2026-01-10T08:00:00Z", ck, utm_source="facebook", landing_page="/lp"),
    ])
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T08:05:00Z", ck, platform="meta", campaign_id="cmp1", ad_id="ad1", session_id="s1"),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-10T09:00:00Z", "lead", ck),
        _conv("k2", "2026-01-11T09:00:00Z", "purchase", ck, value=250, order_id="o1"),
    ])
    insert_rows(api_db, "orders", [order("o1", "2026-01-11T09:00:00Z", ck, gross=250, net=200)])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/leads")

    assert r.status_code == 200
    body = r.json()
    assert body["include_purchases"] is True
    assert body["count"] == 2  # one lead + one purchase conversion
    # Newest first (purchase at 01-11 before lead at 01-10).
    types = [row["conversion_type"] for row in body["rows"]]
    assert types == ["purchase", "lead"]

    purchase_row = body["rows"][0]
    assert purchase_row["customer_key"] == ck
    assert purchase_row["gross"] == 250.0
    assert purchase_row["touchpoint_count"] >= 1
    # Compact path includes the resolved campaign name from ad_names.
    path_joined = " ".join(purchase_row["path"])
    assert "Summer Sale" in path_joined
    assert purchase_row["customer_key_short"].endswith("...")


def test_journey_leads_exclude_purchases(client, api_db):
    """include_purchases=false drops purchase conversions, keeps leads."""
    ck = "lead-cust-2"
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T08:05:00Z", ck, platform="meta", session_id="s1"),
    ])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-10T09:00:00Z", "lead", ck),
        _conv("k2", "2026-01-11T09:00:00Z", "purchase", ck, value=99, order_id="o1"),
    ])
    insert_rows(api_db, "orders", [order("o1", "2026-01-11T09:00:00Z", ck, gross=99)])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/leads", params={"include_purchases": "false"})

    assert r.status_code == 200
    body = r.json()
    assert body["include_purchases"] is False
    assert body["count"] == 1
    assert body["rows"][0]["conversion_type"] == "lead"


def test_journey_leads_date_filter(client, api_db):
    """start_date/end_date filter conversions by date."""
    ck = "lead-cust-3"
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-01T08:00:00Z", ck, platform="meta")])
    insert_rows(api_db, "conversions", [
        _conv("k1", "2026-01-05T09:00:00Z", "lead", ck),
        _conv("k2", "2026-02-05T09:00:00Z", "lead", ck),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/leads", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["rows"][0]["conversion_id"] == "k1"
    assert body["date_range"] == {"start": "2026-01-01", "end": "2026-01-31"}


# ===========================================================================
# journey/common-paths
# ===========================================================================


def test_journey_common_paths_groups_identical_sequences(client, api_db):
    """Two customers sharing meta→google path get grouped (count == 2)."""
    # c1 and c2 both: meta then google. c3: meta only (won't meet min_conversions=2).
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-01T08:00:00Z", "c1", platform="meta", session_id="a1"),
        touchpoint("2026-01-02T08:00:00Z", "c1", platform="google", session_id="a2"),
        touchpoint("2026-01-01T08:00:00Z", "c2", platform="meta", session_id="b1"),
        touchpoint("2026-01-02T08:00:00Z", "c2", platform="google", session_id="b2"),
        touchpoint("2026-01-01T08:00:00Z", "c3", platform="meta", session_id="d1"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-03T08:00:00Z", "c1", gross=100),
        order("o2", "2026-01-03T08:00:00Z", "c2", gross=200),
        order("o3", "2026-01-03T08:00:00Z", "c3", gross=300),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/common-paths")

    assert r.status_code == 200
    body = r.json()
    # Default min_conversions=2 → only the shared meta→google path qualifies.
    rows = {row["path"]: row for row in body["rows"]}
    assert "meta → google" in rows
    grouped = rows["meta → google"]
    assert grouped["conversions"] == 2
    assert grouped["total_revenue"] == 300.0   # 100 + 200
    assert grouped["avg_revenue"] == 150.0
    assert grouped["touchpoints"] == 2
    # The single-customer "meta" path is filtered out (count 1 < 2).
    assert "meta" not in rows
    assert body["count"] == 1


def test_journey_common_paths_min_conversions_one(client, api_db):
    """min_conversions=1 surfaces even single-customer paths."""
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-01T08:00:00Z", "solo", platform="tiktok", session_id="z1"),
    ])
    insert_rows(api_db, "orders", [order("o1", "2026-01-02T08:00:00Z", "solo", gross=500)])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/journey/common-paths", params={"min_conversions": 1})

    assert r.status_code == 200
    rows = {row["path"]: row for row in r.json()["rows"]}
    assert "tiktok" in rows
    assert rows["tiktok"]["conversions"] == 1
    assert rows["tiktok"]["total_revenue"] == 500.0


# ===========================================================================
# ltv/by-customer
# ===========================================================================


def test_ltv_by_customer_aggregates_and_enriches(client, api_db):
    """Per-customer LTV with order aggregation + acquisition source enrichment."""
    # Whale: 2 orders. Minnow: 1 order. Sorted by total_gross desc.
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-01T08:00:00Z", "whale", platform="meta", campaign_id="cmpW", channel="paid"),
        touchpoint("2026-01-01T08:00:00Z", "minnow", platform="google", campaign_id="cmpM", channel="paid"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-05T10:00:00Z", "whale", gross=1000, net=900, refunds=50),
        order("o2", "2026-02-05T10:00:00Z", "whale", gross=500, net=450, refunds=0),
        order("o3", "2026-01-10T10:00:00Z", "minnow", gross=200, net=180, refunds=0),
    ])

    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ltv/by-customer")

    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2
    rows = body["rows"]
    # Sorted by total_gross desc → whale first.
    assert rows[0]["customer_key"] == "whale"
    whale = rows[0]
    assert whale["order_count"] == 2
    assert whale["total_revenue"] == 1500.0
    assert whale["total_net"] == 1350.0
    assert whale["total_refunds"] == 50.0
    assert whale["first_order"] == "2026-01-05T10:00:00Z"
    assert whale["last_order"] == "2026-02-05T10:00:00Z"
    assert whale["acquisition_platform"] == "meta"
    assert whale["acquisition_campaign"] == "cmpW"
    assert whale["acquisition_channel"] == "paid"

    minnow = rows[1]
    assert minnow["customer_key"] == "minnow"
    assert minnow["order_count"] == 1
    assert minnow["total_revenue"] == 200.0
    assert minnow["acquisition_platform"] == "google"


def test_ltv_by_customer_limit(client, api_db):
    """limit caps the number of returned rows."""
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-05T10:00:00Z", "c1", gross=300),
        order("o2", "2026-01-05T10:00:00Z", "c2", gross=200),
        order("o3", "2026-01-05T10:00:00Z", "c3", gross=100),
    ])
    with _no_net() as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ltv/by-customer", params={"limit": 2})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2
    # Top-2 by gross → c1 (300), c2 (200).
    assert [row["customer_key"] for row in body["rows"]] == ["c1", "c2"]
