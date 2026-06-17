"""Tests for backend/api/ai_recommendations.py (mounted at /api/ai).

The two endpoints (/recommendations, /insights) are pure heuristics over the
SQLite warehouse — they make NO outbound HTTP / LLM calls. We still wrap the
HTTP-touching tests in a respx router (assert_all_mocked=False) to guarantee no
real network is hit if the implementation ever changes.
"""

from __future__ import annotations

import httpx
import respx

from tests.helpers import insert_rows, order, spend, touchpoint


def _conversion(conversion_id, ts, ctype, value, customer_key, order_id=""):
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


# ---------------------------------------------------------------------------
# /api/ai/recommendations
# ---------------------------------------------------------------------------


def test_recommendations_scale_high_priority_for_strong_roas(client, api_db):
    """ROAS >= 3 → action 'scale', priority 'high', +25% budget suggestion."""
    # Spend $100 on a campaign.
    insert_rows(api_db, "spend", [spend(date="2026-01-10", platform="meta", campaign_id="cmpA", cost=100, clicks=50)])
    # Customer touched that campaign, then bought $500 → ROAS 5.0.
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-11T09:00:00Z", "cust1", platform="meta", campaign_id="cmpA")])
    insert_rows(api_db, "orders", [order("o1", "2026-01-12T10:00:00Z", "cust1", gross=500, net=450)])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(
            return_value=httpx.Response(200, json={"content": []})
        )
        r = client.get("/api/ai/recommendations", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    assert r.status_code == 200
    body = r.json()
    assert body["period"] == {"start": "2026-01-01", "end": "2026-01-31"}
    assert len(body["recommendations"]) == 1
    rec = body["recommendations"][0]
    assert rec["platform"] == "meta"
    assert rec["campaign_id"] == "cmpA"
    assert rec["spend"] == 100.0
    assert rec["revenue"] == 500.0
    assert rec["profit"] == 400.0
    assert rec["roas"] == 5.0
    assert rec["orders"] == 1
    assert rec["action"] == "scale"
    assert rec["priority"] == "high"
    assert rec["suggested_budget_change"] == "+25%"


def test_recommendations_pause_when_spend_no_orders(client, api_db):
    """Spend with zero attributed orders → action 'pause', high priority."""
    insert_rows(api_db, "spend", [spend(date="2026-01-10", platform="google", campaign_id="cmpDead", cost=200, clicks=80)])
    # No touchpoints / orders for this campaign at all.

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/recommendations", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    assert r.status_code == 200
    recs = r.json()["recommendations"]
    assert len(recs) == 1
    rec = recs[0]
    assert rec["campaign_id"] == "cmpDead"
    assert rec["orders"] == 0
    assert rec["revenue"] == 0.0
    assert rec["roas"] == 0.0
    assert rec["action"] == "pause"
    assert rec["priority"] == "high"
    assert "suggestions" in rec and len(rec["suggestions"]) == 3


def test_recommendations_optimize_and_optimize_or_pause_branches(client, api_db):
    """Cover roas>=1 'optimize' branch and roas<1 with orders 'optimize_or_pause'."""
    # Campaign B: spend 100, revenue 150 → roas 1.5 → optimize.
    insert_rows(api_db, "spend", [
        spend(date="2026-01-10", platform="meta", campaign_id="cmpB", cost=100, clicks=20),
        spend(date="2026-01-10", platform="meta", campaign_id="cmpC", cost=300, clicks=20),
    ])
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-11T09:00:00Z", "bCust", platform="meta", campaign_id="cmpB"),
        touchpoint("2026-01-11T09:00:00Z", "cCust", platform="meta", campaign_id="cmpC"),
    ])
    insert_rows(api_db, "orders", [
        order("ob", "2026-01-12T10:00:00Z", "bCust", gross=150, net=150),
        # Campaign C: spend 300, revenue 100 → roas 0.33 with an order → optimize_or_pause.
        order("oc", "2026-01-12T10:00:00Z", "cCust", gross=100, net=100),
    ])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/recommendations", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    assert r.status_code == 200
    recs = {rec["campaign_id"]: rec for rec in r.json()["recommendations"]}
    assert recs["cmpB"]["action"] == "optimize"
    assert recs["cmpB"]["priority"] == "medium"
    assert "suggestions" in recs["cmpB"]
    assert recs["cmpC"]["action"] == "optimize_or_pause"
    assert recs["cmpC"]["priority"] == "high"
    assert recs["cmpC"]["profit"] == -200.0


def test_recommendations_medium_scale_branch(client, api_db):
    """2 <= ROAS < 3 → action 'scale', medium priority, +15%."""
    insert_rows(api_db, "spend", [spend(date="2026-01-10", platform="tiktok", campaign_id="cmpM", cost=100, clicks=10)])
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-11T09:00:00Z", "mCust", platform="tiktok", campaign_id="cmpM")])
    insert_rows(api_db, "orders", [order("om", "2026-01-12T10:00:00Z", "mCust", gross=250, net=250)])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/recommendations", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    rec = r.json()["recommendations"][0]
    assert rec["roas"] == 2.5
    assert rec["action"] == "scale"
    assert rec["priority"] == "medium"
    assert rec["suggested_budget_change"] == "+15%"


def test_recommendations_default_dates_and_empty(client, api_db):
    """No spend rows + default (omitted) date params → empty recommendations, period filled."""
    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/recommendations")

    assert r.status_code == 200
    body = r.json()
    assert body["recommendations"] == []
    # Default period dates are populated (YYYY-MM-DD).
    assert len(body["period"]["start"]) == 10
    assert len(body["period"]["end"]) == 10


def test_recommendations_priority_sort_order(client, api_db):
    """High-priority recs sort before low; within a tier, worst profit first."""
    insert_rows(api_db, "spend", [
        # Strong: roas 5 → high priority, profit +400.
        spend(date="2026-01-10", platform="meta", campaign_id="good", cost=100, clicks=10),
        # Dead spend: roas 0 → high priority, profit -200 (worst).
        spend(date="2026-01-10", platform="meta", campaign_id="dead", cost=200, clicks=10),
    ])
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-11T09:00:00Z", "g", platform="meta", campaign_id="good")])
    insert_rows(api_db, "orders", [order("og", "2026-01-12T10:00:00Z", "g", gross=500, net=500)])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/recommendations", params={"start_date": "2026-01-01", "end_date": "2026-01-31"})

    recs = r.json()["recommendations"]
    # Both high priority; worst profit (dead, -200) sorts first.
    assert recs[0]["campaign_id"] == "dead"
    assert recs[1]["campaign_id"] == "good"


# ---------------------------------------------------------------------------
# /api/ai/insights
# ---------------------------------------------------------------------------


def test_insights_empty_db(client, api_db):
    """No data → no insights, but a well-formed response."""
    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/insights")
    assert r.status_code == 200
    assert r.json() == {"insights": []}


def test_insights_top_platform_and_tracking_gap(client, api_db):
    """Top platform insight + tracking gap when <50% sessions identified."""
    # meta is the top revenue platform.
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T09:00:00Z", "c1", platform="meta", campaign_id="cmp1"),
        touchpoint("2026-01-10T09:00:00Z", "c2", platform="meta", campaign_id="cmp1"),
        touchpoint("2026-01-10T09:00:00Z", "c3", platform="google", campaign_id="cmp2"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-11T10:00:00Z", "c1", gross=400, net=400),
        order("o2", "2026-01-11T10:00:00Z", "c2", gross=600, net=600),
        order("o3", "2026-01-11T10:00:00Z", "c3", gross=100, net=100),
    ])
    # 4 sessions, only 1 identified → 25% < 50% tracking gap.
    insert_rows(api_db, "sessions", [
        _session("s1", "2026-01-09T08:00:00Z", "c1"),
        _session("s2", "2026-01-09T08:00:00Z", ""),
        _session("s3", "2026-01-09T08:00:00Z", ""),
        _session("s4", "2026-01-09T08:00:00Z", ""),
    ])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/insights")

    assert r.status_code == 200
    insights = {i["type"]: i for i in r.json()["insights"]}
    assert "top_platform" in insights
    assert "Meta" in insights["top_platform"]["title"]
    assert "tracking_gap" in insights
    assert insights["tracking_gap"]["priority"] == "warning"
    assert "25%" in insights["tracking_gap"]["title"]


def test_insights_high_refunds_and_multi_touch(client, api_db):
    """Refund rate > 10% warning + multi-touch insight when avg touches > 1.5."""
    # Customer with 3 distinct sessions (multi-touch).
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T09:00:00Z", "c1", platform="meta", session_id="s1"),
        touchpoint("2026-01-10T10:00:00Z", "c1", platform="meta", session_id="s2"),
        touchpoint("2026-01-10T11:00:00Z", "c1", platform="google", session_id="s3"),
    ])
    # Refunds 200 on 1000 revenue → 20% > 10% threshold.
    insert_rows(api_db, "orders", [order("o1", "2026-01-11T10:00:00Z", "c1", gross=1000, net=800, refunds=200)])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/insights")

    insights = {i["type"]: i for i in r.json()["insights"]}
    assert "high_refunds" in insights
    assert "20.0%" in insights["high_refunds"]["title"]
    assert "multi_touch" in insights
    # 3 distinct sessions for the single customer → avg 3.0 touchpoints.
    assert "3.0" in insights["multi_touch"]["title"]


def test_insights_repeat_customers(client, api_db):
    """Repeat purchase rate > 20% → repeat_customers info insight."""
    # c1 buys twice, c2 once → 50% repeat rate.
    insert_rows(api_db, "touchpoints", [
        touchpoint("2026-01-10T09:00:00Z", "c1", platform="meta", session_id="s1"),
        touchpoint("2026-01-10T09:00:00Z", "c2", platform="meta", session_id="s2"),
    ])
    insert_rows(api_db, "orders", [
        order("o1", "2026-01-11T10:00:00Z", "c1", gross=100, net=100),
        order("o2", "2026-02-11T10:00:00Z", "c1", gross=120, net=120),
        order("o3", "2026-01-11T10:00:00Z", "c2", gross=90, net=90),
    ])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://api.anthropic.com").mock(return_value=httpx.Response(200, json={}))
        r = client.get("/api/ai/insights")

    insights = {i["type"]: i for i in r.json()["insights"]}
    assert "repeat_customers" in insights
    assert "50%" in insights["repeat_customers"]["title"]
    assert insights["repeat_customers"]["priority"] == "info"
