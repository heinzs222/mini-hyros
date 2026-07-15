from __future__ import annotations

from attributionops.db import sql_rows


def test_list_campaigns_combines_spend_touchpoints_names_and_defaults(client, api_db):
    from attributionops.db import connect

    with connect(api_db) as conn:
        conn.executemany(
            """INSERT INTO spend
               (platform, date, account_id, campaign_id, adset_id, ad_id, creative_id,
                clicks, cost, impressions, metadata)
               VALUES (?, ?, '', ?, '', '', '', '0', ?, '0', '')""",
            [
                ("meta", "2026-07-10", "meta-campaign", "12.345"),
                ("meta", "2026-07-11", "meta-campaign", "7.655"),
                ("google", "2026-07-09", "google-campaign", "5"),
            ],
        )
        conn.execute(
            """INSERT INTO touchpoints
               (ts, channel, platform, campaign_id, customer_key, session_id)
               VALUES ('2026-07-11T12:00:00Z', 'email', 'email', 'email-campaign', 'c', 's')"""
        )
        conn.execute(
            """INSERT INTO ad_names
               (platform, entity_type, entity_id, name, parent_id, source, updated_at)
               VALUES ('meta', 'campaign', 'meta-campaign', 'Meta Campaign Name', '', 'api', '')"""
        )
        conn.execute(
            """INSERT INTO campaign_settings
               (platform, campaign_id, tracked, updated_at)
               VALUES ('google', 'google-campaign', 0, '')"""
        )
        conn.commit()

    response = client.get("/api/campaigns")
    assert response.status_code == 200
    campaigns = {item["campaign_id"]: item for item in response.json()["campaigns"]}

    assert campaigns["meta-campaign"] == {
        "platform": "meta",
        "campaign_id": "meta-campaign",
        "name": "Meta Campaign Name",
        "tracked": True,
        "lifetime_spend": 20.0,
        "last_seen": "2026-07-11",
    }
    assert campaigns["google-campaign"]["tracked"] is False
    assert campaigns["email-campaign"]["tracked"] is True
    assert campaigns["email-campaign"]["lifetime_spend"] == 0.0


def test_update_tracking_inserts_then_updates(client, api_db):
    payload = {"platform": "meta", "campaign_id": "campaign-1", "tracked": False}
    assert client.put("/api/campaigns/tracking", json=payload).json() == {"ok": True}

    row = sql_rows(
        api_db,
        "SELECT tracked, updated_at FROM campaign_settings WHERE platform = ? AND campaign_id = ?",
        ["meta", "campaign-1"],
    )[0]
    assert row["tracked"] == 0
    assert row["updated_at"]

    payload["tracked"] = True
    assert client.put("/api/campaigns/tracking", json=payload).json() == {"ok": True}
    row = sql_rows(
        api_db,
        "SELECT tracked FROM campaign_settings WHERE platform = ? AND campaign_id = ?",
        ["meta", "campaign-1"],
    )[0]
    assert row["tracked"] == 1


def test_update_tracking_batch_updates_all_items(client, api_db):
    response = client.put(
        "/api/campaigns/tracking/batch",
        json={
            "items": [
                {"platform": "meta", "campaign_id": "m-1", "tracked": False},
                {"platform": "google", "campaign_id": "g-1", "tracked": True},
            ]
        },
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True, "updated": 2}

    rows = sql_rows(
        api_db,
        """SELECT platform, campaign_id, tracked
             FROM campaign_settings
             WHERE campaign_id IN ('m-1', 'g-1')
             ORDER BY campaign_id""",
    )
    assert rows == [
        {"platform": "google", "campaign_id": "g-1", "tracked": 1},
        {"platform": "meta", "campaign_id": "m-1", "tracked": 0},
    ]
