"""Small, dependency-free helpers for seeding fixture SQLite databases.

Every column in the schema is ``TEXT`` (numbers are stored as strings and parsed
by the application via ``to_int`` / ``to_float``), so these builders accept either
strings or numbers and the application layer is responsible for coercion.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Mapping, Sequence


def insert_rows(db_path: str, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
    """Insert ``rows`` (list of column->value dicts) into ``table``."""
    rows = list(rows)
    if not rows:
        return
    with sqlite3.connect(db_path) as conn:
        for r in rows:
            cols = list(r.keys())
            placeholders = ", ".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                [r[c] for c in cols],
            )
        conn.commit()


def touchpoint(
    ts: str,
    customer_key: str,
    *,
    channel: str = "paid",
    platform: str = "meta",
    campaign_id: str = "cmp1",
    adset_id: str = "set1",
    ad_id: str = "ad1",
    creative_id: str = "cr1",
    gclid: str = "",
    fbclid: str = "",
    ttclid: str = "",
    session_id: str = "sess1",
    visitor_id: str = "",
) -> dict[str, Any]:
    return {
        "ts": ts,
        "channel": channel,
        "platform": platform,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "ad_id": ad_id,
        "creative_id": creative_id,
        "gclid": gclid,
        "fbclid": fbclid,
        "ttclid": ttclid,
        "customer_key": customer_key,
        "session_id": session_id,
        "visitor_id": visitor_id,
    }


def order(
    order_id: str,
    ts: str,
    customer_key: str,
    *,
    gross: Any = "0",
    net: Any = "0",
    refunds: Any = "0",
    chargebacks: Any = "0",
    cogs: Any = "0",
    fees: Any = "0",
    subscription_id: str = "",
    session_id: str = "",
    visitor_id: str = "",
    channel: str = "",
    platform: str = "",
    campaign_id: str = "",
    adset_id: str = "",
    ad_id: str = "",
    creative_id: str = "",
    gclid: str = "",
    fbclid: str = "",
    ttclid: str = "",
    currency: str = "",
    processor: str = "",
    processor_customer_id: str = "",
    payment_fingerprint: str = "",
    product_key: str = "",
    is_recurring: int = 0,
    sale_group_id: str = "",
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "ts": ts,
        "gross": str(gross),
        "net": str(net),
        "refunds": str(refunds),
        "chargebacks": str(chargebacks),
        "cogs": str(cogs),
        "fees": str(fees),
        "customer_key": customer_key,
        "subscription_id": subscription_id,
        "session_id": session_id,
        "visitor_id": visitor_id,
        "channel": channel,
        "platform": platform,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "ad_id": ad_id,
        "creative_id": creative_id,
        "gclid": gclid,
        "fbclid": fbclid,
        "ttclid": ttclid,
        "currency": currency,
        "processor": processor,
        "processor_customer_id": processor_customer_id,
        "payment_fingerprint": payment_fingerprint,
        "product_key": product_key,
        "is_recurring": is_recurring,
        "sale_group_id": sale_group_id or order_id,
    }


def spend(
    *,
    date: str,
    platform: str = "meta",
    account_id: str = "acc_meta",
    campaign_id: str = "cmp1",
    adset_id: str = "set1",
    ad_id: str = "ad1",
    creative_id: str = "cr1",
    clicks: Any = "0",
    cost: Any = "0",
    impressions: Any = "0",
    metadata: str = "{}",
    currency: str | None = None,
) -> dict[str, Any]:
    row = {
        "platform": platform,
        "date": date,
        "account_id": account_id,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "ad_id": ad_id,
        "creative_id": creative_id,
        "clicks": str(clicks),
        "cost": str(cost),
        "impressions": str(impressions),
        "metadata": metadata,
    }
    # Only included when requested: the base schema predates the currency
    # column, so unconditional inclusion would break inserts against databases
    # that have not run the spend-table migration.
    if currency is not None:
        row["currency"] = str(currency)
    return row


def ad_name(
    *,
    platform: str,
    entity_type: str,
    entity_id: str,
    name: str,
    parent_id: str = "",
    source: str = "manual",
    updated_at: str = "2026-01-01T00:00:00Z",
) -> dict[str, Any]:
    return {
        "platform": platform,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "name": name,
        "parent_id": parent_id,
        "source": source,
        "updated_at": updated_at,
    }
