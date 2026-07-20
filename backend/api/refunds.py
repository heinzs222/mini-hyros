"""Refund & Chargeback Tracking — adjust attribution when refunds/chargebacks happen.

Endpoints:
  POST /api/refunds/record       — record a refund or chargeback
  GET  /api/refunds/summary      — refund stats per source/campaign
  GET  /api/refunds/list         — list all refunds
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import anyio
from fastapi import APIRouter, HTTPException, Request, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows as db_query  # noqa
from attributionops.db import is_postgres

router = APIRouter()
logger = logging.getLogger("refunds")
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _ensure_refunds_table(db_path: str) -> None:
    if is_postgres():
        # Postgres schema is provisioned once by migrations/postgres/0001_schema.sql;
        # the SQLite-style CREATE/ALTER/PRAGMA below never runs against Postgres.
        return
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS refund_log (
            id TEXT PRIMARY KEY,
            ts TEXT,
            order_id TEXT,
            customer_key TEXT,
            type TEXT,
            amount TEXT,
            reason TEXT,
            source TEXT
        )""")
        conn.commit()


@router.post("/record")
async def record_refund(request: Request):
    """Record a refund or chargeback. Updates the original order's refund/chargeback amounts."""
    payload = await request.json()
    db_path = _db()
    now = _iso_ts(datetime.now(UTC))

    order_id = str(payload.get("order_id", ""))
    amount = float(payload.get("amount", 0))
    refund_type = str(payload.get("type", "refund")).lower()  # "refund" or "chargeback"
    reason = str(payload.get("reason", ""))
    source = str(payload.get("source", "manual"))  # manual, shopify, stripe, ghl

    if not order_id:
        return {"ok": False, "error": "order_id is required"}

    _ensure_refunds_table(db_path)
    # Stable, content-derived id (never the receive time) so a retry/redelivery is
    # idempotent. Callers may pass an explicit source refund id.
    refund_id = str(payload.get("refund_id") or "") or _sha256(
        f"refund|{order_id}|{amount}|{refund_type}"
    )

    # Get customer_key + amounts from the original order.
    orders = db_query(
        db_path,
        "SELECT customer_key, gross, net, refunds, chargebacks FROM orders WHERE order_id = ?",
        [order_id],
    )
    customer_key = orders[0]["customer_key"] if orders else ""

    # Clamp so refunds + chargebacks can never exceed the order gross.
    if orders:
        try:
            gross = float(orders[0].get("gross") or 0)
            prior = float(orders[0].get("refunds") or 0) + float(orders[0].get("chargebacks") or 0)
            allowable = max(0.0, gross - prior)
            amount = min(amount, allowable)
        except (TypeError, ValueError):
            pass

    def _apply() -> bool:
        with connect(db_path) as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO refund_log (id, ts, order_id, customer_key, type, amount, reason, source) VALUES (?,?,?,?,?,?,?,?)",
                (refund_id, now, order_id, customer_key, refund_type, str(amount), reason, source),
            )
            # Only adjust the order when this refund was newly recorded, so
            # replays don't double-decrement net.
            if not cur.rowcount or cur.rowcount < 1:
                conn.commit()
                return False
            if refund_type == "chargeback":
                conn.execute(
                    "UPDATE orders SET chargebacks = CAST(CAST(chargebacks AS REAL) + ? AS TEXT), net = CAST(CAST(net AS REAL) - ? AS TEXT) WHERE order_id = ?",
                    (amount, amount, order_id),
                )
            else:
                conn.execute(
                    "UPDATE orders SET refunds = CAST(CAST(refunds AS REAL) + ? AS TEXT), net = CAST(CAST(net AS REAL) - ? AS TEXT) WHERE order_id = ?",
                    (amount, amount, order_id),
                )
            conn.commit()
            return True

    applied = await anyio.to_thread.run_sync(_apply)

    # Broadcast
    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast({
        "type": "refund",
        "order_id": order_id,
        "amount": amount,
        "refund_type": refund_type,
        "customer_key": customer_key,
        "ts": now,
    }))

    return {"ok": True, "refund_id": refund_id, "order_id": order_id, "amount": amount, "applied": applied}


@router.get("/summary")
def refund_summary(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Refund/chargeback stats, optionally broken down by source."""
    db_path = _db()
    _ensure_refunds_table(db_path)

    where = ""
    params = []
    if start_date:
        where += " AND r.ts >= ?"
        params.append(start_date)
    if end_date:
        where += " AND r.ts <= ?"
        params.append(end_date + "T23:59:59Z")

    try:
        totals = db_query(db_path, f"""
            SELECT r.type,
                   COUNT(*) as count,
                   SUM(CAST(r.amount AS REAL)) as total_amount
            FROM refund_log r
            WHERE 1=1 {where}
            GROUP BY r.type
        """, params)

        # Overall order stats for refund rate
        order_stats = db_query(db_path, """
            SELECT COUNT(*) as total_orders,
                   SUM(CAST(gross AS REAL)) as total_revenue,
                   SUM(CAST(refunds AS REAL)) as total_refunds,
                   SUM(CAST(chargebacks AS REAL)) as total_chargebacks
            FROM orders
        """)

        os_data = order_stats[0] if order_stats else {}
        total_rev = float(os_data.get("total_revenue", 0) or 0)
        total_ref = float(os_data.get("total_refunds", 0) or 0)
        total_cb = float(os_data.get("total_chargebacks", 0) or 0)
        total_orders = int(os_data.get("total_orders", 0) or 0)

        # By-source attribution: credit each refund to exactly ONE touchpoint
        # (the customer's last touch) instead of fanning out over every
        # touchpoint the customer ever had (which multiplied the refund amount
        # by the touchpoint count).
        by_source = db_query(db_path, f"""
            SELECT source,
                   COUNT(*) as refund_count,
                   SUM(amount) as refund_amount
            FROM (
                SELECT r.order_id,
                       CAST(r.amount AS REAL) as amount,
                       (SELECT t.platform FROM touchpoints t
                          WHERE t.customer_key = o.customer_key AND t.platform != ''
                          ORDER BY t.ts DESC LIMIT 1) as source
                FROM refund_log r
                LEFT JOIN orders o ON r.order_id = o.order_id
                WHERE 1=1 {where}
            )
            WHERE source IS NOT NULL AND source != ''
            GROUP BY source
        """, params)

        return {
            "totals": {k["type"]: {"count": int(k["count"]), "amount": round(float(k["total_amount"]), 2)} for k in totals},
            "refund_rate": round(total_ref / max(total_rev, 1) * 100, 1),
            "chargeback_rate": round(total_cb / max(total_rev, 1) * 100, 1),
            "total_orders": total_orders,
            "total_revenue": round(total_rev, 2),
            "net_after_refunds": round(total_rev - total_ref - total_cb, 2),
            "by_source": by_source,
        }
    except Exception:
        logger.exception("refund_summary failed")
        raise HTTPException(status_code=500, detail="refund summary failed")


@router.get("/list")
def refund_list(
    limit: int = Query(default=50),
    order_id: str = Query(default=""),
):
    """List refund/chargeback records."""
    db_path = _db()
    _ensure_refunds_table(db_path)

    sql = "SELECT * FROM refund_log WHERE 1=1"
    params = []
    if order_id:
        sql += " AND order_id = ?"
        params.append(order_id)
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)

    try:
        rows = db_query(db_path, sql, params)
    except Exception:
        logger.exception("refund_list failed")
        raise HTTPException(status_code=500, detail="refund list failed")

    return {"rows": rows, "count": len(rows)}
