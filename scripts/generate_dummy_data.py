#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _daterange(start: date, end: date) -> Iterable[date]:
    if end < start:
        raise ValueError("end_date must be >= start_date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _rand_time(rng: random.Random) -> time:
    return time(
        hour=rng.randint(0, 23),
        minute=rng.randint(0, 59),
        second=rng.randint(0, 59),
        tzinfo=UTC,
    )


def _rand_click_id(rng: random.Random, prefix: str) -> str:
    return f"{prefix}.{rng.getrandbits(96):024x}"


@dataclass(frozen=True)
class SpendRow:
    platform: str
    date: str
    account_id: str
    campaign_id: str
    adset_id: str
    ad_id: str
    creative_id: str
    clicks: int
    cost: float
    impressions: int
    metadata: str


@dataclass(frozen=True)
class SessionRow:
    session_id: str
    ts: str
    utm_source: str
    utm_medium: str
    utm_campaign: str
    utm_content: str
    utm_term: str
    referrer: str
    landing_page: str
    device: str
    gclid: str
    fbclid: str
    ttclid: str
    customer_key: str


@dataclass(frozen=True)
class TouchpointRow:
    ts: str
    channel: str
    platform: str
    campaign_id: str
    adset_id: str
    ad_id: str
    creative_id: str
    gclid: str
    fbclid: str
    ttclid: str
    customer_key: str
    session_id: str


@dataclass(frozen=True)
class OrderRow:
    order_id: str
    ts: str
    gross: float
    net: float
    refunds: float
    chargebacks: float
    cogs: float
    fees: float
    customer_key: str
    subscription_id: str


@dataclass(frozen=True)
class ConversionRow:
    conversion_id: str
    ts: str
    type: str
    value: float
    order_id: str
    customer_key: str


@dataclass(frozen=True)
class ReportedValueRow:
    platform: str
    date: str
    account_id: str
    campaign_id: str
    adset_id: str
    ad_id: str
    conversion_type: str
    reported_value: float


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"No rows to write for {path}")
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_sqlite(db_path: Path, tables: dict[str, list[dict[str, Any]]]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        for table_name, rows in tables.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            col_defs = ", ".join([f"{c} TEXT" for c in cols])
            conn.execute(f"CREATE TABLE {table_name} ({col_defs});")

            placeholders = ", ".join(["?"] * len(cols))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders});"
            conn.executemany(insert_sql, ([str(row[c]) for c in cols] for row in rows))

        conn.commit()


def _build_structure() -> dict[str, Any]:
    # Minimal cross-platform structure suitable for HYROS-style rollups.
    platforms = [
        {
            "platform": "meta",
            "account_id": "meta_acc_001",
            "channel": "paid_social",
            "utm_source": "facebook",
            "utm_medium": "paid_social",
            "base_cpc": 1.10,
        },
        {
            "platform": "google",
            "account_id": "gads_acc_001",
            "channel": "paid_search",
            "utm_source": "google",
            "utm_medium": "cpc",
            "base_cpc": 2.20,
        },
        {
            "platform": "tiktok",
            "account_id": "tt_ads_acc_001",
            "channel": "paid_social",
            "utm_source": "tiktok",
            "utm_medium": "paid_social",
            "base_cpc": 0.85,
        },
    ]

    ads: list[dict[str, str]] = []
    for p in platforms:
        for campaign_idx in range(1, 3):  # 2 campaigns / platform
            campaign_id = f"{p['platform']}_camp_{campaign_idx:02d}"
            for adset_idx in range(1, 3):  # 2 ad sets / campaign
                adset_id = f"{campaign_id}_as_{adset_idx:02d}"
                for ad_idx in range(1, 4):  # 3 ads / ad set
                    ad_id = f"{adset_id}_ad_{ad_idx:02d}"
                    creative_id = f"{ad_id}_cr_{ad_idx:02d}"
                    ads.append(
                        {
                            "platform": p["platform"],
                            "account_id": p["account_id"],
                            "channel": p["channel"],
                            "utm_source": p["utm_source"],
                            "utm_medium": p["utm_medium"],
                            "campaign_id": campaign_id,
                            "campaign_name": f"{p['platform'].title()} Campaign {campaign_idx}",
                            "adset_id": adset_id,
                            "adset_name": f"{p['platform'].title()} Ad Set {campaign_idx}.{adset_idx}",
                            "ad_id": ad_id,
                            "ad_name": f"{p['platform'].title()} Ad {campaign_idx}.{adset_idx}.{ad_idx}",
                            "creative_id": creative_id,
                            "creative_name": f"{p['platform'].title()} Creative {campaign_idx}.{adset_idx}.{ad_idx}",
                            "base_cpc": str(p["base_cpc"]),
                        }
                    )

    return {"platforms": platforms, "ads": ads}


def generate_dummy_data(
    *,
    start_date: date,
    end_date: date,
    seed: int,
    out_dir: Path,
    sqlite_path: Path,
) -> dict[str, Any]:
    rng = random.Random(seed)

    structure = _build_structure()
    ads: list[dict[str, str]] = structure["ads"]

    customers = [_sha256_hex(f"cust_{i:05d}")[:32] for i in range(1, 1201)]

    spend_rows: list[SpendRow] = []
    session_rows: list[SessionRow] = []
    touchpoint_rows: list[TouchpointRow] = []
    order_rows: list[OrderRow] = []
    conversion_rows: list[ConversionRow] = []
    reported_rows: list[ReportedValueRow] = []

    # --- Spend + Sessions + Touchpoints ---
    for day in _daterange(start_date, end_date):
        for ad in ads:
            platform = ad["platform"]
            base_cpc = float(ad["base_cpc"])

            clicks = rng.randint(4, 28)
            cpc = max(0.15, rng.gauss(mu=base_cpc, sigma=base_cpc * 0.18))
            cost = round(clicks * cpc, 2)
            impressions = int(clicks * rng.randint(60, 240))

            metadata = json.dumps(
                {
                    "campaign_name": ad["campaign_name"],
                    "adset_name": ad["adset_name"],
                    "ad_name": ad["ad_name"],
                    "creative_name": ad["creative_name"],
                },
                separators=(",", ":"),
            )

            spend_rows.append(
                SpendRow(
                    platform=platform,
                    date=_iso_date(day),
                    account_id=ad["account_id"],
                    campaign_id=ad["campaign_id"],
                    adset_id=ad["adset_id"],
                    ad_id=ad["ad_id"],
                    creative_id=ad["creative_id"],
                    clicks=clicks,
                    cost=cost,
                    impressions=impressions,
                    metadata=metadata,
                )
            )

            # Sessions: approximate 1 session per click (with small noise).
            sessions_count = max(1, int(round(clicks * rng.uniform(0.85, 1.05))))
            for _ in range(sessions_count):
                session_id = _sha256_hex(f"{platform}|{day.isoformat()}|{rng.getrandbits(64)}")[:24]
                ts = datetime.combine(day, _rand_time(rng), tzinfo=UTC)

                # Privacy-safe: customer_key may be blank (unknown).
                customer_key = rng.choice(customers) if rng.random() < 0.65 else ""

                gclid = _rand_click_id(rng, "gclid") if platform == "google" else ""
                fbclid = _rand_click_id(rng, "fbclid") if platform == "meta" else ""
                ttclid = _rand_click_id(rng, "ttclid") if platform == "tiktok" else ""

                landing_page = rng.choice(
                    [
                        "/",
                        "/lp/offer-a",
                        "/lp/offer-b",
                        "/lp/webinar",
                        "/products/widget",
                        "/products/gizmo",
                    ]
                )
                device = rng.choice(["mobile", "desktop", "tablet"])

                session_rows.append(
                    SessionRow(
                        session_id=session_id,
                        ts=_iso_ts(ts),
                        utm_source=ad["utm_source"],
                        utm_medium=ad["utm_medium"],
                        utm_campaign=ad["campaign_id"],
                        utm_content=ad["ad_id"],
                        utm_term=rng.choice(["", "brand", "nonbrand", "competitor"]),
                        referrer=rng.choice(
                            [
                                "https://google.com/",
                                "https://facebook.com/",
                                "https://tiktok.com/",
                                "",
                            ]
                        ),
                        landing_page=landing_page,
                        device=device,
                        gclid=gclid,
                        fbclid=fbclid,
                        ttclid=ttclid,
                        customer_key=customer_key,
                    )
                )

                touchpoint_rows.append(
                    TouchpointRow(
                        ts=_iso_ts(ts),
                        channel=ad["channel"],
                        platform=platform,
                        campaign_id=ad["campaign_id"],
                        adset_id=ad["adset_id"],
                        ad_id=ad["ad_id"],
                        creative_id=ad["creative_id"],
                        gclid=gclid,
                        fbclid=fbclid,
                        ttclid=ttclid,
                        customer_key=customer_key,
                        session_id=session_id,
                    )
                )

        # Extra organic/direct sessions (not tied to spend).
        organic_sessions = rng.randint(25, 85)
        for _ in range(organic_sessions):
            session_id = _sha256_hex(f"organic|{day.isoformat()}|{rng.getrandbits(64)}")[:24]
            ts = datetime.combine(day, _rand_time(rng), tzinfo=UTC)
            customer_key = rng.choice(customers) if rng.random() < 0.55 else ""

            source_medium = rng.choice(
                [
                    ("google", "organic", "https://google.com/"),
                    ("direct", "(none)", ""),
                    ("newsletter", "email", "https://mail.google.com/"),
                    ("referral", "referral", "https://example.com/"),
                ]
            )

            session_rows.append(
                SessionRow(
                    session_id=session_id,
                    ts=_iso_ts(ts),
                    utm_source=source_medium[0],
                    utm_medium=source_medium[1],
                    utm_campaign="",
                    utm_content="",
                    utm_term="",
                    referrer=source_medium[2],
                    landing_page=rng.choice(["/", "/blog/post-1", "/blog/post-2", "/pricing"]),
                    device=rng.choice(["mobile", "desktop"]),
                    gclid="",
                    fbclid="",
                    ttclid="",
                    customer_key=customer_key,
                )
            )

            touchpoint_rows.append(
                TouchpointRow(
                    ts=_iso_ts(ts),
                    channel="organic" if source_medium[1] != "email" else "email",
                    platform="",
                    campaign_id="",
                    adset_id="",
                    ad_id="",
                    creative_id="",
                    gclid="",
                    fbclid="",
                    ttclid="",
                    customer_key=customer_key,
                    session_id=session_id,
                )
            )

    # --- Orders + Conversions (Purchase) ---
    # Build a per-customer last touchpoint index to simulate last-click attribution.
    touchpoints_by_customer: dict[str, list[TouchpointRow]] = {}
    for tp in touchpoint_rows:
        if not tp.customer_key:
            continue
        touchpoints_by_customer.setdefault(tp.customer_key, []).append(tp)
    for customer_key in touchpoints_by_customer:
        touchpoints_by_customer[customer_key].sort(key=lambda t: t.ts)

    eligible_customers = [c for c in customers if c in touchpoints_by_customer]
    orders_target = int(max(80, len(eligible_customers) * 0.12))

    order_to_last_touch: dict[str, TouchpointRow] = {}

    for _ in range(orders_target):
        customer_key = rng.choice(eligible_customers)
        customer_tps = touchpoints_by_customer[customer_key]
        last_tp = rng.choice(customer_tps)

        last_tp_dt = datetime.strptime(last_tp.ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        purchase_dt = last_tp_dt + timedelta(hours=rng.randint(1, 96))
        if purchase_dt.date() > end_date:
            purchase_dt = datetime.combine(end_date, time(23, 40, tzinfo=UTC))

        order_id = _sha256_hex(f"order|{customer_key}|{purchase_dt.isoformat()}|{rng.getrandbits(32)}")[:18]
        gross = round(max(19.0, rng.lognormvariate(mu=4.2, sigma=0.35)), 2)  # ~ $66 median

        refunded = rng.random() < 0.06
        charged_back = (not refunded) and (rng.random() < 0.012)

        refunds = round(gross if refunded else 0.0, 2)
        chargebacks = round(gross if charged_back else 0.0, 2)
        net = round(max(0.0, gross - refunds - chargebacks), 2)

        cogs = round(gross * rng.uniform(0.22, 0.38), 2)
        fees = round(gross * rng.uniform(0.022, 0.041) + 0.25, 2)

        subscription_id = ""
        if rng.random() < 0.22:
            subscription_id = _sha256_hex(f"sub|{customer_key}|{purchase_dt.date().isoformat()}")[:16]

        order_rows.append(
            OrderRow(
                order_id=order_id,
                ts=_iso_ts(purchase_dt),
                gross=gross,
                net=net,
                refunds=refunds,
                chargebacks=chargebacks,
                cogs=cogs,
                fees=fees,
                customer_key=customer_key,
                subscription_id=subscription_id,
            )
        )

        conversion_rows.append(
            ConversionRow(
                conversion_id=_sha256_hex(f"conv|{order_id}")[:16],
                ts=_iso_ts(purchase_dt),
                type="Purchase",
                value=gross,
                order_id=order_id,
                customer_key=customer_key,
            )
        )

        order_to_last_touch[order_id] = last_tp

    # --- Reported value (platform dashboards) ---
    # Simulate platform-reported value drifting from warehouse net/gross attribution.
    reported_agg: dict[tuple[str, str, str, str, str], float] = {}
    for order in order_rows:
        last_tp = order_to_last_touch.get(order.order_id)
        if not last_tp or not last_tp.platform:
            continue

        key = (
            last_tp.platform,
            _iso_date(datetime.strptime(order.ts, "%Y-%m-%dT%H:%M:%SZ").date()),
            last_tp.campaign_id,
            last_tp.adset_id,
            last_tp.ad_id,
        )
        reported_agg[key] = reported_agg.get(key, 0.0) + float(order.gross)

    # Map platform+campaign/adset/ad to account_id using spend structure.
    account_by_key: dict[tuple[str, str, str, str], str] = {}
    for ad in ads:
        account_by_key[(ad["platform"], ad["campaign_id"], ad["adset_id"], ad["ad_id"])] = ad["account_id"]

    for (platform, day, campaign_id, adset_id, ad_id), attributed_gross in reported_agg.items():
        account_id = account_by_key.get((platform, campaign_id, adset_id, ad_id), "")
        # Add drift: under/over report and occasional missing value.
        if rng.random() < 0.03:
            continue
        drift = rng.uniform(0.72, 1.08)
        reported_value = round(attributed_gross * drift, 2)
        reported_rows.append(
            ReportedValueRow(
                platform=platform,
                date=day,
                account_id=account_id,
                campaign_id=campaign_id,
                adset_id=adset_id,
                ad_id=ad_id,
                conversion_type="Purchase",
                reported_value=reported_value,
            )
        )

    # --- Write outputs ---
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(out_dir / "spend.csv", [asdict(r) for r in spend_rows])
    _write_csv(out_dir / "sessions.csv", [asdict(r) for r in session_rows])
    _write_csv(out_dir / "touchpoints.csv", [asdict(r) for r in touchpoint_rows])
    _write_csv(out_dir / "orders.csv", [asdict(r) for r in order_rows])
    _write_csv(out_dir / "conversions.csv", [asdict(r) for r in conversion_rows])
    _write_csv(out_dir / "reported_value.csv", [asdict(r) for r in reported_rows])

    _seed_sqlite(
        sqlite_path,
        {
            "spend": [asdict(r) for r in spend_rows],
            "sessions": [asdict(r) for r in session_rows],
            "touchpoints": [asdict(r) for r in touchpoint_rows],
            "orders": [asdict(r) for r in order_rows],
            "conversions": [asdict(r) for r in conversion_rows],
            "reported_value": [asdict(r) for r in reported_rows],
        },
    )

    return {
        "seed": seed,
        "date_range": {"start": _iso_date(start_date), "end": _iso_date(end_date)},
        "paths": {
            "out_dir": str(out_dir),
            "sqlite": str(sqlite_path),
        },
        "row_counts": {
            "spend": len(spend_rows),
            "sessions": len(session_rows),
            "touchpoints": len(touchpoint_rows),
            "orders": len(order_rows),
            "conversions": len(conversion_rows),
            "reported_value": len(reported_rows),
        },
        "notes": [
            "All data is synthetic (dummy) and not business truth.",
            "customer_key values are deterministic sha256-derived tokens (no raw PII stored).",
            "conversions.value is gross order value (total_revenue). orders.net reflects refunds/chargebacks.",
        ],
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic attribution dummy data (CSV + SQLite).")
    parser.add_argument("--start-date", type=str, default="", help="YYYY-MM-DD (default: 30 days ago)")
    parser.add_argument("--end-date", type=str, default="", help="YYYY-MM-DD (default: today)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", type=str, default="data/dummy")
    parser.add_argument("--sqlite-path", type=str, default="data/dummy/attributionops_demo.sqlite")
    args = parser.parse_args(argv)

    today = date.today()
    start = today - timedelta(days=30) if not args.start_date else date.fromisoformat(args.start_date)
    end = today if not args.end_date else date.fromisoformat(args.end_date)

    out_dir = Path(args.out_dir)
    sqlite_path = Path(args.sqlite_path)

    result = generate_dummy_data(
        start_date=start,
        end_date=end,
        seed=args.seed,
        out_dir=out_dir,
        sqlite_path=sqlite_path,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

