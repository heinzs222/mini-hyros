from __future__ import annotations

import json
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from attributionops.db import query


UTC = timezone.utc

# Matches a trailing " UTC-06:00" / "UTC+05:30" / "UTC-6" style label that Hyros
# exports append after a (bogus) trailing "Z", e.g. "2026-06-26T08:26:30Z UTC-06:00".
_TZ_LABEL_RE = re.compile(r"\s*UTC\s*([+-])(\d{1,2})(?::?(\d{2}))?\s*$", re.IGNORECASE)


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_iso_ts(value: str) -> datetime:
    """Parse a timestamp to an aware UTC datetime, tolerantly.

    Handles the shapes that actually land in this warehouse:
      * ``2026-01-23T02:41:28Z``               (clean UTC)
      * ``2026-06-26T08:26:30Z UTC-06:00``     (Hyros: bogus Z + real offset label)
      * ``2026-07-02 23:32:20``                (naive, space separator — treated UTC)
      * ``2026-07-02T23:32:20+00:00``          (offset-aware)

    A trailing ``UTC±HH:MM`` label wins over a bogus ``Z``. Naive values are
    assumed to already be UTC (warehouse writers store UTC), never the host's
    local timezone. Raises ValueError only on genuinely unparseable input.
    """
    if value is None:
        raise ValueError("cannot parse None timestamp")
    s = str(value).strip()

    # Extract a trailing "UTC±HH:MM" label if present; it is the real offset.
    offset: timezone | None = None
    m = _TZ_LABEL_RE.search(s)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        hh = int(m.group(2))
        mm = int(m.group(3) or 0)
        offset = timezone(sign * timedelta(hours=hh, minutes=mm))
        s = s[: m.start()].strip()

    # A trailing "Z" is a bogus marker when a real offset label was present
    # (offset wins) — drop it; otherwise it is the UTC marker.
    if s.endswith("Z"):
        s = s[:-1] if offset is not None else s[:-1] + "+00:00"
    s = s.replace(" ", "T", 1) if ("T" not in s and " " in s) else s

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Last resort: try common explicit formats.
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"unparseable timestamp: {value!r}")

    if offset is not None and dt.tzinfo is None:
        dt = dt.replace(tzinfo=offset)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def try_parse_iso_ts(value: Any) -> datetime | None:
    """Non-raising variant: returns None instead of raising on bad input."""
    try:
        return parse_iso_ts(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


# ── Reporting timezone ───────────────────────────────────────────────────────
# Day boundaries (report ranges, daily buckets) are interpreted in this zone so
# that a merchant's "July 2" means local July 2, not the UTC calendar day.
# Configure via REPORT_TIMEZONE (IANA name). This account defaults to the same
# fixed UTC-06 reporting offset configured in Hyros.
def report_timezone_name() -> str:
    """Configured IANA reporting-timezone name (what report_timezone() resolves).

    Exposed so the dashboard can compute preset dates (Today/Yesterday/...) in the
    SAME zone the backend buckets days into, instead of the browser's local zone —
    otherwise a user far from UTC-6 can request a window that is entirely in the
    server's future and legitimately gets zero rows for "Today".
    """
    return os.environ.get("REPORT_TIMEZONE", "Etc/GMT+6").strip() or "Etc/GMT+6"


def report_timezone() -> timezone:
    name = report_timezone_name()
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)  # type: ignore[return-value]
        except Exception:
            pass
    return UTC


def local_day_start_utc(day: str | date, tz: timezone | None = None) -> str:
    """UTC ISO instant (``YYYY-MM-DDTHH:MM:SSZ``) for local midnight of ``day``."""
    tz = tz or report_timezone()
    d = day if isinstance(day, date) else date.fromisoformat(str(day)[:10])
    local_midnight = datetime(d.year, d.month, d.day, tzinfo=tz)
    return iso_ts(local_midnight)


def local_day_bounds_utc(start_day: str, end_day: str, tz: timezone | None = None) -> tuple[str, str]:
    """UTC instants for an inclusive local-day range [start, end].

    Returns (start_utc, end_exclusive_utc): local midnight of start_day, and
    local midnight of the day AFTER end_day, so callers can filter
    ``ts >= start_utc AND ts < end_exclusive_utc``.
    """
    tz = tz or report_timezone()
    end_d = date.fromisoformat(str(end_day)[:10]) + timedelta(days=1)
    return local_day_start_utc(start_day, tz), local_day_start_utc(end_d, tz)


def utc_ts_to_local_date(value: Any, tz: timezone | None = None) -> str:
    """Local-zone calendar day (``YYYY-MM-DD``) for a stored UTC timestamp."""
    tz = tz or report_timezone()
    dt = try_parse_iso_ts(value)
    if dt is None:
        return str(value)[:10]
    return dt.astimezone(tz).strftime("%Y-%m-%d")


# ── Campaign identity normalization ──────────────────────────────────────────
_BRACE_RE = re.compile(r"^[{\[]+|[}\]]+$")


def normalize_campaign_key(value: Any) -> str:
    """Canonicalize a campaign identifier for cross-source joins.

    Spend rows carry numeric platform IDs or (CSV imports) the display name with
    en-dashes; touchpoints carry UTM slugs like ``@pmax-qc-broad-prospecting`` or
    braced templates like ``{22687198727}`` / ``%7B22687198727%7D``. This maps all
    of them to a comparable token: URL-decode braces, strip wrapping braces and a
    leading ``@``, lowercase, unify dash variants, collapse whitespace to hyphens.
    Pure-numeric IDs are returned as-is (digits only) so ``{22687198727}`` and
    ``22687198727`` match.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # URL-decode common brace encodings (%7B/%7D and double-encoded %257B/%257D).
    s = (
        s.replace("%257B", "{").replace("%257D", "}")
        .replace("%7B", "{").replace("%7D", "}")
        .replace("%7b", "{").replace("%7d", "}")
    )
    s = _BRACE_RE.sub("", s.strip())
    s = s.lstrip("@").strip()
    # If it's a bare numeric id, that's the canonical form.
    if s.isdigit():
        return s
    s = s.lower()
    # Unify en/em dash and underscores to hyphen; collapse whitespace to hyphen.
    s = s.replace("–", "-").replace("—", "-").replace("_", "-")
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if s == "":
        return default
    try:
        return int(float(s))
    except ValueError:
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s == "":
        return default
    try:
        return float(s)
    except ValueError:
        return default


def safe_div(n: float, d: float) -> float | None:
    if d == 0:
        return None
    return n / d


def round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return float(f"{value:.2f}")


def parse_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    s = str(value).strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}


def exp_decay_weight(delta_days: float, half_life_days: float = 7.0) -> float:
    if half_life_days <= 0:
        return 1.0
    lam = math.log(2.0) / half_life_days
    return math.exp(-lam * max(0.0, delta_days))


# ── Warehouse schema helpers ─────────────────────────────────────────────────
def table_columns(db_path: str, table: str) -> set[str]:
    from attributionops.db import is_postgres

    try:
        if is_postgres():
            return {
                str(r.get("name") or "")
                for r in query(
                    db_path,
                    "SELECT column_name AS name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :t",
                    {"t": table},
                ).rows
            }
        return {str(r.get("name") or "") for r in query(db_path, f"PRAGMA table_info({table})").rows}
    except Exception:
        return set()


def session_identity_agg_exprs(session_cols: set[str]) -> tuple[str, str]:
    """SELECT expressions (visitor, customer) for the per-session identity subquery.

    Each aggregate is guarded by actual column presence: on a warehouse whose
    sessions table lacks one of these columns, hard-coding MAX(visitor_id)/
    MAX(customer_key) makes SQLite raise "misuse of aggregate: MAX()" and crash
    the whole query.
    """
    session_visitor_agg = (
        "MAX(NULLIF(visitor_id, '')) AS visitor_id"
        if "visitor_id" in session_cols
        else "'' AS visitor_id"
    )
    session_customer_agg = (
        "MAX(NULLIF(customer_key, '')) AS customer_key"
        if "customer_key" in session_cols
        else "'' AS customer_key"
    )
    return session_visitor_agg, session_customer_agg

