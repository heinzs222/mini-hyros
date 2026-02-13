from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from typing import Any


UTC = timezone.utc


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_iso_ts(value: str) -> datetime:
    # Expect e.g. 2026-01-23T02:41:28Z
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


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

