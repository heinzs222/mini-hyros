"""Unit tests for attributionops.util — pure helpers, no I/O."""

from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pytest

from attributionops.util import (
    UTC,
    exp_decay_weight,
    iso_date,
    iso_ts,
    parse_iso_date,
    parse_iso_ts,
    parse_json,
    round_money,
    safe_div,
    to_float,
    to_int,
)


# ── date / timestamp parsing ──────────────────────────────────────────────────
def test_parse_iso_date():
    assert parse_iso_date("2026-01-15") == date(2026, 1, 15)


def test_parse_iso_ts_handles_trailing_z():
    assert parse_iso_ts("2026-01-23T02:41:28Z") == datetime(
        2026, 1, 23, 2, 41, 28, tzinfo=UTC
    )


def test_parse_iso_ts_converts_offset_to_utc():
    # 02:41 at +02:00 is 00:41 UTC.
    assert parse_iso_ts("2026-01-23T02:41:28+02:00") == datetime(
        2026, 1, 23, 0, 41, 28, tzinfo=UTC
    )


def test_iso_ts_strips_microseconds_and_appends_z():
    dt = datetime(2026, 1, 23, 2, 41, 28, 999999, tzinfo=timezone.utc)
    assert iso_ts(dt) == "2026-01-23T02:41:28Z"


def test_iso_date():
    assert iso_date(date(2026, 1, 5)) == "2026-01-05"


# ── numeric coercion ──────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "value,expected",
    [
        (None, 0),
        (5, 5),
        ("5", 5),
        ("5.9", 5),  # int(float("5.9"))
        ("", 0),
        ("abc", 0),
        ("1,000", 0),  # comma not handled by util.to_int
        (-3, -3),
    ],
)
def test_to_int(value, expected):
    assert to_int(value) == expected


def test_to_int_custom_default():
    assert to_int("nope", default=7) == 7


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, 0.0),
        (5, 5.0),
        (5.5, 5.5),
        ("5.5", 5.5),
        ("", 0.0),
        ("abc", 0.0),
        ("  12.25 ", 12.25),
    ],
)
def test_to_float(value, expected):
    assert to_float(value) == expected


def test_to_float_custom_default():
    assert to_float(None, default=1.5) == 1.5


# ── arithmetic helpers ────────────────────────────────────────────────────────
def test_safe_div_normal():
    assert safe_div(10, 4) == 2.5


def test_safe_div_by_zero_returns_none():
    assert safe_div(1, 0) is None


def test_round_money_none():
    assert round_money(None) is None


@pytest.mark.parametrize(
    "value,expected",
    [(1.239, 1.24), (2.5, 2.5), (10, 10.0), (1.0, 1.0), (-3.456, -3.46)],
)
def test_round_money(value, expected):
    assert round_money(value) == expected


# ── json parsing ──────────────────────────────────────────────────────────────
def test_parse_json_dict_passthrough():
    assert parse_json({"a": 1}) == {"a": 1}


def test_parse_json_string():
    assert parse_json('{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}


@pytest.mark.parametrize("value", [None, "", "   ", "not json", "{bad}"])
def test_parse_json_falls_back_to_empty_dict(value):
    assert parse_json(value) == {}


# ── exponential time-decay weighting ──────────────────────────────────────────
def test_exp_decay_weight_zero_delta_is_one():
    assert exp_decay_weight(0.0) == 1.0


def test_exp_decay_weight_at_one_half_life_is_one_half():
    assert exp_decay_weight(7.0, half_life_days=7.0) == pytest.approx(0.5)


def test_exp_decay_weight_negative_delta_clamped_to_one():
    # Touchpoints "after" the order shouldn't be boosted above 1.0.
    assert exp_decay_weight(-5.0, half_life_days=7.0) == 1.0


def test_exp_decay_weight_nonpositive_half_life_returns_one():
    assert exp_decay_weight(5.0, half_life_days=0.0) == 1.0


def test_exp_decay_weight_matches_formula():
    lam = math.log(2.0) / 7.0
    assert exp_decay_weight(3.0, half_life_days=7.0) == pytest.approx(math.exp(-lam * 3.0))
