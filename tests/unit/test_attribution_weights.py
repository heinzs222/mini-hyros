"""Unit tests for the attribution weighting models (_weights_for_model)."""

from __future__ import annotations

import pytest

from attributionops.tools.attribution import _weights_for_model
from attributionops.util import parse_iso_ts

ORDER_TS = parse_iso_ts("2026-01-15T12:00:00Z")


def _tps(*timestamps: str) -> list[dict]:
    return [{"ts": ts} for ts in timestamps]


def test_empty_touchpoints_returns_empty():
    assert _weights_for_model("last_click", [], ORDER_TS) == []


def test_single_touchpoint_gets_full_weight():
    assert _weights_for_model("last_click", _tps("2026-01-10T00:00:00Z"), ORDER_TS) == [1.0]


def test_last_click_assigns_all_weight_to_final_touch():
    w = _weights_for_model("last_click", _tps("a", "b", "c"), ORDER_TS)
    assert w == [0.0, 0.0, 1.0]


def test_first_click_assigns_all_weight_to_first_touch():
    w = _weights_for_model("first_click", _tps("a", "b", "c"), ORDER_TS)
    assert w == [1.0, 0.0, 0.0]


def test_linear_splits_evenly():
    w = _weights_for_model("linear", _tps("a", "b", "c", "d"), ORDER_TS)
    assert w == [0.25, 0.25, 0.25, 0.25]


def test_time_decay_weights_sum_to_one_and_favor_recent():
    tps = _tps("2026-01-05T12:00:00Z", "2026-01-14T12:00:00Z")
    w = _weights_for_model("time_decay", tps, ORDER_TS)
    assert sum(w) == pytest.approx(1.0)
    # The more recent touchpoint must carry more weight.
    assert w[1] > w[0]


def test_data_driven_proxy_two_touches_is_forty_sixty_split_endpoints():
    # With only two touches there is no middle, so 0.4 + 0.4 normalised stays as-is.
    w = _weights_for_model("data_driven_proxy", _tps("a", "b"), ORDER_TS)
    assert w == [0.4, 0.4]


def test_data_driven_proxy_distributes_middle_evenly():
    w = _weights_for_model("data_driven_proxy", _tps("a", "b", "c", "d"), ORDER_TS)
    # 0.4 first, 0.4 last, 0.2 spread across the 2 middle touches.
    assert w[0] == pytest.approx(0.4)
    assert w[-1] == pytest.approx(0.4)
    assert w[1] == pytest.approx(0.1)
    assert w[2] == pytest.approx(0.1)
    assert sum(w) == pytest.approx(1.0)


@pytest.mark.parametrize(
    "alias,expected_last_weight",
    [
        ("last", 1.0),
        ("LAST_CLICK", 1.0),
        ("last-click", 1.0),
        ("last click", 1.0),
    ],
)
def test_model_name_normalisation(alias, expected_last_weight):
    w = _weights_for_model(alias, _tps("a", "b"), ORDER_TS)
    assert w[-1] == expected_last_weight


def test_unknown_model_raises():
    with pytest.raises(ValueError, match="model must be one of"):
        _weights_for_model("made_up_model", _tps("a", "b"), ORDER_TS)
