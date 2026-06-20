"""Unit tests for the attributionops CLI dispatch (__main__.main)."""

from __future__ import annotations

import json

import pytest

from attributionops.__main__ import main
from tests.helpers import insert_rows, order, spend, touchpoint


def _run(capsys, argv) -> tuple[int, dict]:
    code = main(argv)
    out = capsys.readouterr().out
    return code, json.loads(out)


def test_tool_integrations_status(empty_db, capsys):
    code, payload = _run(capsys, ["--db", empty_db, "tool", "integrations.status"])
    assert code == 0
    assert payload["connected"] is True


def test_tool_tracking_health_check(empty_db, capsys):
    code, payload = _run(capsys, ["--db", empty_db, "tool", "tracking.health_check"])
    assert code == 0
    assert "coverage" in payload


def test_tool_list_platforms(empty_db, capsys):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10", platform="meta")])
    code, payload = _run(capsys, ["--db", empty_db, "tool", "ads.list_platforms"])
    assert code == 0
    assert payload == {"platforms": ["meta"]}


def test_tool_get_spend_requires_dates(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "ads.get_spend"])


def test_tool_attribution_run(empty_db, capsys):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad1")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "attribution.run",
         "--start-date", "2026-01-01", "--end-date", "2026-01-31"],
    )
    assert code == 0
    assert payload["rows"][0]["revenue"] == 100.0


def test_tool_get_spend(empty_db, capsys):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10", platform="meta", clicks=10)])
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "ads.get_spend", "--breakdown", "campaign",
         "--start-date", "2026-01-01", "--end-date", "2026-01-31"],
    )
    assert code == 0
    assert payload["breakdown"] == "campaign"


def test_tool_get_reported_value(empty_db, capsys):
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "ads.get_reported_value",
         "--start-date", "2026-01-01", "--end-date", "2026-01-31"],
    )
    assert code == 0
    assert payload["rows"] == []


def test_tool_get_reported_value_requires_dates(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "ads.get_reported_value"])


def test_tool_attribution_run_requires_dates(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "attribution.run"])


def test_tool_forecasting_run(empty_db, capsys):
    insert_rows(empty_db, "orders", [order("o1", "2026-01-10T00:00:00Z", "c1", net=100)])
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "forecasting.run", "--method", "simple",
         "--start-date", "2026-01-01", "--end-date", "2026-03-31"],
    )
    assert code == 0
    assert payload["rows"][0]["customers"] == 1


def test_tool_forecasting_run_requires_dates(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "forecasting.run"])


def test_tool_warehouse_query(empty_db, capsys):
    code, payload = _run(capsys, ["--db", empty_db, "tool", "warehouse.query", "--sql", "SELECT 1 AS x"])
    assert code == 0
    assert payload["rows"] == [{"x": 1}]


def test_tool_warehouse_query_requires_sql(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "warehouse.query"])


def test_tool_conversions_push_writes_to_outbox(empty_db, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)  # default out_dir is relative ("data/outbox")
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "conversions.push", "--platform", "meta",
         "--events-json", '[{"event": "Purchase"}]'],
    )
    assert code == 0
    assert payload["event_count"] == 1


def test_tool_audiences_sync_writes_to_outbox(empty_db, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "audiences.sync", "--platform", "google",
         "--segment-json", '{"name": "vip"}'],
    )
    assert code == 0
    assert payload["ok"] is True


def test_tool_logs_search(empty_db, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)  # default log path is relative and absent here
    code, payload = _run(
        capsys,
        ["--db", empty_db, "tool", "logs.search", "--query", "x",
         "--start-date", "2026-01-01", "--end-date", "2026-01-31"],
    )
    assert code == 0
    assert payload["rows"] == []


def test_unknown_tool_exits(empty_db):
    with pytest.raises(SystemExit):
        main(["--db", empty_db, "tool", "does.not.exist"])


def test_report_command_emits_json(empty_db, capsys):
    code, payload = _run(
        capsys,
        ["--db", empty_db, "report", "--start-date", "2026-01-01", "--end-date", "2026-01-31"],
    )
    assert code == 0
    assert "report_meta" in payload
    assert payload["table"]["active_tab"] == "traffic_source"


def test_report_command_writes_out_file(empty_db, tmp_path, capsys):
    out_file = tmp_path / "report.json"
    code = main(
        ["--db", empty_db, "report", "--start-date", "2026-01-01",
         "--end-date", "2026-01-31", "--out", str(out_file)],
    )
    capsys.readouterr()
    assert code == 0
    assert out_file.exists()
    assert "report_meta" in json.loads(out_file.read_text())
