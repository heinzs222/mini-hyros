"""Server-owned periodic sync (SYNC_INTERVAL_MINUTES scheduler in main.py)."""

from __future__ import annotations

import asyncio

import main


def test_sync_interval_default_and_parsing(monkeypatch):
    monkeypatch.delenv("SYNC_INTERVAL_MINUTES", raising=False)
    assert main._sync_interval_minutes() == 30.0
    monkeypatch.setenv("SYNC_INTERVAL_MINUTES", "0")
    assert main._sync_interval_minutes() == 0.0
    monkeypatch.setenv("SYNC_INTERVAL_MINUTES", "15.5")
    assert main._sync_interval_minutes() == 15.5
    monkeypatch.setenv("SYNC_INTERVAL_MINUTES", "not-a-number")
    assert main._sync_interval_minutes() == 0.0


def test_scheduled_sync_runs_all_parts_without_credentials(api_db):
    # With no platform credentials configured, every handler returns its
    # "not configured" error payload quickly — the scheduler must complete,
    # carry all three results, and never raise.
    results = asyncio.run(main._run_scheduled_sync())
    assert set(results) >= {"start_date", "end_date", "spend", "stripe", "ghl"}
    assert results["start_date"] <= results["end_date"]


def test_scheduled_sync_isolates_a_failing_part(api_db, monkeypatch):
    import api.spend_sync as spend_sync_module

    async def boom(**kwargs):
        raise RuntimeError("simulated spend outage")

    monkeypatch.setattr(spend_sync_module, "sync_spend", boom)
    results = asyncio.run(main._run_scheduled_sync())
    # The failing part is captured as an error; the other parts still ran.
    assert results["spend"] == {"error": "simulated spend outage"}
    assert "stripe" in results and "ghl" in results


def test_auto_sync_loop_survives_a_crashing_run(monkeypatch):
    calls = {"n": 0}
    second_run_done = asyncio.Event()

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("first run crashes")
        second_run_done.set()
        return {}

    monkeypatch.setattr(main, "_run_scheduled_sync", flaky)

    # Shrink the loop's sleeps so iterations happen instantly; capture the real
    # sleep first so the shim still yields control to the event loop.
    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds):
        await real_sleep(0)

    monkeypatch.setattr(main.asyncio, "sleep", fast_sleep)

    async def run_until_second_iteration():
        task = asyncio.create_task(main._auto_sync_loop(30, initial_delay_seconds=0))
        await asyncio.wait_for(second_run_done.wait(), timeout=5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run_until_second_iteration())
    # The crash in iteration 1 did not kill the loop: it ran again.
    assert calls["n"] >= 2
