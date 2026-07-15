#!/usr/bin/env python3
"""Idempotently apply the lightweight GHL sync endpoint changes.

The repository is patched during Docker builds and test collection so the deployed
backend and CI exercise the same source. This can be removed once the changes are
folded directly into backend/api/ghl_sync.py.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def _replace_exact(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"Cannot apply {label}: expected source text was not found")
    return text.replace(old, new, 1)


def _replace_pattern(
    text: str,
    pattern: str,
    replacement: str,
    done_needle: str,
    label: str,
) -> str:
    if done_needle in text:
        return text
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"Cannot apply {label}: expected source pattern was not found")
    return updated


def apply_backend_sync_fix(target: Path) -> bool:
    original = target.read_text(encoding="utf-8")
    text = original

    text = _replace_exact(
        text,
        '''async def ghl_sync(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    limit: int = Query(default=500, ge=1, le=2000),
):''',
        '''async def ghl_sync(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    limit: int = Query(default=500, ge=1, le=2000),
    include_forms: bool = Query(default=True),
    include_opportunities: bool = Query(default=True),
):''',
        "GHL sync query flags",
    )

    text = _replace_pattern(
        text,
        r'''            contacts_result, submissions_result, opportunities_result = await asyncio\.gather\(
.*?
                return_exceptions=True,
            \)''',
        '''            async def _empty_result() -> list[dict]:
                return []

            contacts_result, submissions_result, opportunities_result = await asyncio.gather(
                _fetch_contacts(
                    client, token, location_id, limit, start_date=start_date, end_date=end_date
                ),
                _fetch_form_submissions(
                    client, token, location_id, limit, start_date, end_date
                ) if include_forms else _empty_result(),
                _fetch_opportunities(client, token, location_id, limit)
                if include_opportunities else _empty_result(),
                return_exceptions=True,
            )''',
        "if include_forms else _empty_result()",
        "conditional GHL API fetches",
    )

    text = _replace_exact(
        text,
        '        "end_date": end_date,\n        "errors": errors,',
        '        "end_date": end_date,\n        "include_forms": include_forms,\n        "include_opportunities": include_opportunities,\n        "errors": errors,',
        "GHL sync response flags",
    )

    if text == original:
        print(f"Already patched {target}")
        return False

    target.write_text(text, encoding="utf-8")
    print(f"Patched {target}")
    return True


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    target = (
        Path(sys.argv[1]).resolve()
        if len(sys.argv) > 1
        else project_root / "backend" / "api" / "ghl_sync.py"
    )
    apply_backend_sync_fix(target)


if __name__ == "__main__":
    main()
