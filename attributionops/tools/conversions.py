from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def conversions_push(*, platform: str, events: list[dict[str, Any]], out_dir: str = "data/outbox") -> dict[str, object]:
    # Local-only: write payload to disk so you can validate shape before wiring real APIs.
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(out_dir) / f"conversions_push_{platform}_{ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump({"platform": platform, "events": events}, f, indent=2, sort_keys=True)
    return {"ok": True, "written_to": str(path), "event_count": len(events), "mode": "local_dummy"}

