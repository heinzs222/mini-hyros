from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def audiences_sync(
    *,
    platform: str,
    segment_definition: dict[str, Any],
    out_dir: str = "data/outbox",
) -> dict[str, object]:
    # Local-only: persist the audience definition for review.
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(out_dir) / f"audiences_sync_{platform}_{ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump({"platform": platform, "segment_definition": segment_definition}, f, indent=2, sort_keys=True)
    return {"ok": True, "written_to": str(path), "mode": "local_dummy"}

