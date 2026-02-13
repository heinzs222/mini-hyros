from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def logs_search(*, query: str, start_date: str, end_date: str, log_path: str = "data/dummy/logs.jsonl") -> dict[str, Any]:
    # Local-only stub: scan a jsonl file if present.
    path = Path(log_path)
    if not path.exists():
        return {"rows": [], "notes": ["No local logs file found."], "mode": "local_dummy"}

    rows: list[dict[str, Any]] = []
    q = query.lower()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            hay = json.dumps(obj, separators=(",", ":")).lower()
            if q in hay:
                rows.append(obj)
    return {"rows": rows, "row_count": len(rows), "mode": "local_dummy"}

