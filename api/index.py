"""Vercel serverless entrypoint for the Mini Hyros FastAPI backend.

Vercel's Python runtime imports this file and serves the module-level ``app``
(an ASGI application). The backend lives in ``backend/`` and imports its route
routers as ``from api.webhooks import ...`` — i.e. it expects the name ``api``
to resolve to ``backend/api``, NOT to this repo-root ``api/`` directory that
merely hosts this entrypoint. We therefore put ``backend/`` first on sys.path and
bind ``api`` to the backend package before importing the app.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACKEND = ROOT / "backend"

# Backend first so `import api` (and its submodules) resolves to backend/api.
for p in (str(ROOT), str(BACKEND)):
    if p in sys.path:
        sys.path.remove(p)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(BACKEND))

# If the platform pre-imported this repo-root `api/` package under the name
# `api`, drop it so the backend's `api` package wins the (re)import below.
sys.modules.pop("api", None)
import api  # noqa: E402,F401  -> backend/api

from main import app  # noqa: E402,F401

# Exported for Vercel's ASGI runtime.
__all__ = ["app"]
