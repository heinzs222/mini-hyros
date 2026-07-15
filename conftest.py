"""Repository-wide pytest bootstrap for build-time source patches."""

from pathlib import Path

from scripts.apply_backend_sync_fix import apply_backend_sync_fix


apply_backend_sync_fix(Path(__file__).resolve().parent / "backend" / "api" / "ghl_sync.py")
