"""Repository-wide pytest bootstrap for build-time source patches."""

from pathlib import Path

from scripts.apply_backend_sync_fix import apply_backend_sync_fix
from scripts.apply_report_integrity_fixes import apply_report_integrity_fixes


ROOT = Path(__file__).resolve().parent
apply_backend_sync_fix(ROOT / "backend" / "api" / "ghl_sync.py")
apply_report_integrity_fixes(ROOT / "attributionops" / "report.py")
