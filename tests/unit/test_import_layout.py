"""The backend must import its own modules by their deployed package path.

``backend/api/*.py`` is importable as ``api.x`` only when ``backend/`` itself is
on sys.path — true under the Docker entrypoint (``--app-dir /app/backend``) and
in this test suite, but NOT on Vercel, where the entrypoint is the repo-root
``app.py`` and the root ``api/`` package holds only ``index.py``.

An ``from api.x import ...`` inside a try/except therefore fails silently in
production. That is what made a GoHighLevel account connected through Settings
report "Not configured": the credential lookup raised ModuleNotFoundError and
fell back to unset environment variables.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"


def _local_api_imports(path: Path) -> list[str]:
    """Return `api.*` imports in one file (any depth, including inside defs)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "api" or module.startswith("api."):
                found.append(f"{path.relative_to(ROOT)}:{node.lineno} from {module} import ...")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "api" or alias.name.startswith("api."):
                    found.append(f"{path.relative_to(ROOT)}:{node.lineno} import {alias.name}")
    return found


def test_backend_never_imports_itself_as_the_bare_api_package():
    offenders: list[str] = []
    for path in sorted(BACKEND.rglob("*.py")):
        if "tests" in path.relative_to(BACKEND).parts:
            continue
        offenders.extend(_local_api_imports(path))

    assert offenders == [], (
        "These resolve only when backend/ is on sys.path and fail on Vercel; "
        "import them as backend.api.* instead:\n  " + "\n  ".join(offenders)
    )


def test_backend_api_modules_import_from_the_repo_root_alone():
    """Reproduce the deployed sys.path: repo root only, no backend/ entry."""
    program = (
        "import sys; sys.path.insert(0, %r)\n"
        "from backend.api.connections import _ghl_credentials, _meta_credentials\n"
        "from backend.api.ghl_sync import get_ghl_credentials\n"
        "from backend.api.platform_auth import get_meta_credentials\n"
        "print('ok')\n" % str(ROOT)
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=str(ROOT.parent),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
