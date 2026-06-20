# Tests

Automated tests for the attribution engine (`attributionops/`), the FastAPI
backend (`backend/`), and the Next.js dashboard (`dashboard/`).

## Python (pytest)

One-time setup:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
```

Run everything with branch coverage:

```bash
python -m pytest --cov=attributionops --cov=backend --cov-report=term-missing
```

Run a subset:

```bash
python -m pytest tests/unit          # pure logic: attribution engine, report math, tools, CLI
python -m pytest tests/api           # FastAPI endpoints, auth, webhooks, sync (respx-mocked)
python -m pytest tests/unit/test_attribution_run.py -q
```

### Layout & fixtures

- `tests/conftest.py` — `empty_db`: a fresh, schema-correct temporary SQLite
  warehouse per test (built from `scripts/init_empty_db.py`).
- `tests/helpers.py` — `insert_rows()` plus row builders (`order`, `touchpoint`,
  `spend`, `ad_name`) for seeding deterministic fixtures.
- `tests/api/conftest.py` — `api_db` (temp warehouse wired via
  `ATTRIBUTIONOPS_DB_PATH`, auth disabled) and `client` (a `TestClient` over the
  backend app). `backend/` is placed on `sys.path` so `import api.<module>` works.

### Conventions

- **No network.** Every outbound HTTP call (Meta/Google/TikTok/Stripe/GHL …) is
  mocked with [`respx`](https://lundberg.github.io/respx/) using
  `respx.mock(assert_all_mocked=False)` so the in-process `TestClient` → ASGI
  request still passes through.
- **Deterministic.** No `time.sleep`, no reliance on wall-clock dates beyond what
  the code under test computes itself.
- **Golden values.** The attribution engine is asserted to exact attributed
  revenue / order counts, not just "it ran".

## Dashboard (Vitest)

```bash
cd dashboard
npm install
npm run test
```

## CI

`.github/workflows/ci.yml` runs the Python suite (with a coverage gate) and the
dashboard Vitest suite on every push and pull request.
