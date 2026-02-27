# hyros (local sandbox)

This folder contains **synthetic (dummy) attribution data** you can use to test reporting/UI without connecting real ad platforms or a real warehouse.

## Generate dummy data

PowerShell:

```powershell
python .\scripts\generate_dummy_data.py --seed 42 --out-dir data/dummy --sqlite-path data/dummy/attributionops_demo.sqlite
```

One-shot demo (generates last-30-days dummy data + a report file):

```powershell
.\scripts\bootstrap_demo.ps1
```

Outputs:

- `data/dummy/spend.csv`
- `data/dummy/sessions.csv`
- `data/dummy/touchpoints.csv`
- `data/dummy/orders.csv`
- `data/dummy/conversions.csv`
- `data/dummy/reported_value.csv`
- `data/dummy/attributionops_demo.sqlite`

Notes:

- All values are **fake** and only meant for testing.
- `customer_key` is a deterministic sha256 token (no raw PII).

## Run local tools (dummy implementations)

These are local stand-ins for the required AttributionOps tools (SQLite-backed).

```powershell
# integrations.status()
python -m attributionops tool integrations.status

# tracking.health_check()
python -m attributionops tool tracking.health_check

# ads.list_platforms()
python -m attributionops tool ads.list_platforms

# ads.get_spend(platform,start,end,breakdown)
python -m attributionops tool ads.get_spend --start-date 2025-12-24 --end-date 2026-01-23 --breakdown campaign

# attribution.run(model,start,end,lookback,conversion_type,value_type)
python -m attributionops tool attribution.run --start-date 2025-12-24 --end-date 2026-01-23 --model last_click --lookback-days 30 --value-type revenue

# ads.get_reported_value(platform,start,end,breakdown,conversion_type)
python -m attributionops tool ads.get_reported_value --start-date 2025-12-24 --end-date 2026-01-23 --breakdown campaign --conversion-type Purchase
```

If your SQLite file is elsewhere:

```powershell
$env:ATTRIBUTIONOPS_DB_PATH="C:\path\to\attributionops_demo.sqlite"
python -m attributionops tool integrations.status
```

## Generate a HYROS-like performance report JSON

```powershell
python -m attributionops report --start-date 2025-12-24 --end-date 2026-01-23 --model last_click --lookback-days 30 --active-tab traffic_source
```

This prints a single JSON object matching the requested HYROS-like schema (built from the dummy warehouse data).

Optional click-date attribution (spend/revenue aligned to touchpoint date):

```powershell
python -m attributionops report --start-date 2025-12-24 --end-date 2026-01-23 --model last_click --lookback-days 30 --active-tab campaign --use-date-of-click-attribution
```

To write a UTF-8 file on Windows PowerShell (avoids UTF-16 redirection):

```powershell
python -m attributionops report --start-date 2025-12-24 --end-date 2026-01-23 --model last_click --lookback-days 30 --active-tab traffic_source --out data/dummy/report.json > $null
```
