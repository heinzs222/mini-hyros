$ErrorActionPreference = "Stop"

$end = (Get-Date).ToString("yyyy-MM-dd")
$start = (Get-Date).AddDays(-30).ToString("yyyy-MM-dd")

python .\scripts\generate_dummy_data.py --seed 42 --start-date $start --end-date $end --out-dir data/dummy --sqlite-path data/dummy/attributionops_demo.sqlite

python -m attributionops report --start-date $start --end-date $end --model last_click --lookback-days 30 --active-tab traffic_source --out data/dummy/report.json > $null

Write-Host "Dummy data + report generated:"
Write-Host " - data/dummy/attributionops_demo.sqlite"
Write-Host " - data/dummy/report.json"

