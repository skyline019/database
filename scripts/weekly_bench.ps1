#requires -version 5.1
<#
.SYNOPSIS
  Weekly / CI bulk gate: mega_data 1M rows + TPS compare to baseline (90% floor).
#>
param(
  [string]$BuildDir = "",
  [long]$Rows = 1000000,
  [double]$MinTpsRatio = 0.90,
  [string]$BuildConfig = "Release"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $BuildDir) { $BuildDir = Join-Path $repoRoot "build" }

$baselineJson = Join-Path $repoRoot "benchmarks\baselines\mega_data_baseline.json"
if (-not (Test-Path -LiteralPath $baselineJson)) {
  $baselineJson = Join-Path $repoRoot "scripts\results\mega_data_summary_20260516_103927_414.json"
}

& (Join-Path $PSScriptRoot "bench\mega_data_mdb_stress.ps1") -BuildDir $BuildDir -Rows $Rows -RowsPerLine 1000
$current = Get-ChildItem (Join-Path $repoRoot "scripts\results") -Filter "mega_data_summary_*.json" |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not (Test-Path -LiteralPath $baselineJson)) {
  Write-Warning "No baseline JSON; skipping compare (current: $($current.FullName))"
  exit 0
}

$py = Join-Path $repoRoot "benchmarks\scripts\compare_mega_summary.py"
python $py --baseline $baselineJson --current $current.FullName --max-tps-ratio-delta 0.10
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$base = Get-Content -LiteralPath $baselineJson -Raw | ConvertFrom-Json
$cur = Get-Content -LiteralPath $current.FullName -Raw | ConvertFrom-Json
$btps = [double]$base.runtime_pressure_tps_est
$ctps = [double]$cur.runtime_pressure_tps_est
if ($btps -gt 0 -and ($ctps / $btps) -lt $MinTpsRatio) {
  throw "TPS $ctps below $MinTpsRatio of baseline $btps"
}
Write-Host "weekly_bench OK tps=$ctps baseline=$btps ratio=$([Math]::Round($ctps/$btps,3))"
