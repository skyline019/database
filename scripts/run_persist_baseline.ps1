#Requires -Version 5.1
<#
.SYNOPSIS
  Persist-path micro-benchmarks: structdb_bench + optional mega_data stress.

.PARAMETER BuildDir
  CMake build tree (default <repo>/build).

.PARAMETER RunMega
  Also run mega_data_mdb_stress.ps1 (100k rows, -ImportMode).

.PARAMETER Rows
  Rows for mega_data when -RunMega (default 100000).
#>
param(
  [string]$BuildDir = "",
  [switch]$RunMega,
  [long]$Rows = 100000,
  [string]$BuildConfig = "Release"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $BuildDir) { $BuildDir = Join-Path $repoRoot "build" }

& (Join-Path $PSScriptRoot "run_perf_baseline.ps1") -BuildDir $BuildDir -BuildConfig $BuildConfig

& (Join-Path $PSScriptRoot "bench" "oltp_persist_micro.ps1") -BuildDir $BuildDir -BuildConfig $BuildConfig

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$summaryPath = Join-Path (Join-Path (Join-Path $repoRoot "scripts") "results") "persist_baseline_$stamp.json"
$oltpBaseline = Join-Path (Join-Path $repoRoot "benchmarks") "baselines\oltp_persist_baseline.json"
$summary = [ordered]@{
  timestamp = [DateTime]::UtcNow.ToString("o")
  structdb_bench_json = (Get-ChildItem (Join-Path (Join-Path $repoRoot "benchmarks") "baselines") -Filter "structdb_bench_run_*.json" |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
  oltp_persist_baseline_json = $(if (Test-Path -LiteralPath $oltpBaseline) { $oltpBaseline } else { $null })
}

if ($RunMega) {
  & (Join-Path $PSScriptRoot "bench" "mega_data_mdb_stress.ps1") -BuildDir $BuildDir -Rows $Rows -RowsPerLine 1000
  & (Join-Path $PSScriptRoot "bench" "mega_data_mdb_stress.ps1") -BuildDir $BuildDir -Rows $Rows -RowsPerLine 1000 -ImportMode
  $mega = Get-ChildItem (Join-Path (Join-Path $repoRoot "scripts") "results") -Filter "mega_data_summary_*.json" |
    Sort-Object LastWriteTime -Descending | Select-Object -First 2
  $summary.mega_data_recent = @($mega | ForEach-Object { $_.FullName })
}

$outDir = Split-Path $summaryPath -Parent
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }
($summary | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $summaryPath -Encoding UTF8
Write-Host "Persist baseline summary: $summaryPath"
