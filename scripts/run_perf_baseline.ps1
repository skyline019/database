#Requires -Version 5.1
<#
.SYNOPSIS
  Run structdb_bench and capture a JSON baseline for put/get/prefix/open/recovery/flush/compaction.

.PARAMETER BuildConfig
  CMake configuration (default Release).

.PARAMETER BuildDir
  Build tree root (default <repo>/build).

.PARAMETER OutFile
  JSON output path. Default: benchmarks/baselines/structdb_bench_run_<timestamp>.json

.PARAMETER CompareBaseline
  When set, compare the run to benchmarks/baselines/structdb_bench_baseline.json via compare_bench.py.

.PARAMETER MaxRatio
  Max allowed current/baseline real_time ratio when -CompareBaseline is used (default 1.5).
#>
param(
  [string]$BuildConfig = "Release",
  [string]$BuildDir = "",
  [string]$OutFile = "",
  [switch]$CompareBaseline,
  [double]$MaxRatio = 1.5
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $BuildDir) {
  $BuildDir = Join-Path $repoRoot "build"
}

$benchExe = Join-Path (Join-Path (Join-Path $BuildDir "benchmarks") $BuildConfig) "structdb_bench.exe"
if (-not (Test-Path -LiteralPath $benchExe)) {
  throw "structdb_bench not found: $benchExe (configure with -DSTRUCTDB_BUILD_BENCHMARKS=ON)"
}

if (-not $OutFile) {
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $OutFile = Join-Path (Join-Path (Join-Path $repoRoot "benchmarks") "baselines") "structdb_bench_run_$stamp.json"
}

$outDir = Split-Path $OutFile -Parent
if (-not (Test-Path -LiteralPath $outDir)) {
  New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

Write-Host "Running $benchExe -> $OutFile"
& $benchExe `
  --benchmark_format=json `
  "--benchmark_out=$OutFile" `
  --benchmark_out_format=json
if ($LASTEXITCODE -ne 0) {
  throw "structdb_bench failed (exit $LASTEXITCODE)"
}

$rows = (Get-Content -LiteralPath $OutFile -Raw | ConvertFrom-Json).benchmarks
Write-Host ""
Write-Host "Benchmark summary (real_time ns/iter):"
foreach ($row in $rows) {
  if ($row.PSObject.Properties.Name -contains "real_time") {
    Write-Host ("  {0,-48} {1,12:N0}" -f $row.name, $row.real_time)
  }
}

if ($CompareBaseline) {
  $baseline = Join-Path (Join-Path (Join-Path $repoRoot "benchmarks") "baselines") "structdb_bench_baseline.json"
  $comparePy = Join-Path (Join-Path $repoRoot "benchmarks") "scripts" "compare_bench.py"
  if (-not (Test-Path -LiteralPath $baseline)) {
    throw "Baseline missing: $baseline"
  }
  if (-not (Test-Path -LiteralPath $comparePy)) {
    throw "compare_bench.py missing: $comparePy"
  }
  Write-Host ""
  Write-Host "Comparing to $baseline (max ratio $MaxRatio)..."
  & python $comparePy --baseline $baseline --current $OutFile --max-ratio $MaxRatio
  if ($LASTEXITCODE -ne 0) {
    throw "compare_bench.py failed (exit $LASTEXITCODE)"
  }
}

Write-Host ""
Write-Host "Done. JSON: $OutFile"
