#requires -version 5.1
<#
.SYNOPSIS
  OLTP persist micro-benchmark: 1K single-row INSERT + UPDATE via structdb_app --oltp-persist-micro.

.OUTPUTS
  benchmarks/baselines/oltp_persist_baseline.json (canonical)
  scripts/results/oltp_persist_summary_<UTC>.json (run archive)
#>
param(
  [string]$BuildDir = "build",
  [long]$Rows = 1000,
  [string]$BuildConfig = "Release"
)

$ErrorActionPreference = "Stop"

function Find-StructdbApp {
  param([string]$Root)
  $candidates = @(
    (Join-Path $Root "src\app\$BuildConfig\structdb_app.exe"),
    (Join-Path $Root "src\app\Release\structdb_app.exe"),
    (Join-Path $Root "src\app\RelWithDebInfo\structdb_app.exe"),
    (Join-Path $Root "src\app\Debug\structdb_app.exe"),
    (Join-Path $Root "$BuildConfig\structdb_app.exe"),
    (Join-Path $Root "Release\structdb_app.exe")
  )
  foreach ($p in $candidates) {
    if (Test-Path -LiteralPath $p) { return (Resolve-Path -LiteralPath $p).Path }
  }
  throw "structdb_app.exe not found under BuildDir=$Root (BuildConfig=$BuildConfig)"
}

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$App = Find-StructdbApp $BuildDir
$stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss_fff")
$workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("structdb_oltp_" + $stamp)
New-Item -ItemType Directory -Path $workRoot -Force | Out-Null

try {
  $data = Join-Path $workRoot "_data"
  $sess = Join-Path $workRoot "embed_session"
  New-Item -ItemType Directory -Path $data -Force | Out-Null
  New-Item -ItemType Directory -Path $sess -Force | Out-Null

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  $psi.Arguments = @(
    "--data-dir", $data,
    "--session-dir", $sess,
    "--oltp-persist-micro",
    "--oltp-rows", "$Rows"
  ) -join ' '
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  $sw.Stop()
  if ($p.ExitCode -ne 0) {
    throw "structdb_app exit $($p.ExitCode): $err`n$out"
  }

  $jsonLine = ($out -split "`n" | Where-Object { $_ -match '^\[OLTP_MICRO_JSON\]' } | Select-Object -Last 1)
  if (-not $jsonLine) { throw "missing [OLTP_MICRO_JSON] in output" }
  $micro = $jsonLine -replace '^\[OLTP_MICRO_JSON\]', '' | ConvertFrom-Json

  $gitSha = $null
  try {
    $gitSha = (git -C $repoRoot rev-parse --short HEAD 2>$null)
  } catch { }

  $summary = [ordered]@{
    timestamp           = [DateTime]::UtcNow.ToString("o")
    benchmark_profile   = "oltp_persist_micro_v1"
    rows                = [long]$Rows
    wall_ms             = $sw.ElapsedMilliseconds
    build_dir           = $BuildDir
    build_config        = $BuildConfig
    git_sha             = $gitSha
    insert_p50_ms       = [double]$micro.insert_p50_ms
    insert_p99_ms       = [double]$micro.insert_p99_ms
    insert_tps          = [double]$micro.insert_tps
    update_p50_ms       = [double]$micro.update_p50_ms
    update_p99_ms       = [double]$micro.update_p99_ms
    update_tps          = [double]$micro.update_tps
    insert_total_ms     = [double]$micro.insert_total_ms
    update_total_ms     = [double]$micro.update_total_ms
    mdb_incremental_persist_default = $true
  }

  $baselinesDir = Join-Path $repoRoot "benchmarks\baselines"
  if (-not (Test-Path -LiteralPath $baselinesDir)) {
    New-Item -ItemType Directory -Path $baselinesDir -Force | Out-Null
  }
  $canonical = Join-Path $baselinesDir "oltp_persist_baseline.json"
  $archiveDir = Join-Path $repoRoot "scripts\results"
  if (-not (Test-Path -LiteralPath $archiveDir)) {
    New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null
  }
  $archive = Join-Path $archiveDir ("oltp_persist_summary_{0}.json" -f $stamp)
  $jsonText = ($summary | ConvertTo-Json -Depth 6)
  Write-Utf8NoBom $canonical $jsonText
  Write-Utf8NoBom $archive $jsonText
  Write-Host "OLTP baseline: $canonical (archive: $archive)"
  Write-Host ("  insert p50/p99={0}/{1} ms tps={2}" -f $micro.insert_p50_ms, $micro.insert_p99_ms, [Math]::Round($micro.insert_tps, 1))
  Write-Host ("  update p50/p99={0}/{1} ms tps={2}" -f $micro.update_p50_ms, $micro.update_p99_ms, [Math]::Round($micro.update_tps, 1))
}
finally {
  Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
}
