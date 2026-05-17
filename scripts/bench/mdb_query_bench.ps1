#requires -version 5.1
<#
.SYNOPSIS
  MDB 查询性能：可选先 bulk 加载，再在同一 data_dir 上跑 COUNT / WHERE / PAGE_JSON 计时。

.NOTES
  依赖 structdb_app --query-bench（单进程、单会话，输出 [QUERY_BENCH] 行）。
  结果：scripts/results/mdb_query_summary_<UTC>.json
#>
param(
  [string]$BuildDir = "build",
  [ValidateSet("RelWithDebInfo", "Release", "Debug")]
  [string]$Configuration = "RelWithDebInfo",
  [long]$LoadRows = 100000,
  [int]$RowsPerLine = 800,
  [string]$Table = "mega",
  [int]$BenchPageSize = 100,
  [int]$BenchWarmup = 1,
  [int]$BenchIters = 5,
  [switch]$SkipLoad,
  [string]$DataDir = "",
  [string]$SessionDir = "",
  [switch]$EngineBulkImport,
  [switch]$ColdRestart,
  [switch]$MemtableBulkPut,
  [switch]$VariedSchema,
  [ValidateSet("standard", "analytics", "all")]
  [string]$BenchProfile = "standard"
)

$ErrorActionPreference = "Stop"

function Find-StructdbApp {
  param([string]$Root, [string]$PreferredConfig)
  $names = @($PreferredConfig, "RelWithDebInfo", "Release", "Debug") | Select-Object -Unique
  foreach ($cfg in $names) {
    $candidates = @(
      (Join-Path $Root "src\app\$cfg\structdb_app.exe"),
      (Join-Path $Root "$cfg\structdb_app.exe")
    )
    foreach ($p in $candidates) {
      if (Test-Path -LiteralPath $p) { return (Resolve-Path -LiteralPath $p).Path }
    }
  }
  throw "structdb_app.exe not found under BuildDir=$Root"
}

function New-BulkChunk {
  param([long]$StartId, [int]$Count, [switch]$Varied)
  $sb = New-Object System.Text.StringBuilder
  for ($i = 0; $i -lt $Count; $i++) {
    $rid = $StartId + $i
    if ($i -gt 0) { [void]$sb.Append("|") }
    if ($Varied) {
      $tag = "t$rid"
      $payload = "pay-$rid-unique-payload"
      [void]$sb.Append("$rid,$rid,$rid,""$tag"",""$payload""")
    } else {
      [void]$sb.Append("$rid,$rid,0,0")
    }
  }
  return $sb.ToString()
}

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

function Invoke-StructdbApp {
  param([string[]]$AppArgs)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  $psi.Arguments = ($AppArgs | ForEach-Object { if ($_ -match '\s') { "`"$_`"" } else { $_ } }) -join ' '
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  return @{ ExitCode = $p.ExitCode; Out = $out; Err = $err }
}

function Parse-QueryBenchLines {
  param([string]$Text)
  $rows = @()
  foreach ($line in ($Text -split "`n")) {
    if ($line -notmatch '^\[QUERY_BENCH\]') { continue }
    $row = @{ raw = $line.Trim() }
    foreach ($kv in ($line -replace '^\[QUERY_BENCH\]\s*', '') -split '\s+') {
      if ($kv -match '^([^=]+)=(.*)$') { $row[$Matches[1]] = $Matches[2] }
    }
    $rows += [pscustomobject]$row
  }
  return $rows
}

function Summarize-BenchPhase {
  param($Rows)
  $bench = $Rows | Where-Object { $_.phase -eq 'bench' -and $_.ok -eq '1' }
  $groups = $bench | Group-Object name
  $out = @()
  foreach ($g in $groups) {
    $ms = @($g.Group | ForEach-Object { [double]$_.ms })
    $sorted = $ms | Sort-Object
    $n = $sorted.Count
    if ($n -eq 0) { continue }
    $p50 = $sorted[[int][Math]::Floor(($n - 1) * 0.5)]
    $p95 = $sorted[[int][Math]::Floor(($n - 1) * 0.95)]
    $avg = ($ms | Measure-Object -Average).Average
    $out += [ordered]@{
      name   = $g.Name
      n      = $n
      ms_avg = [Math]::Round($avg, 3)
      ms_p50 = [Math]::Round($p50, 3)
      ms_p95 = [Math]::Round($p95, 3)
    }
  }
  return $out
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $here "..\..")
$resultsDir = Join-Path (Join-Path $here "..") "results"
if (-not (Test-Path -LiteralPath $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }

$App = Find-StructdbApp $BuildDir $Configuration
$stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss_fff")
$ownedWork = [string]::IsNullOrWhiteSpace($DataDir)
if ($ownedWork) {
  $workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("structdb_qbench_" + $stamp)
  New-Item -ItemType Directory -Path $workRoot | Out-Null
  $dataDir = Join-Path $workRoot "_data"
  $sessDir = Join-Path $workRoot "embed_session"
} else {
  $workRoot = $null
  $dataDir = (Resolve-Path -LiteralPath $DataDir).Path
  $sessDir = if ([string]::IsNullOrWhiteSpace($SessionDir)) { Join-Path $dataDir "embed_session" } else { $SessionDir }
}

try {
  if (-not $SkipLoad -and $LoadRows -gt 0) {
    $mdb = Join-Path $(if ($workRoot) { $workRoot } else { $dataDir }) "load.mdb"
    $lines = New-Object System.Collections.Generic.List[string]
    [void]$lines.Add("CREATE TABLE($Table)")
    [void]$lines.Add("USE($Table)")
    if ($VariedSchema) {
      [void]$lines.Add("DEFATTR(id:int,a:int,tag:string,payload:string)")
    } else {
      [void]$lines.Add("DEFATTR(id:int,a:int,b:int)")
    }
    if ($VariedSchema -and -not $EngineBulkImport) { [void]$lines.Add("IMPORT MODE ON") }
    $next = [long]1
    while ($next -le $LoadRows) {
      $n = [int][Math]::Min([long]$RowsPerLine, $LoadRows - $next + 1)
      [void]$lines.Add("BULKINSERTFAST($(New-BulkChunk $next $n -Varied:$VariedSchema))")
      $next += $n
    }
    [void]$lines.Add("FLUSH PERSIST")
    if ($VariedSchema) { [void]$lines.Add("REBUILD INDEX") }
    [void]$lines.Add("COUNT")
    Write-Utf8NoBom $mdb ($lines -join "`n")
    $loadArgs = @("--data-dir", $dataDir, "--session-dir", $sessDir, "--run-mdb", $mdb)
    if ($EngineBulkImport) { $loadArgs += "--mdb-bulk-import" }
    if ($MemtableBulkPut) { $loadArgs += "--memtable-bulk-put" }
    $swLoad = [System.Diagnostics.Stopwatch]::StartNew()
    $lr = Invoke-StructdbApp $loadArgs
    $swLoad.Stop()
    if ($lr.ExitCode -ne 0) { throw "load failed: $($lr.Err)`n$($lr.Out)" }
    $loadWallMs = $swLoad.ElapsedMilliseconds
    $loadTps = if ($loadWallMs -gt 0) { [double]$LoadRows / ($loadWallMs / 1000.0) } else { 0 }
  } else {
    $loadWallMs = 0
    $loadTps = 0
  }

  $rowHint = if ($LoadRows -gt 0) { $LoadRows } else { 100000 }
  $runBench = {
    param([string]$Label)
    $qargs = @(
      "--data-dir", $dataDir,
      "--session-dir", $sessDir,
      "--table", $Table,
      "--query-bench",
      "--bench-row-count", "$rowHint",
      "--bench-page-size", "$BenchPageSize",
      "--bench-warmup", "$BenchWarmup",
      "--bench-iters", "$BenchIters"
    )
    if ($VariedSchema) { $qargs += "--bench-varied" }
    if ($BenchProfile -and $BenchProfile -ne "standard") { $qargs += "--bench-profile"; $qargs += $BenchProfile }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $qr = Invoke-StructdbApp $qargs
    $sw.Stop()
    if ($qr.ExitCode -ne 0) { throw "query-bench ($Label) failed: $($qr.Err)`n$($qr.Out)" }
  @{
      label  = $Label
      wallMs = $sw.ElapsedMilliseconds
      rows   = Parse-QueryBenchLines $qr.Out
      text   = $qr.Out
    }
  }

  $warm = & $runBench "warm_session"
  $cold = $null
  if ($ColdRestart) {
    $cold = & $runBench "cold_session"
  }

  $summary = [ordered]@{
    timestamp       = [DateTime]::UtcNow.ToString("o")
    table           = $Table
    load_rows       = $LoadRows
    row_count_hint  = $rowHint
    bench_page_size = $BenchPageSize
    bench_warmup    = $BenchWarmup
    bench_iters     = $BenchIters
    data_dir        = $dataDir
    load_wall_ms    = $loadWallMs
    load_tps_est    = [Math]::Round($loadTps, 1)
    query_wall_ms   = $warm.wallMs
    queries_warm    = Summarize-BenchPhase $warm.rows
    queries_cold    = if ($cold) { Summarize-BenchPhase $cold.rows } else { $null }
    cold_restart    = [bool]$ColdRestart
    varied_schema   = [bool]$VariedSchema
  }

  $outJson = Join-Path $resultsDir ("mdb_query_summary_{0}.json" -f $stamp)
  Write-Utf8NoBom $outJson (($summary | ConvertTo-Json -Depth 6))
  Write-Host "=== MDB query bench ($Table, rows=$rowHint) ===" -ForegroundColor Cyan
  if ($LoadRows -gt 0) {
    Write-Host ("Load: {0} rows in {1} ms (~{2} TPS)" -f $LoadRows, $loadWallMs, [Math]::Round($loadTps, 1))
  }
  Write-Host "Query (warm session, bench phase ms):"
  $summary.queries_warm | Format-Table -AutoSize | Out-String | Write-Host
  if ($cold) {
    Write-Host "Query (cold session restart, bench phase ms):"
    $summary.queries_cold | Format-Table -AutoSize | Out-String | Write-Host
  }
  Write-Host "JSON -> $outJson"
}
finally {
  if ($ownedWork -and $workRoot -and (Test-Path -LiteralPath $workRoot)) {
    Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
  }
}
