#requires -version 5.1
<#
.SYNOPSIS
  高压 + 复杂查询：大表 BULKINSERTFAST 加载（analytics  schema）后跑标准 + 分析类 MDB 查询压测。

.DESCRIPTION
  1) 加载 `DEFATTR(id:int,dept:int,val:int,k:string)`，dept=id%100、k=t{id%50}，便于 GROUP BY / 索引扫描。
  2) `CREATE INDEX ik ON <table>(k)` 后 `FLUSH PERSIST`。
  3) `structdb_app --query-bench --bench-profile all`（COUNT/WHERE/PAGE_JSON/GROUP BY/SCAN INDEX/…）。

.PARAMETER Rows
  加载行数；默认读环境变量 STRUCTDB_QUERY_COMPLEX_ROWS，否则 200000。

.NOTES
  结果：`scripts/results/mdb_query_complex_summary_<UTC>.json`
  对比：`python benchmarks/scripts/compare_mdb_query_summary.py --baseline ... --current ...`
#>
param(
  [string]$BuildDir = "build",
  [ValidateSet("RelWithDebInfo", "Release", "Debug")]
  [string]$Configuration = "RelWithDebInfo",
  [long]$Rows = 0,
  [int]$RowsPerLine = 800,
  [string]$Table = "qcx",
  [int]$BenchPageSize = 100,
  [int]$BenchWarmup = 1,
  [int]$BenchIters = 5,
  [string]$BenchProfile = "all",
  [switch]$EngineBulkImport,
  [switch]$MemtableBulkPut,
  [string]$BaselineJson = "",
  [switch]$SkipCompare,
  [switch]$ColdRestart,
  [switch]$SkipLoad,
  [switch]$EchoProgress,
  [string]$DataDir = "",
  [string]$SessionDir = ""
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
  throw "structdb_app.exe not found under BuildDir=$Root (tried configs: $($names -join ', '))"
}

function New-AnalyticsBulkChunk {
  param([long]$StartId, [int]$Count)
  $sb = New-Object System.Text.StringBuilder
  for ($i = 0; $i -lt $Count; $i++) {
    $rid = $StartId + $i
    if ($i -gt 0) { [void]$sb.Append("|") }
    $dept = $rid % 100
    $val = [int](($rid * 17) % 1000000)
    $kTag = "t$($rid % 50)"
    [void]$sb.Append("$rid,$rid,$dept,$val,""$kTag""")
  }
  return $sb.ToString()
}

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

function Invoke-StructdbApp {
  param(
    [string[]]$AppArgs,
    [switch]$StreamOutput
  )
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  $psi.Arguments = ($AppArgs | ForEach-Object { if ($_ -match '\s') { "`"$_`"" } else { $_ } }) -join ' '
  $p = [System.Diagnostics.Process]::Start($psi)
  if (-not $StreamOutput) {
    $out = $p.StandardOutput.ReadToEnd()
    $err = $p.StandardError.ReadToEnd()
    $p.WaitForExit()
    return @{ ExitCode = $p.ExitCode; Out = $out; Err = $err }
  }
  $outSb = New-Object System.Text.StringBuilder
  $errSb = New-Object System.Text.StringBuilder
  $outHandler = {
    if ($EventArgs.Data) {
      [void]$Event.MessageData.Sb.AppendLine($EventArgs.Data)
      Write-Host $EventArgs.Data
    }
  }
  $errHandler = {
    if ($EventArgs.Data) {
      [void]$Event.MessageData.Sb.AppendLine($EventArgs.Data)
      Write-Host $EventArgs.Data -ForegroundColor Yellow
    }
  }
  $outReg = Register-ObjectEvent -InputObject $p -EventName OutputDataReceived -Action $outHandler -MessageData @{ Sb = $outSb }
  $errReg = Register-ObjectEvent -InputObject $p -EventName ErrorDataReceived -Action $errHandler -MessageData @{ Sb = $errSb }
  $p.BeginOutputReadLine()
  $p.BeginErrorReadLine()
  $p.WaitForExit()
  Unregister-Event -SourceIdentifier $outReg.Name -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier $errReg.Name -ErrorAction SilentlyContinue
  return @{ ExitCode = $p.ExitCode; Out = $outSb.ToString(); Err = $errSb.ToString() }
}

function New-StructdbAppArgs {
  param(
    [string[]]$BaseArgs,
    [switch]$StreamOutput
  )
  $args = [System.Collections.Generic.List[string]]::new()
  $args.AddRange($BaseArgs)
  if ($StreamOutput) { $args.Add("--mdb-stream-log") }
  if ($EngineBulkImport) { $args.Add("--mdb-bulk-import") }
  if ($MemtableBulkPut) { $args.Add("--memtable-bulk-put") }
  return @($args)
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
$repoRoot = Split-Path -Parent (Split-Path -Parent $here)
$resultsDir = Join-Path (Split-Path -Parent $here) "results"
if (-not (Test-Path -LiteralPath $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }

if ($Rows -le 0) {
  $envN = [Environment]::GetEnvironmentVariable("STRUCTDB_QUERY_COMPLEX_ROWS")
  if ([string]::IsNullOrWhiteSpace($envN)) { $Rows = [long]200000 } else { $Rows = [long]$envN }
}
$rpl = [Math]::Max(50, $RowsPerLine)
$showProgress = $EchoProgress.IsPresent -or $Rows -ge 100000

$App = Find-StructdbApp $BuildDir $Configuration
Write-Host "structdb_app: $App" -ForegroundColor DarkGray
$stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss_fff")
$ownedWork = [string]::IsNullOrWhiteSpace($DataDir)
if ($ownedWork) {
  $workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("structdb_qcx_" + $stamp)
  New-Item -ItemType Directory -Path $workRoot | Out-Null
  $dataDir = Join-Path $workRoot "_data"
  $sessDir = Join-Path $workRoot "embed_session"
} else {
  $workRoot = $null
  $dataDir = (Resolve-Path -LiteralPath $DataDir).Path
  $sessDir = if ([string]::IsNullOrWhiteSpace($SessionDir)) { Join-Path $dataDir "embed_session" } else { $SessionDir }
}

try {
  $loadWallMs = 0
  $loadTps = 0.0
  if (-not $SkipLoad -and $Rows -gt 0) {
    $loadDir = if ($workRoot) { $workRoot } else { $dataDir }
    $mdb = Join-Path $loadDir "complex_load.mdb"
    if ($showProgress) {
      Write-Host ("=== Generate + load {0:N0} rows ===" -f $Rows) -ForegroundColor Cyan
    }
    $enc = New-Object System.Text.UTF8Encoding $false
    $mdbWriter = New-Object System.IO.StreamWriter($mdb, $false, $enc)
    try {
      $mdbWriter.WriteLine("CREATE TABLE($Table)")
      $mdbWriter.WriteLine("USE($Table)")
      $mdbWriter.WriteLine("DEFATTR(id:int,dept:int,val:int,k:string)")
      if (-not $EngineBulkImport) { $mdbWriter.WriteLine("IMPORT MODE ON") }
      $next = [long]1
      $bulkLines = 0
      while ($next -le $Rows) {
        $n = [int][Math]::Min([long]$rpl, $Rows - $next + 1)
        $mdbWriter.WriteLine("BULKINSERTFAST($(New-AnalyticsBulkChunk $next $n))")
        $next += $n
        $bulkLines++
        if ($showProgress -and ($bulkLines -eq 1 -or $bulkLines % 100 -eq 0 -or $next -gt $Rows)) {
          $done = [Math]::Min($next - 1, $Rows)
          $pct = [Math]::Round(100.0 * $done / $Rows, 1)
          Write-Host ("[mdb-gen] {0:N0} / {1:N0} rows ({2}%) bulk_lines={3}" -f $done, $Rows, $pct, $bulkLines)
        }
      }
      $mdbWriter.WriteLine("FLUSH PERSIST")
      $mdbWriter.WriteLine("CREATE INDEX ik ON $Table(k)")
      $mdbWriter.WriteLine("FLUSH PERSIST")
      $mdbWriter.WriteLine("COUNT")
    } finally {
      $mdbWriter.Dispose()
    }
    $swLoad = [System.Diagnostics.Stopwatch]::StartNew()
    $loadBase = @("--data-dir", $dataDir, "--session-dir", $sessDir, "--run-mdb", $mdb)
    $lr = Invoke-StructdbApp (New-StructdbAppArgs $loadBase -StreamOutput:$showProgress) -StreamOutput:$showProgress
    $swLoad.Stop()
    if ($lr.ExitCode -ne 0) { throw "load failed: $($lr.Err)`n$($lr.Out)" }
    $loadWallMs = $swLoad.ElapsedMilliseconds
    $loadTps = if ($loadWallMs -gt 0) { [double]$Rows / ($loadWallMs / 1000.0) } else { 0 }
    if ($showProgress) {
      Write-Host ("=== Load done: {0:N0} rows in {1} ms (~{2:N0} TPS) ===" -f $Rows, $loadWallMs, [Math]::Round($loadTps, 0)) -ForegroundColor Cyan
    }
  }

  $rowHint = if ($Rows -gt 0) { $Rows } else { 200000 }
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
      "--bench-iters", "$BenchIters",
      "--bench-profile", $BenchProfile
    )
    if ($showProgress) {
      Write-Host ("=== Query bench ({0}) profile={1} ===" -f $Label, $BenchProfile) -ForegroundColor Cyan
    }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $qr = Invoke-StructdbApp (New-StructdbAppArgs $qargs) -StreamOutput:$showProgress
    $sw.Stop()
    if ($qr.ExitCode -ne 0) { throw "query-bench ($Label) failed: $($qr.Err)`n$($qr.Out)" }
    @{
      label  = $Label
      wallMs = $sw.ElapsedMilliseconds
      rows   = Parse-QueryBenchLines $qr.Out
    }
  }

  $warm = & $runBench "warm_session"
  $cold = $null
  if ($ColdRestart) { $cold = & $runBench "cold_session" }

  $summary = [ordered]@{
    timestamp          = [DateTime]::UtcNow.ToString("o")
    benchmark_profile  = "mdb_query_complex_v1"
    table              = $Table
    load_rows          = $Rows
    row_count_hint     = $rowHint
    bench_page_size    = $BenchPageSize
    bench_warmup       = $BenchWarmup
    bench_iters        = $BenchIters
    bench_profile      = $BenchProfile
    schema             = "id,dept,val,k + INDEX ik(k)"
    data_dir           = $dataDir
    load_wall_ms       = $loadWallMs
    load_tps_est       = [Math]::Round($loadTps, 1)
    query_wall_ms      = $warm.wallMs
    queries_warm       = Summarize-BenchPhase $warm.rows
    queries_cold       = if ($cold) { Summarize-BenchPhase $cold.rows } else { $null }
    cold_restart       = [bool]$ColdRestart
    engine_bulk_import = [bool]$EngineBulkImport
  }

  $outJson = Join-Path $resultsDir ("mdb_query_complex_summary_{0}.json" -f $stamp)
  Write-Utf8NoBom $outJson (($summary | ConvertTo-Json -Depth 8))
  Write-Host "=== MDB query complex stress ($Table, rows=$rowHint, profile=$BenchProfile) ===" -ForegroundColor Cyan
  if ($Rows -gt 0) {
    Write-Host ("Load: {0} rows in {1} ms (~{2} TPS)" -f $Rows, $loadWallMs, [Math]::Round($loadTps, 1))
  }
  Write-Host "Query latency (warm, ms_p95):"
  $summary.queries_warm | Format-Table -AutoSize | Out-String | Write-Host
  if ($cold) {
    Write-Host "Query latency (cold restart, ms_p95):"
    $summary.queries_cold | Format-Table -AutoSize | Out-String | Write-Host
  }
  Write-Host "JSON -> $outJson"

  if (-not $SkipCompare) {
    $baseline = $BaselineJson
    if ([string]::IsNullOrWhiteSpace($baseline)) {
      $baseline = Join-Path $repoRoot "benchmarks\baselines\mdb_query_complex_baseline.json"
    }
    if (Test-Path -LiteralPath $baseline) {
      $comparePy = Join-Path $repoRoot "benchmarks\scripts\compare_mdb_query_summary.py"
      Write-Host "=== Compare vs baseline ($baseline) ===" -ForegroundColor Cyan
      & python $comparePy --baseline $baseline --current $outJson --ignore-queries scan_index_ik_stats_full
      if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    } else {
      Write-Host "Skip compare: baseline not found ($baseline). Promote with: python benchmarks/scripts/promote_mdb_query_baseline.py --source $outJson" -ForegroundColor Yellow
    }
  }
}
finally {
  if ($ownedWork -and $workRoot -and (Test-Path -LiteralPath $workRoot)) {
    Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
  }
}
