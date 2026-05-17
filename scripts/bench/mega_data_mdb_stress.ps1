#requires -version 5.1
<#
.SYNOPSIS
  单进程超大批量 MDB 插入（BULKINSERTFAST），用于验证大表 + 持久化路径。

.PARAMETER Rows
  总行数。未指定时读环境变量 STRUCTDB_MEGA_ROWS，默认 120000。

.PARAMETER RowsPerLine
  每条 BULKINSERTFAST 内的行数（| 分隔），默认 800。

.NOTES
  需要已构建的 structdb_app.exe（-BuildDir 下 Release/RelWithDebInfo/Debug）。
  结果 JSON：`scripts/results/mega_data_summary_<UTC>.json`
#>
param(
  [Parameter(Mandatory = $false)][string]$BuildDir = "build",
  [Parameter(Mandatory = $false)][long]$Rows = 0,
  [Parameter(Mandatory = $false)][int]$RowsPerLine = 800,
  [switch]$ImportMode,
  [switch]$EngineBulkImport,
  [switch]$Coalesce,
  [switch]$JournalSkipUntilCommit,
  [switch]$MemtableBulkPut,
  [int]$PersistChunkMaxPuts = 0,
  [long]$StorageEmbedBatchMaxFrameBytes = 0,
  [switch]$SampleWorkingSet
)

$ErrorActionPreference = "Stop"

function Find-StructdbApp {
  param([string]$Root)
  $candidates = @(
    (Join-Path $Root "src\app\Release\structdb_app.exe"),
    (Join-Path $Root "src\app\RelWithDebInfo\structdb_app.exe"),
    (Join-Path $Root "src\app\Debug\structdb_app.exe"),
    (Join-Path $Root "Release\structdb_app.exe"),
    (Join-Path $Root "RelWithDebInfo\structdb_app.exe"),
    (Join-Path $Root "Debug\structdb_app.exe")
  )
  foreach ($p in $candidates) {
    if (Test-Path -LiteralPath $p) { return (Resolve-Path -LiteralPath $p).Path }
  }
  throw "structdb_app.exe not found under BuildDir=$Root"
}

function New-BulkChunk {
  param([long]$StartId, [int]$Count)
  $sb = New-Object System.Text.StringBuilder
  for ($i = 0; $i -lt $Count; $i++) {
    $rid = $StartId + $i
    if ($i -gt 0) { [void]$sb.Append("|") }
    [void]$sb.Append("$rid,$rid,0,0")
  }
  return $sb.ToString()
}

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$resultsDir = Join-Path (Split-Path -Parent $here) "results"
if (-not (Test-Path -LiteralPath $resultsDir)) {
  New-Item -ItemType Directory -Path $resultsDir | Out-Null
}

if ($Rows -le 0) {
  $envN = [Environment]::GetEnvironmentVariable("STRUCTDB_MEGA_ROWS")
  if ([string]::IsNullOrWhiteSpace($envN)) { $Rows = [long]120000 } else { $Rows = [long]$envN }
}
$rpl = [Math]::Max(50, $RowsPerLine)

$App = Find-StructdbApp $BuildDir
$stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss_fff")
$workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("structdb_mega_" + $stamp + "_" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Path $workRoot | Out-Null

try {
  $data = Join-Path $workRoot "_data"
  $sess = Join-Path $workRoot "embed_session"
  New-Item -ItemType Directory -Path $data -Force | Out-Null
  New-Item -ItemType Directory -Path $sess -Force | Out-Null

  $mdb = Join-Path $workRoot "mega_load.mdb"
  $lines = New-Object System.Collections.Generic.List[string]
  [void]$lines.Add("CREATE TABLE(mega)")
  [void]$lines.Add("USE(mega)")
  [void]$lines.Add("DEFATTR(id:int,a:int,b:int)")
  if ($ImportMode -and -not $EngineBulkImport) {
    [void]$lines.Add("IMPORT MODE ON")
  }
  $next = [long]1
  while ($next -le $Rows) {
    $n = [int][Math]::Min([long]$rpl, $Rows - $next + 1)
    $chunk = New-BulkChunk $next $n
    [void]$lines.Add("BULKINSERTFAST($chunk)")
    $next += $n
  }
  if ($ImportMode -and -not $EngineBulkImport) {
    [void]$lines.Add("FLUSH PERSIST")
    [void]$lines.Add("REBUILD INDEX")
  }
  [void]$lines.Add("COUNT")
  Write-Utf8NoBom $mdb ($lines -join "`n")

  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  $appArgs = @("--data-dir", $data, "--session-dir", $sess, "--run-mdb", $mdb)
  if ($EngineBulkImport) { $appArgs += "--mdb-bulk-import" }
  if ($Coalesce) { $appArgs += "--mdb-persist-coalesce" }
  if ($JournalSkipUntilCommit) { $appArgs += "--embed-journal-skip-until-commit" }
  if ($MemtableBulkPut) { $appArgs += "--memtable-bulk-put" }
  if ($PersistChunkMaxPuts -gt 0) { $appArgs += @("--mdb-persist-chunk-max-puts", "$PersistChunkMaxPuts") }
  if ($StorageEmbedBatchMaxFrameBytes -gt 0) {
    $appArgs += @("--storage-embed-batch-max-frame-bytes", "$StorageEmbedBatchMaxFrameBytes")
  }
  $psi.Arguments = ($appArgs | ForEach-Object {
    if ($_ -match '\s') { "`"$_`"" } else { $_ }
  }) -join ' '
  $p = [System.Diagnostics.Process]::Start($psi)
  $peakWs = 0L
  if ($SampleWorkingSet) {
    while (-not $p.HasExited) {
      try { $p.Refresh(); $ws = $p.WorkingSet64; if ($ws -gt $peakWs) { $peakWs = $ws } } catch { }
      Start-Sleep -Milliseconds 50
    }
  }
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  if ($SampleWorkingSet) {
    try { $p.Refresh(); if ($p.WorkingSet64 -gt $peakWs) { $peakWs = $p.WorkingSet64 } } catch { }
  }
  $sw.Stop()
  if ($p.ExitCode -ne 0) {
    throw "structdb_app exit $($p.ExitCode): $err`n$out"
  }

  $tps = if ($sw.ElapsedMilliseconds -gt 0) { [double]$Rows / ($sw.ElapsedMilliseconds / 1000.0) } else { 0.0 }
  $repoRoot = Split-Path (Split-Path $here -Parent) -Parent
  $gitSha = $null
  try { $gitSha = (git -C $repoRoot rev-parse --short HEAD 2>$null) } catch { }
  $buildConfig = "Release"
  if ($App -match '\\Debug\\') { $buildConfig = "Debug" }
  elseif ($App -match '\\RelWithDebInfo\\') { $buildConfig = "RelWithDebInfo" }
  $summary = [ordered]@{
    timestamp                = [DateTime]::UtcNow.ToString("o")
    benchmark_profile        = @(
      "mega_data_mdb_bulk_v1"
      if ($ImportMode) { "script_import" }
      if ($EngineBulkImport) { "engine_bulk" }
      if ($Coalesce) { "coalesce" }
      if ($JournalSkipUntilCommit) { "journal_skip" }
      if ($MemtableBulkPut) { "memtable_bulk" }
      if ($PersistChunkMaxPuts -gt 0) { "chunk_puts_$PersistChunkMaxPuts" }
      if ($StorageEmbedBatchMaxFrameBytes -gt 0) { "embed_split_$StorageEmbedBatchMaxFrameBytes" }
    ) -join "+"
    import_mode              = [bool]$ImportMode
    engine_bulk_import       = [bool]$EngineBulkImport
    persist_coalesce         = [bool]$Coalesce
    embed_journal_skip       = [bool]$JournalSkipUntilCommit
    memtable_bulk_put        = [bool]$MemtableBulkPut
    persist_chunk_max_puts   = $PersistChunkMaxPuts
    storage_embed_batch_max_frame_bytes = $StorageEmbedBatchMaxFrameBytes
    runtime_pressure_tps_est = [Math]::Round($tps, 3)
    runtime_pressure_batch_ms_p95 = [Math]::Round([double]$sw.ElapsedMilliseconds, 3)
    total_rows               = $Rows
    rows_per_bulk_line       = $rpl
    wall_ms                  = $sw.ElapsedMilliseconds
    build_dir                = $BuildDir
    build_config             = $buildConfig
    git_sha                  = $gitSha
    peak_working_set_bytes   = if ($SampleWorkingSet) { $peakWs } else { $null }
  }
  $outJson = Join-Path $resultsDir ("mega_data_summary_{0}.json" -f $stamp)
  Write-Utf8NoBom $outJson (($summary | ConvertTo-Json -Depth 5))
  Write-Host "OK rows=$Rows wall_ms=$($sw.ElapsedMilliseconds) TPS~$([Math]::Round($tps,1)) -> $outJson"
}
finally {
  Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
}
