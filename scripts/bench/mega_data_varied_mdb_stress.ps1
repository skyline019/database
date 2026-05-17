#requires -version 5.1
<#
.SYNOPSIS
  宽表压测：每行唯一 tag/payload 字符串 + int 列（对比 mega_data 重复 int）。

.NOTES
  默认 IMPORT MODE + FLUSH + REBUILD INDEX（字符串侧车索引）。
  结果 JSON：scripts/results/mega_data_varied_summary_<UTC>.json
#>
param(
  [string]$BuildDir = "build",
  [long]$Rows = 0,
  [int]$RowsPerLine = 400,
  [switch]$EngineBulkImport,
  [switch]$SkipRebuildIndex
)

$ErrorActionPreference = "Stop"

function Find-StructdbApp {
  param([string]$Root)
  $candidates = @(
    (Join-Path $Root "src\app\Release\structdb_app.exe"),
    (Join-Path $Root "src\app\RelWithDebInfo\structdb_app.exe"),
    (Join-Path $Root "src\app\Debug\structdb_app.exe")
  )
  foreach ($p in $candidates) {
    if (Test-Path -LiteralPath $p) { return (Resolve-Path -LiteralPath $p).Path }
  }
  throw "structdb_app.exe not found under BuildDir=$Root"
}

function New-VariedBulkChunk {
  param([long]$StartId, [int]$Count)
  $sb = New-Object System.Text.StringBuilder
  for ($i = 0; $i -lt $Count; $i++) {
    $rid = $StartId + $i
    if ($i -gt 0) { [void]$sb.Append("|") }
    $tag = "t$rid"
    $payload = "pay-$rid-unique-payload"
    [void]$sb.Append("$rid,$rid,$rid,""$tag"",""$payload""")
  }
  return $sb.ToString()
}

function Write-Utf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$resultsDir = Join-Path (Split-Path -Parent $here) "results"
if (-not (Test-Path -LiteralPath $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }

if ($Rows -le 0) {
  $envN = [Environment]::GetEnvironmentVariable("STRUCTDB_MEGA_VARIED_ROWS")
  if ([string]::IsNullOrWhiteSpace($envN)) { $Rows = [long]50000 } else { $Rows = [long]$envN }
}
$rpl = [Math]::Max(20, $RowsPerLine)

$App = Find-StructdbApp $BuildDir
$stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss_fff")
$workRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("structdb_mega_varied_" + $stamp)
New-Item -ItemType Directory -Path $workRoot | Out-Null

try {
  $data = Join-Path $workRoot "_data"
  $sess = Join-Path $workRoot "embed_session"
  New-Item -ItemType Directory -Path $data -Force | Out-Null
  New-Item -ItemType Directory -Path $sess -Force | Out-Null

  $mdb = Join-Path $workRoot "varied_load.mdb"
  $lines = New-Object System.Collections.Generic.List[string]
  [void]$lines.Add("CREATE TABLE(varied)")
  [void]$lines.Add("USE(varied)")
  [void]$lines.Add("DEFATTR(id:int,a:int,tag:string,payload:string)")
  if (-not $EngineBulkImport) { [void]$lines.Add("IMPORT MODE ON") }
  $next = [long]1
  while ($next -le $Rows) {
    $n = [int][Math]::Min([long]$rpl, $Rows - $next + 1)
    [void]$lines.Add("BULKINSERTFAST($(New-VariedBulkChunk $next $n))")
    $next += $n
  }
  [void]$lines.Add("FLUSH PERSIST")
  if (-not $SkipRebuildIndex) { [void]$lines.Add("REBUILD INDEX") }
  [void]$lines.Add("COUNT")
  Write-Utf8NoBom $mdb ($lines -join "`n")

  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $appArgs = @("--data-dir", $data, "--session-dir", $sess, "--run-mdb", $mdb)
  if ($EngineBulkImport) { $appArgs += "--mdb-bulk-import" }
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  $psi.Arguments = ($appArgs | ForEach-Object { if ($_ -match '\s') { "`"$_`"" } else { $_ } }) -join ' '
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  $sw.Stop()
  if ($p.ExitCode -ne 0) { throw "load failed: $err`n$out" }

  $wallMs = $sw.ElapsedMilliseconds
  $tps = if ($wallMs -gt 0) { [double]$Rows / ($wallMs / 1000.0) } else { 0 }
  $summary = [ordered]@{
    timestamp         = [DateTime]::UtcNow.ToString("o")
    table             = "varied"
    rows              = $Rows
    rows_per_line     = $rpl
    engine_bulk_import = [bool]$EngineBulkImport
    rebuild_index     = -not $SkipRebuildIndex
    wall_ms           = $wallMs
    tps_est           = [Math]::Round($tps, 1)
    data_dir          = $data
  }
  $outJson = Join-Path $resultsDir ("mega_data_varied_summary_{0}.json" -f $stamp)
  Write-Utf8NoBom $outJson (($summary | ConvertTo-Json -Depth 4))
  Write-Host "=== mega_data_varied ($Rows rows) ===" -ForegroundColor Cyan
  Write-Host ("Load: {0} ms (~{1} TPS)" -f $wallMs, [Math]::Round($tps, 1))
  Write-Host "JSON -> $outJson"
  Write-Host "Compare query: mdb_query_bench.ps1 -VariedSchema -Table varied -DataDir $data -LoadRows $Rows -SkipLoad"
}
finally {
  if (Test-Path -LiteralPath $workRoot) {
    Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
  }
}
