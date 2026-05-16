#requires -version 5.1
<#
.SYNOPSIS
  StructDB 高压：多进程并行 MDB 批量插入，或单进程多批 BULKINSERTFAST 延迟采样。

.DESCRIPTION
  - Jobs>1：每个 Job 独立 `_data`，并行 `structdb_app --run-mdb`；TPS ≈ 总行数 /（最慢 Job 秒）；p95 为各 Job 墙钟耗时的 95 分位。
  - Jobs==1：单库连续多批插入，p95 为批耗时 95 分位，TPS ≈ 总行数 / 批耗时之和秒。
  - 结果：`scripts/results/concurrent_pressure_summary_<UTC>.json`
  - `-RepeatUntilFail`：失败时最多尝试次数（含首次）；大于 1 时在异常后换临时目录重跑直至成功或耗尽次数。
#>
param(
  [Parameter(Mandatory = $false)][string]$BuildDir = "build",
  [Parameter(Mandatory = $false)][int]$Jobs = 4,
  [Parameter(Mandatory = $false)][int]$RuntimePressureBatches = 32,
  [Parameter(Mandatory = $false)][int]$RuntimePressureBatchSize = 400,
  [Parameter(Mandatory = $false)][int]$RuntimeSampleEveryBatches = 4,
  [Parameter(Mandatory = $false)][int]$RuntimeProgressEveryBatches = 1,
  [Parameter(Mandatory = $false)][int]$RuntimeProgressEveryRows = 100,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmSegmentTargetBytes = 256,
  [Parameter(Mandatory = $false)][int]$RuntimeSidecarInvalidateEveryN = 16,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmCompactionWorkers = 2,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmCompactionReapBudget = 4,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmL0CompactTrigger = 8,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmL0CompactBatch = 12,
  [Parameter(Mandatory = $false)][int]$RuntimeLsmFlushTriggerMultiplier = 2,
  [Parameter(Mandatory = $false)][int]$RepeatUntilFail = 1,
  [Parameter(Mandatory = $false)][switch]$RuntimeSidecarInvalidateAsync,
  [Parameter(Mandatory = $false)][switch]$RuntimeQuietSessionLog,
  [Parameter(Mandatory = $false)][switch]$RuntimeUseBulkInsertFast,
  [Parameter(Mandatory = $false)][switch]$RuntimeLsmCompactionAsync,
  [Parameter(Mandatory = $false)][switch]$RuntimeEchoProgress
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
  throw "structdb_app.exe not found under BuildDir=$Root (tried Release/RelWithDebInfo/Debug)."
}

function New-BulkChunk {
  param([int]$StartId, [int]$Count)
  $sb = New-Object System.Text.StringBuilder
  for ($i = 0; $i -lt $Count; $i++) {
    $rid = $StartId + $i
    if ($i -gt 0) { [void]$sb.Append("|") }
    [void]$sb.Append("$rid,$rid,0,0")
  }
  return $sb.ToString()
}

function Write-MdbUtf8NoBom([string]$Path, [string]$Content) {
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

function Set-StructdbRunMdbArguments {
  param(
    [System.Diagnostics.ProcessStartInfo]$Psi,
    [string]$DataDir,
    [string]$SessionDir,
    [string]$MdbPath
  )
  # 使用 Arguments 字符串以兼容 Windows PowerShell 5.1（无 ArgumentList）。
  $Psi.Arguments = "--data-dir `"$DataDir`" --session-dir `"$SessionDir`" --run-mdb `"$MdbPath`""
}

function Invoke-StructdbMdb {
  param(
    [string]$App,
    [string]$DataDir,
    [string]$SessionDir,
    [string]$MdbPath
  )
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $App
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true
  Set-StructdbRunMdbArguments $psi $DataDir $SessionDir $MdbPath
  $p = [System.Diagnostics.Process]::Start($psi)
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  if ($p.ExitCode -ne 0) {
    throw "structdb_app exit $($p.ExitCode): $err`n$out"
  }
  return $out
}

function Percentile-Double {
  param([double[]]$Values, [double]$P)
  if ($null -eq $Values -or $Values.Count -eq 0) { return 0.0 }
  $s = $Values | Sort-Object
  $idx = [math]::Ceiling(($P / 100.0) * $s.Count) - 1
  if ($idx -lt 0) { $idx = 0 }
  if ($idx -ge $s.Count) { $idx = $s.Count - 1 }
  return [double]$s[$idx]
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$resultsDir = Join-Path (Split-Path -Parent $here) "results"
if (-not (Test-Path -LiteralPath $resultsDir)) {
  New-Item -ItemType Directory -Path $resultsDir | Out-Null
}

$App = Find-StructdbApp $BuildDir
$maxAttempts = [Math]::Max(1, $RepeatUntilFail)
$tempBase = [System.IO.Path]::GetTempPath()

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
  $stamp = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss")
  $workRoot = Join-Path $tempBase ("structdb_pressure_{0}_try{1}" -f $stamp, $attempt)
  New-Item -ItemType Directory -Path $workRoot | Out-Null

  try {
  $jobs = [Math]::Max(1, $Jobs)
  $batches = [Math]::Max(1, $RuntimePressureBatches)
  $bsize = [Math]::Max(1, $RuntimePressureBatchSize)

  $totalRows = 0L
  $p95 = 0.0
  $tps = 0.0
  $mode = ""

  if ($jobs -le 1) {
    $mode = "serial_batches"
    $dataS = Join-Path $workRoot "data_serial"
    $sessS = Join-Path $workRoot "sess_serial"
    New-Item -ItemType Directory -Path $dataS -Force | Out-Null
    New-Item -ItemType Directory -Path $sessS -Force | Out-Null
    $setup = Join-Path $workRoot "serial_setup.mdb"
    Write-MdbUtf8NoBom $setup (@(
      "CREATE TABLE(ts)"
      "USE(ts)"
      "DEFATTR(id:int,a:int,b:int)"
    ) -join "`n")
    Invoke-StructdbMdb $App $dataS $sessS $setup

    $batchMs = New-Object System.Collections.Generic.List[double]
    $nextId = 1
    $swAll = [System.Diagnostics.Stopwatch]::StartNew()
    for ($b = 1; $b -le $batches; $b++) {
      $bat = Join-Path $workRoot ("serial_batch_{0}.mdb" -f $b)
      $chunk = New-BulkChunk $nextId $bsize
      Write-MdbUtf8NoBom $bat (@("USE(ts)", "BULKINSERTFAST($chunk)") -join "`n")
      $swb = [System.Diagnostics.Stopwatch]::StartNew()
      $null = Invoke-StructdbMdb $App $dataS $sessS $bat
      $swb.Stop()
      $batchMs.Add([double]$swb.ElapsedMilliseconds)
      $nextId += $bsize
      if ($RuntimeEchoProgress -and (($b % [Math]::Max(1, $RuntimeProgressEveryBatches)) -eq 0)) {
        Write-Host "[serial] batch $b / $batches last_ms=$([int]$swb.ElapsedMilliseconds)"
      }
    }
    $swAll.Stop()
    $totalRows = [long]$batches * [long]$bsize
    $sumMs = ($batchMs | Measure-Object -Sum).Sum
    $p95 = Percentile-Double ($batchMs.ToArray()) 95.0
    if ($sumMs -gt 0) { $tps = [double]$totalRows / ($sumMs / 1000.0) }
  }
  else {
    $mode = "parallel_jobs"
    $rowsPerJob = [long][math]::Max(1L, [long][math]::Floor([double]([long]$batches * [long]$bsize) / [double]$jobs))
    $workerJobs = @()
    for ($j = 1; $j -le $jobs; $j++) {
      $workerJobs += Start-Job -ScriptBlock {
        param([int]$JobId, [long]$RowsPerJob, [int]$ChunkSize, [bool]$EchoBool)
        $AppPath = [string]$($using:App)
        $Root = [string]$($using:workRoot)
        $ErrorActionPreference = "Stop"
        if ([string]::IsNullOrWhiteSpace($AppPath) -or [string]::IsNullOrWhiteSpace($Root)) {
          throw "job ${JobId}: AppPath or Root missing (AppPath='$AppPath' Root='$Root')"
        }
        function New-ChunkLocal([int]$StartId, [int]$Count) {
          $sb = New-Object System.Text.StringBuilder
          for ($i = 0; $i -lt $Count; $i++) {
            $rid = $StartId + $i
            if ($i -gt 0) { [void]$sb.Append("|") }
            [void]$sb.Append("$rid,$rid,0,0")
          }
          return $sb.ToString()
        }
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $data = Join-Path $Root "data_j$JobId"
        $sess = Join-Path $Root "sess_j$JobId"
        New-Item -ItemType Directory -Path $data -Force | Out-Null
        New-Item -ItemType Directory -Path $sess -Force | Out-Null
        $mdb = Join-Path $Root "load_j$JobId.mdb"
        $tname = "tp$JobId"
        $lines = New-Object System.Collections.Generic.List[string]
        [void]$lines.Add("CREATE TABLE($tname)")
        [void]$lines.Add("USE($tname)")
        [void]$lines.Add("DEFATTR(id:int,a:int,b:int)")
        $base = [long]$JobId * [long]100000000
        $written = 0L
        while ($written -lt $RowsPerJob) {
          $n = [Math]::Min($ChunkSize, [int]($RowsPerJob - $written))
          $bulkPayload = New-ChunkLocal ([int]($base + $written)) $n
          [void]$lines.Add("BULKINSERTFAST($bulkPayload)")
          $written += $n
          if ($EchoBool -and (($written % ([Math]::Max($ChunkSize, 1) * 20)) -eq 0)) { Write-Host "[job $JobId] rows=$written" }
        }
        [void]$lines.Add("COUNT")
        $encNoBom = New-Object System.Text.UTF8Encoding $false
        [System.IO.File]::WriteAllText($mdb, ($lines -join "`n"), $encNoBom)
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $AppPath
        $psi.UseShellExecute = $false
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.CreateNoWindow = $true
        $psi.Arguments = "--data-dir `"$data`" --session-dir `"$sess`" --run-mdb `"$mdb`""
        $p = [System.Diagnostics.Process]::Start($psi)
        $out = $p.StandardOutput.ReadToEnd()
        $err = $p.StandardError.ReadToEnd()
        $p.WaitForExit()
        $sw.Stop()
        if ($p.ExitCode -ne 0) { throw "job ${JobId} exit $($p.ExitCode): $err`n$out" }
        return @{ job = $JobId; ms = $sw.ElapsedMilliseconds; rows = $RowsPerJob }
      } -ArgumentList $j, $rowsPerJob, $bsize, ([bool]$RuntimeEchoProgress)
    }
    $null = $workerJobs | Wait-Job
    $parallelResults = @($workerJobs | Receive-Job)
    $workerJobs | Remove-Job
    $jobMs = foreach ($r in $parallelResults) { [double]$r.ms }
    $p95 = Percentile-Double $jobMs 95.0
    $maxMs = ($jobMs | Measure-Object -Maximum).Maximum
    $totalRows = $rowsPerJob * [long]$jobs
    if ($maxMs -gt 0) { $tps = [double]$totalRows / ($maxMs / 1000.0) }
  }

  $summary = [ordered]@{
    timestamp                     = [DateTime]::UtcNow.ToString("o")
    benchmark_profile             = "concurrent_mdb_bulk_v2"
    runtime_walsync_mode          = "process_default"
    runtime_pressure_tps_est      = [Math]::Round($tps, 3)
    runtime_pressure_batch_ms_p95 = [Math]::Round($p95, 3)
    runtime_pressure_batches      = $batches
    runtime_pressure_batch_size   = $bsize
    jobs                          = $jobs
    mode                          = $mode
    total_rows                    = $totalRows
    build_dir                     = $BuildDir
  }
  $outJson = Join-Path $resultsDir ("concurrent_pressure_summary_{0}.json" -f $stamp)
  ($summary | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $outJson -Encoding utf8

  Write-Host "OK: wrote $outJson"
  Write-Host "mode=$mode TPS_est=$([Math]::Round($tps,2)) p95_ms=$([Math]::Round($p95,2)) total_rows=$totalRows"
  break
  }
  catch {
    Write-Warning ("concurrent_pressure_bench attempt {0}/{1} failed: {2}" -f $attempt, $maxAttempts, $_.Exception.Message)
    if ($attempt -eq $maxAttempts) { throw }
  }
  finally {
    if (Test-Path -LiteralPath $workRoot) {
      Remove-Item -LiteralPath $workRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
  }
}
