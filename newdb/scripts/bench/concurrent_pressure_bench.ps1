param(
    [string]$BuildDir = "build_mingw",
    [int]$Jobs = 8,
    [int]$RepeatUntilFail = 30,
    [string]$RuntimeStatsJsonl = "",
    [switch]$AppendRuntimeJsonl = $false,
    [switch]$RunRuntimeGate = $false,
    [int]$RuntimePressureBatches = 12,
    [int]$RuntimePressureBatchSize = 40,
    [int]$RuntimeSampleEveryBatches = 1,
    [double]$MinVacuumEfficiency = -1.0,
    [double]$MaxConflictRate = -1.0,
    [double]$MinVacuumEfficiencyP50 = -1.0,
    [double]$MaxConflictRateP95 = -1.0,
    [double]$MaxTxnBeginLockConflictDelta = -1.0,
    [double]$MaxWalCompactDelta = -1.0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}

function Resolve-Binary {
    param(
        [string]$Name
    )
    $isWin = ($env:OS -eq "Windows_NT")
    $exe = if ($isWin) { "$Name.exe" } else { $Name }
    $candidate = Join-Path $BuildDir $exe
    if (Test-Path $candidate) { return $candidate }
    foreach ($cfg in @("RelWithDebInfo", "Release", "Debug", "MinSizeRel")) {
        $p = Join-Path (Join-Path $BuildDir $cfg) $exe
        if (Test-Path $p) { return $p }
    }
    throw "binary not found: $exe under $BuildDir"
}

function Invoke-RuntimePressureAndCapture {
    param(
        [string]$DemoExe,
        [string]$WorkspaceDir,
        [string]$OutJsonlPath,
        [int]$Batches,
        [int]$BatchSize,
        [int]$SampleEveryBatches
    )

    if ($Batches -le 0 -or $BatchSize -le 0 -or $SampleEveryBatches -le 0) {
        throw "runtime pressure batches/batch-size/sample-every must be > 0"
    }

    $runId = "pressure_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
    $scriptPath = Join-Path $WorkspaceDir "runtime_pressure.mdb"
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("CREATE TABLE(rtstats)")
    $lines.Add("USE(rtstats)")
    $lines.Add("AUTOVACUUM on 25")
    $lines.Add("AUTOVACUUM interval 0")
    $lines.Add("SHOW TUNING JSON")
    $startId = 1
    for ($i = 0; $i -lt $Batches; $i++) {
        $lines.Add("BEGIN")
        $lines.Add(("BULKINSERT({0},{1},ops)" -f $startId, $BatchSize))
        if (($i % 3) -eq 0) {
            $lines.Add(("DELETE({0})" -f $startId))
        }
        $lines.Add("COMMIT")
        if ((($i + 1) % $SampleEveryBatches) -eq 0) {
            $lines.Add("SHOW TUNING JSON")
        }
        $startId += $BatchSize
    }
    $lines.Add("SHOW TUNING JSON")
    $lines.Add("EXIT")
    Set-Content -Path $scriptPath -Value $lines -Encoding ascii

    & $DemoExe --data-dir $WorkspaceDir --run-mdb $scriptPath | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "runtime pressure mdb script failed: exit_code=$LASTEXITCODE"
    }

    $log = Join-Path $WorkspaceDir "demo_log.bin"
    if (-not (Test-Path $log)) {
        throw "runtime log missing: $log"
    }
    $matched = @(Select-String -Path $log -Pattern '^\{".*"write_conflicts":')
    if ($matched.Count -lt 3) {
        throw "need at least 3 SHOW TUNING JSON lines in $log (before + trend + after)"
    }
    $ts0 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    for ($i = 0; $i -lt $matched.Count; $i++) {
        $label = if ($i -eq 0) { "pressure_before" } elseif ($i -eq ($matched.Count - 1)) { "pressure_after" } else { "pressure_sample_$i" }
        $ts = $ts0 + $i
        $json = "{""schema_version"":""newdb.runtime_stats.v1"",""ts_ms"":$ts,""run_id"":""$runId"",""label"":""$label"",""stats"":" + $matched[$i].Line + "}"
        Add-Content -Path $OutJsonlPath -Value $json
    }

    # Script-level regression guard: ensure this run has expected labels.
    $beforeCnt = @(Select-String -Path $OutJsonlPath -Pattern ('"run_id":"' + $runId + '".*"label":"pressure_before"')).Count
    $afterCnt = @(Select-String -Path $OutJsonlPath -Pattern ('"run_id":"' + $runId + '".*"label":"pressure_after"')).Count
    $sampleCnt = @(Select-String -Path $OutJsonlPath -Pattern ('"run_id":"' + $runId + '".*"label":"pressure_sample_')).Count
    if ($beforeCnt -lt 1 -or $afterCnt -lt 1 -or $sampleCnt -lt 1) {
        throw "runtime snapshot jsonl missing pressure_before/pressure_after labels"
    }
    return $runId
}

Write-Host "Concurrent pressure start: repeat=$RepeatUntilFail jobs=$Jobs"

$runtimeJsonlPath = $RuntimeStatsJsonl
if ([string]::IsNullOrWhiteSpace($runtimeJsonlPath)) {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $runtimeJsonlPath = Join-Path $PSScriptRoot ("results/runtime_stats_concurrent_pressure_" + $stamp + ".jsonl")
}
if (-not [System.IO.Path]::IsPathRooted($runtimeJsonlPath)) {
    $runtimeJsonlPath = Join-Path $projectRoot $runtimeJsonlPath
}
$runtimeDir = Split-Path -Parent $runtimeJsonlPath
if (-not (Test-Path $runtimeDir)) {
    New-Item -Path $runtimeDir -ItemType Directory | Out-Null
}
if ((-not $AppendRuntimeJsonl) -and (Test-Path $runtimeJsonlPath)) {
    Remove-Item -Path $runtimeJsonlPath -Force
}

$demoExe = Resolve-Binary -Name "newdb_demo"
$pressureRunId = ""
$runtimeGateSummary = $null
$runtimeWs = Join-Path $env:TEMP ("newdb_runtime_capture_" + [Guid]::NewGuid().ToString("N"))
New-Item -Path $runtimeWs -ItemType Directory | Out-Null
try {
    $pressureRunId = Invoke-RuntimePressureAndCapture `
        -DemoExe $demoExe `
        -WorkspaceDir $runtimeWs `
        -OutJsonlPath $runtimeJsonlPath `
        -Batches $RuntimePressureBatches `
        -BatchSize $RuntimePressureBatchSize `
        -SampleEveryBatches $RuntimeSampleEveryBatches

    $cmd = @(
        "--test-dir", $BuildDir,
        "--output-on-failure",
        "-j", "$Jobs",
        "-R", "WalConcurrency|WhereConcurrency|WalSegment",
        "--repeat", "until-fail:$RepeatUntilFail"
    )

    ctest @cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Concurrent pressure failed: exit_code=$LASTEXITCODE"
    }

} finally {
    Remove-Item -Path $runtimeWs -Recurse -Force -ErrorAction SilentlyContinue
}

if ($RunRuntimeGate) {
    $reportExe = Resolve-Binary -Name "newdb_runtime_report"
    $reportCmd = @("--input", $runtimeJsonlPath, "--run-id", $pressureRunId)
    if ($MinVacuumEfficiency -ge 0.0) {
        $reportCmd += @("--min-vacuum-efficiency", "$MinVacuumEfficiency")
    }
    if ($MaxConflictRate -ge 0.0) {
        $reportCmd += @("--max-conflict-rate", "$MaxConflictRate")
    }
    if ($MinVacuumEfficiencyP50 -ge 0.0) {
        $reportCmd += @("--min-vacuum-efficiency-p50", "$MinVacuumEfficiencyP50")
    }
    if ($MaxConflictRateP95 -ge 0.0) {
        $reportCmd += @("--max-conflict-rate-p95", "$MaxConflictRateP95")
    }
    if ($MaxTxnBeginLockConflictDelta -ge 0.0) {
        $reportCmd += @("--max-txn-begin-lock-conflict-delta", "$MaxTxnBeginLockConflictDelta")
    }
    if ($MaxWalCompactDelta -ge 0.0) {
        $reportCmd += @("--max-wal-compact-delta", "$MaxWalCompactDelta")
    }
    $reportOut = (& $reportExe @reportCmd | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "runtime gate failed: exit_code=$LASTEXITCODE"
    }
    if (-not [string]::IsNullOrWhiteSpace($reportOut)) {
        try {
            $runtimeGateSummary = $reportOut | ConvertFrom-Json
        } catch {
            $runtimeGateSummary = [ordered]@{ raw = $reportOut }
        }
    }
}

 $resultDir = Join-Path $PSScriptRoot "results"
 if (-not (Test-Path $resultDir)) {
     New-Item -Path $resultDir -ItemType Directory | Out-Null
 }
 $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
 $summaryPath = Join-Path $resultDir ("concurrent_pressure_summary_" + $stamp + ".json")
 [ordered]@{
     timestamp = (Get-Date).ToString("o")
     build_dir = $BuildDir
     jobs = $Jobs
     repeat_until_fail = $RepeatUntilFail
     runtime_stats_jsonl = $runtimeJsonlPath
     runtime_jsonl_append_mode = [bool]$AppendRuntimeJsonl
     runtime_gate_enabled = [bool]$RunRuntimeGate
     runtime_pressure_batches = $RuntimePressureBatches
     runtime_pressure_batch_size = $RuntimePressureBatchSize
     runtime_sample_every_batches = $RuntimeSampleEveryBatches
     runtime_run_id = $pressureRunId
     min_vacuum_efficiency = $MinVacuumEfficiency
     max_conflict_rate = $MaxConflictRate
     min_vacuum_efficiency_p50 = $MinVacuumEfficiencyP50
     max_conflict_rate_p95 = $MaxConflictRateP95
     max_txn_begin_lock_conflict_delta = $MaxTxnBeginLockConflictDelta
     max_wal_compact_delta = $MaxWalCompactDelta
     runtime_gate_summary = $runtimeGateSummary
     status = "passed"
 } | ConvertTo-Json -Depth 5 | Set-Content -Path $summaryPath
 Write-Host ("Concurrent pressure summary: {0}" -f $summaryPath)

Write-Host "Concurrent pressure done."
