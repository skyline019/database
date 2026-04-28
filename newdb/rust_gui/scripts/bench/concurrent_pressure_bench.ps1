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
    [int]$RuntimeProgressEveryBatches = 1,
    [int]$RuntimeProgressEveryRows = 64,
    [switch]$RuntimeEchoProgress = $true,
    [int]$RuntimeLsmSegmentTargetBytes = 128,
    [int]$RuntimeSidecarInvalidateEveryN = 1,
    [switch]$RuntimeSidecarInvalidateVerbose = $false,
    [switch]$RuntimeSidecarInvalidateAsync = $false,
    [switch]$RuntimeQuietSessionLog = $true,
    [switch]$RuntimeUseBulkInsertFast = $true,
    [switch]$RuntimeLsmCompactionAsync = $true,
    [int]$RuntimeLsmCompactionWorkers = 1,
    [int]$RuntimeLsmCompactionReapBudget = 2,
    [int]$RuntimeLsmL0CompactTrigger = 8,
    [int]$RuntimeLsmL0CompactBatch = 8,
    [int]$RuntimeLsmFlushTriggerMultiplier = 2,
    [double]$MinVacuumEfficiency = -1.0,
    [double]$MaxConflictRate = -1.0,
    [double]$MinVacuumEfficiencyP50 = -1.0,
    [double]$MaxConflictRateP95 = -1.0,
    [double]$MaxTxnBeginLockConflictDelta = -1.0,
    [double]$MaxWalCompactDelta = -1.0,
    [double]$MaxVacuumCompactFailureDelta = -1.0,
    [double]$MinVacuumCompactReclaimedBytesDelta = -1.0,
    [double]$MaxVacuumQueueDepthPeak = -1.0,
    [double]$MaxWalRecoveryLastElapsedMs = -1.0,
    [double]$MaxLockDeadlockDetectDelta = -1.0,
    [double]$MaxLockDeadlockVictimDelta = -1.0,
    [double]$MaxLockWaitMaxMsDelta = -1.0,
    [double]$MaxSchedulerThrottleDelta = -1.0,
    [double]$MinWalGroupCommitBatchCommitsDelta = -1.0,
    [double]$MaxLsmSegmentCount = -1.0,
    [double]$MinLsmMemtableFlushDelta = -1.0,
    [double]$MaxLsmReadSegmentsScannedP95 = -1.0,
    [double]$MinLsmCompactionBytesAmpEfficiency = -1.0,
    [ValidateSet("newdb-default", "leveldb-like", "innodb-like", "hybrid-balanced")]
    [string]$BenchmarkProfile = "newdb-default",
    [ValidateSet("off", "normal", "full")]
    [string]$RuntimeWalSyncMode = "normal",
    [int]$RuntimeWalSyncNormalIntervalMs = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $cwdCandidate = Join-Path (Get-Location) $BuildDir
    if (Test-Path -LiteralPath $cwdCandidate) {
        $BuildDir = (Resolve-Path -LiteralPath $cwdCandidate).Path
    } else {
        $BuildDir = Join-Path $projectRoot $BuildDir
    }
}

function Apply-ProfileDefaultThresholds {
    param(
        [string]$Profile
    )
    $helper = Join-Path $projectRoot "scripts/ci/profile_thresholds.ps1"
    if (Test-Path -LiteralPath $helper) {
        . $helper
        Apply-NewdbPressureProfileThresholdsIfUnset `
            -Profile $Profile `
            -ProjectRoot $projectRoot `
            -MaxVacuumCompactFailureDelta ([ref]$script:MaxVacuumCompactFailureDelta) `
            -MaxVacuumQueueDepthPeak ([ref]$script:MaxVacuumQueueDepthPeak) `
            -MaxWalRecoveryLastElapsedMs ([ref]$script:MaxWalRecoveryLastElapsedMs) `
            -MaxLockDeadlockDetectDelta ([ref]$script:MaxLockDeadlockDetectDelta) `
            -MaxLockDeadlockVictimDelta ([ref]$script:MaxLockDeadlockVictimDelta) `
            -MaxLockWaitMaxMsDelta ([ref]$script:MaxLockWaitMaxMsDelta) `
            -MaxSchedulerThrottleDelta ([ref]$script:MaxSchedulerThrottleDelta) `
            -MinWalGroupCommitBatchCommitsDelta ([ref]$script:MinWalGroupCommitBatchCommitsDelta) `
            -MaxLsmSegmentCount ([ref]$script:MaxLsmSegmentCount) `
            -MinLsmMemtableFlushDelta ([ref]$script:MinLsmMemtableFlushDelta) `
            -MaxLsmReadSegmentsScannedP95 ([ref]$script:MaxLsmReadSegmentsScannedP95) `
            -MinLsmCompactionBytesAmpEfficiency ([ref]$script:MinLsmCompactionBytesAmpEfficiency)
        return
    }
    throw "missing threshold helper script: $helper"
}

Apply-ProfileDefaultThresholds -Profile $BenchmarkProfile

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
        [int]$SampleEveryBatches,
        [int]$RuntimeProgressEveryBatches,
        [int]$RuntimeProgressEveryRows,
        [int]$RuntimeLsmSegmentTargetBytes,
        [int]$RuntimeSidecarInvalidateEveryN,
        [switch]$RuntimeSidecarInvalidateVerbose,
        [switch]$RuntimeSidecarInvalidateAsync,
        [switch]$RuntimeQuietSessionLog,
        [switch]$RuntimeUseBulkInsertFast,
        [switch]$RuntimeLsmCompactionAsync,
        [int]$RuntimeLsmCompactionWorkers,
        [int]$RuntimeLsmCompactionReapBudget,
        [int]$RuntimeLsmL0CompactTrigger,
        [int]$RuntimeLsmL0CompactBatch,
        [int]$RuntimeLsmFlushTriggerMultiplier,
        [string]$RuntimeWalSyncMode,
        [int]$RuntimeWalSyncNormalIntervalMs
    )

    if ($Batches -le 0 -or $BatchSize -le 0 -or $SampleEveryBatches -le 0) {
        throw "runtime pressure batches/batch-size/sample-every must be > 0"
    }
    if ($RuntimeLsmSegmentTargetBytes -le 0) {
        throw "runtime lsm segment target bytes must be > 0"
    }
    if ($RuntimeProgressEveryBatches -le 0) {
        throw "runtime progress every batches must be > 0"
    }
    if ($RuntimeProgressEveryRows -le 0) {
        throw "runtime progress every rows must be > 0"
    }
    if ($RuntimeSidecarInvalidateEveryN -le 0) {
        throw "runtime sidecar invalidate every N must be > 0"
    }
    if ($RuntimeLsmL0CompactTrigger -le 1) {
        throw "runtime lsm L0 compact trigger must be > 1"
    }
    if ($RuntimeLsmL0CompactBatch -le 1) {
        throw "runtime lsm L0 compact batch must be > 1"
    }
    if ($RuntimeLsmFlushTriggerMultiplier -le 0) {
        throw "runtime lsm flush trigger multiplier must be > 0"
    }
    if ($RuntimeLsmCompactionWorkers -le 0) {
        throw "runtime lsm compaction workers must be > 0"
    }
    if ($RuntimeLsmCompactionReapBudget -le 0) {
        throw "runtime lsm compaction reap budget must be > 0"
    }
    if ($RuntimeWalSyncNormalIntervalMs -lt 0) {
        throw "runtime WALSYNC normal interval must be >= 0"
    }

    $runId = "pressure_" + (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
    $scriptPath = Join-Path $WorkspaceDir "runtime_pressure_all.mdb"
    $log = Join-Path $WorkspaceDir "runtime_pressure.log"
    $pressureSw = [System.Diagnostics.Stopwatch]::StartNew()

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("CREATE TABLE(rtstats)")
    $lines.Add("USE(rtstats)")
    $lines.Add("HOTINDEX on")
    if ($RuntimeWalSyncMode -eq "normal") {
        $lines.Add(("WALSYNC normal {0}" -f $RuntimeWalSyncNormalIntervalMs))
    } else {
        $lines.Add(("WALSYNC {0}" -f $RuntimeWalSyncMode))
    }
    $lines.Add(("SEGMENT {0}" -f $RuntimeLsmSegmentTargetBytes))
    $lines.Add("AUTOVACUUM on 25")
    $lines.Add("AUTOVACUUM interval 0")
    $lines.Add("SHOW TUNING JSON")
    # Emit a baseline marker so we can derive per-batch latency from t_ms deltas.
    $lines.Add(("PROGRESS batch=0/{0}" -f $Batches))

    $startId = 1
    for ($i = 0; $i -lt $Batches; $i++) {
        if ($RuntimeUseBulkInsertFast) {
            $lines.Add(("BULKINSERTFAST({0},{1},ops)" -f $startId, $BatchSize))
            # Keep parser-compatible inner marker even in bulk mode.
            $lines.Add(("PROGRESS_INNER batch={0}/{1} row={2}/{3}" -f ($i + 1), $Batches, $BatchSize, $BatchSize))
        } else {
            for ($j = 0; $j -lt $BatchSize; $j++) {
                $id = $startId + $j
                $lines.Add(("INSERT({0},ops,1)" -f $id))
                $rowDone = $j + 1
                if ((($rowDone % $RuntimeProgressEveryRows) -eq 0) -or ($rowDone -eq $BatchSize)) {
                    $lines.Add(("PROGRESS_INNER batch={0}/{1} row={2}/{3}" -f ($i + 1), $Batches, $rowDone, $BatchSize))
                }
            }
        }
        if (($i % 3) -eq 0) {
            $lines.Add(("DELETE({0})" -f $startId))
        }
        # Ensure read path touches segment filtering + lookup stats.
        $probeId = $startId + [Math]::Floor($BatchSize / 2)
        $lines.Add(("FIND({0})" -f $probeId))
        if (($i % 2) -eq 0) {
            $lines.Add(("FIND({0})" -f $startId))
        }
        if ((($i + 1) % $SampleEveryBatches) -eq 0) {
            # Trigger extra rotation opportunities to make compaction observable in small runs.
            $lines.Add(("INSERT({0},ops,1)" -f (900000 + $i)))
            $lines.Add(("DELETE({0})" -f (900000 + $i)))
            $lines.Add("SHOW TUNING JSON")
        }
        # Batch-level progress marker (always emit for latency accounting).
        $lines.Add(("PROGRESS batch={0}/{1}" -f ($i + 1), $Batches))
        $startId += $BatchSize
    }

    # Ensure we always end with a final snapshot from the same process that did work.
    if ((($Batches % $SampleEveryBatches) -ne 0)) {
        $lines.Add("SHOW TUNING JSON")
    }
    $lines.Add("EXIT")

    Set-Content -Path $scriptPath -Value $lines -Encoding ascii
    # Start demo via .NET Process so we can read ExitCode reliably,
    # and redirect stdout/stderr so console progress bars won't be garbled.
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $DemoExe
    $psi.Arguments = ('--data-dir "{0}" --log-file "{1}" --run-mdb "{2}"' -f $WorkspaceDir, $log, $scriptPath)
    # Do not redirect demo output here; callers that need a clean console
    # should run this script via Start-Process with redirected stdout/stderr.
    $psi.UseShellExecute = $true
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    $oldSidecarInvalidateEnv = [Environment]::GetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_EVERY_N", "Process")
    $oldSidecarInvalidateVerboseEnv = [Environment]::GetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_VERBOSE", "Process")
    $oldSidecarInvalidateAsyncEnv = [Environment]::GetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_ASYNC", "Process")
    $oldBenchQuietLogEnv = [Environment]::GetEnvironmentVariable("NEWDB_BENCH_QUIET_LOG", "Process")
    $oldLsmCompactionAsyncEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_COMPACTION_ASYNC", "Process")
    $oldLsmL0CompactTriggerEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_TRIGGER", "Process")
    $oldLsmL0CompactBatchEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_BATCH", "Process")
    $oldLsmFlushTriggerMultiplierEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_FLUSH_TRIGGER_MULTIPLIER", "Process")
    $oldLsmCompactionWorkersEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_COMPACTION_WORKERS", "Process")
    $oldLsmCompactionReapBudgetEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_COMPACTION_REAP_BUDGET", "Process")
    $oldBenchmarkProfileEnv = [Environment]::GetEnvironmentVariable("NEWDB_BENCHMARK_PROFILE", "Process")
    $oldCompactionPolicyEnv = [Environment]::GetEnvironmentVariable("NEWDB_LSM_COMPACTION_POLICY", "Process")
    [Environment]::SetEnvironmentVariable(
        "NEWDB_SIDECAR_INVALIDATE_EVERY_N",
        [string]$RuntimeSidecarInvalidateEveryN,
        "Process"
    )
    $sidecarVerbose = if ($RuntimeSidecarInvalidateVerbose) { "1" } else { "0" }
    [Environment]::SetEnvironmentVariable(
        "NEWDB_SIDECAR_INVALIDATE_VERBOSE",
        $sidecarVerbose,
        "Process"
    )
    $sidecarAsync = if ($RuntimeSidecarInvalidateAsync) { "1" } else { "0" }
    [Environment]::SetEnvironmentVariable(
        "NEWDB_SIDECAR_INVALIDATE_ASYNC",
        $sidecarAsync,
        "Process"
    )
    $benchQuietLog = if ($RuntimeQuietSessionLog) { "1" } else { "0" }
    [Environment]::SetEnvironmentVariable(
        "NEWDB_BENCH_QUIET_LOG",
        $benchQuietLog,
        "Process"
    )
    $lsmCompactionAsync = if ($RuntimeLsmCompactionAsync) { "1" } else { "0" }
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_ASYNC", $lsmCompactionAsync, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_TRIGGER", [string]$RuntimeLsmL0CompactTrigger, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_BATCH", [string]$RuntimeLsmL0CompactBatch, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_FLUSH_TRIGGER_MULTIPLIER", [string]$RuntimeLsmFlushTriggerMultiplier, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_WORKERS", [string]$RuntimeLsmCompactionWorkers, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_REAP_BUDGET", [string]$RuntimeLsmCompactionReapBudget, "Process")
    [Environment]::SetEnvironmentVariable("NEWDB_BENCHMARK_PROFILE", [string]$BenchmarkProfile, "Process")
    $compactionPolicy = if ($BenchmarkProfile -eq "leveldb-like") { "size_tiered" } else { "leveled_lite" }
    [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_POLICY", $compactionPolicy, "Process")
    $null = $p.Start()

    # Tail the demo log in-process and echo PROGRESS markers to stdout so callers
    # can render a true progress bar without knowing internal workspace paths.
    $tailBuf = New-Object System.Collections.Generic.List[string]
    $lastLen = 0
    try {
        while (-not $p.HasExited) {
            if ($RuntimeEchoProgress -and (Test-Path $log)) {
                try {
                    $txt = Get-Content -Path $log -Raw -ErrorAction Stop
                    if ($txt.Length -gt $lastLen) {
                        $new = $txt.Substring($lastLen)
                        $lastLen = $txt.Length
                        foreach ($ln in ($new -split "`r?`n")) {
                            if ($ln -match '^PROGRESS(_INNER)? ') {
                                Write-Host $ln
                            }
                        }
                    }
                } catch {
                    # Best-effort; ignore transient read errors during concurrent writes.
                }
            }
            Start-Sleep -Milliseconds 200
        }
        try { $p.WaitForExit() } catch { }
    } finally {
        [Environment]::SetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_EVERY_N", $oldSidecarInvalidateEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_VERBOSE", $oldSidecarInvalidateVerboseEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_SIDECAR_INVALIDATE_ASYNC", $oldSidecarInvalidateAsyncEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_BENCH_QUIET_LOG", $oldBenchQuietLogEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_ASYNC", $oldLsmCompactionAsyncEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_TRIGGER", $oldLsmL0CompactTriggerEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_L0_COMPACT_BATCH", $oldLsmL0CompactBatchEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_FLUSH_TRIGGER_MULTIPLIER", $oldLsmFlushTriggerMultiplierEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_WORKERS", $oldLsmCompactionWorkersEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_REAP_BUDGET", $oldLsmCompactionReapBudgetEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_BENCHMARK_PROFILE", $oldBenchmarkProfileEnv, "Process")
        [Environment]::SetEnvironmentVariable("NEWDB_LSM_COMPACTION_POLICY", $oldCompactionPolicyEnv, "Process")
    }
    $exitCode = $p.ExitCode
    if ($exitCode -ne 0) {
        throw "runtime pressure mdb failed: exit_code=$exitCode"
    }

    $pressureSw.Stop()

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

    # Derive per-batch latency distribution from PROGRESS markers emitted by the MDB.
    # We insert a baseline PROGRESS batch=0/N marker before work starts.
    $prog = @(Select-String -Path $log -Pattern '^PROGRESS batch=')
    $derivedBatchMs = New-Object System.Collections.Generic.List[double]
    if ($prog.Count -ge 2) {
        $ts = @()
        foreach ($m in $prog) {
            if ($m.Line -match 't_ms=(\d+)') {
                $ts += [double]$Matches[1]
            }
        }
        for ($i = 1; $i -lt $ts.Count; $i++) {
            $delta = $ts[$i] - $ts[$i - 1]
            if ($delta -ge 0) { $derivedBatchMs.Add($delta) }
        }
    }
    return [pscustomobject]@{
        run_id = $runId
        pressure_elapsed_ms = [math]::Round($pressureSw.Elapsed.TotalMilliseconds, 3)
        batch_ms = @($derivedBatchMs)
    }
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
    $pressure = Invoke-RuntimePressureAndCapture `
        -DemoExe $demoExe `
        -WorkspaceDir $runtimeWs `
        -OutJsonlPath $runtimeJsonlPath `
        -Batches $RuntimePressureBatches `
        -BatchSize $RuntimePressureBatchSize `
        -SampleEveryBatches $RuntimeSampleEveryBatches `
        -RuntimeProgressEveryBatches $RuntimeProgressEveryBatches `
        -RuntimeProgressEveryRows $RuntimeProgressEveryRows `
        -RuntimeLsmSegmentTargetBytes $RuntimeLsmSegmentTargetBytes `
        -RuntimeSidecarInvalidateEveryN $RuntimeSidecarInvalidateEveryN `
        -RuntimeSidecarInvalidateVerbose:$RuntimeSidecarInvalidateVerbose `
        -RuntimeSidecarInvalidateAsync:$RuntimeSidecarInvalidateAsync `
        -RuntimeQuietSessionLog:$RuntimeQuietSessionLog `
        -RuntimeUseBulkInsertFast:$RuntimeUseBulkInsertFast `
        -RuntimeLsmCompactionAsync:$RuntimeLsmCompactionAsync `
        -RuntimeLsmCompactionWorkers $RuntimeLsmCompactionWorkers `
        -RuntimeLsmCompactionReapBudget $RuntimeLsmCompactionReapBudget `
        -RuntimeLsmL0CompactTrigger $RuntimeLsmL0CompactTrigger `
        -RuntimeLsmL0CompactBatch $RuntimeLsmL0CompactBatch `
        -RuntimeLsmFlushTriggerMultiplier $RuntimeLsmFlushTriggerMultiplier `
        -RuntimeWalSyncMode $RuntimeWalSyncMode `
        -RuntimeWalSyncNormalIntervalMs $RuntimeWalSyncNormalIntervalMs
    $pressureRunId = $pressure.run_id

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
    if ($MaxVacuumCompactFailureDelta -ge 0.0) {
        $reportCmd += @("--max-vacuum-compact-failure-delta", "$MaxVacuumCompactFailureDelta")
    }
    if ($MinVacuumCompactReclaimedBytesDelta -ge 0.0) {
        $reportCmd += @("--min-vacuum-compact-reclaimed-bytes-delta", "$MinVacuumCompactReclaimedBytesDelta")
    }
    if ($MaxVacuumQueueDepthPeak -ge 0.0) {
        $reportCmd += @("--max-vacuum-queue-depth-peak", "$MaxVacuumQueueDepthPeak")
    }
    if ($MaxWalRecoveryLastElapsedMs -ge 0.0) {
        $reportCmd += @("--max-wal-recovery-last-elapsed-ms", "$MaxWalRecoveryLastElapsedMs")
    }
    if ($MaxLockDeadlockDetectDelta -ge 0.0) {
        $reportCmd += @("--max-lock-deadlock-detect-delta", "$MaxLockDeadlockDetectDelta")
    }
    if ($MaxLockDeadlockVictimDelta -ge 0.0) {
        $reportCmd += @("--max-lock-deadlock-victim-delta", "$MaxLockDeadlockVictimDelta")
    }
    if ($MaxLockWaitMaxMsDelta -ge 0.0) {
        $reportCmd += @("--max-lock-wait-max-ms-delta", "$MaxLockWaitMaxMsDelta")
    }
    if ($MaxSchedulerThrottleDelta -ge 0.0) {
        $reportCmd += @("--max-scheduler-throttle-delta", "$MaxSchedulerThrottleDelta")
    }
    if ($MinWalGroupCommitBatchCommitsDelta -ge 0.0) {
        $reportCmd += @("--min-wal-group-commit-batch-commits-delta", "$MinWalGroupCommitBatchCommitsDelta")
    }
    if ($MaxLsmSegmentCount -ge 0.0) {
        $reportCmd += @("--max-lsm-segment-count", "$MaxLsmSegmentCount")
    }
    if ($MinLsmMemtableFlushDelta -ge 0.0) {
        $reportCmd += @("--min-lsm-memtable-flush-delta", "$MinLsmMemtableFlushDelta")
    }
    if ($MaxLsmReadSegmentsScannedP95 -ge 0.0) {
        $reportCmd += @("--max-lsm-read-segments-scanned-p95", "$MaxLsmReadSegmentsScannedP95")
    }
    if ($MinLsmCompactionBytesAmpEfficiency -ge 0.0) {
        $reportCmd += @("--min-lsm-compaction-bytes-amp-efficiency", "$MinLsmCompactionBytesAmpEfficiency")
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
    if ($null -eq $runtimeGateSummary) {
        throw "runtime gate summary is empty"
    }
}

 $resultDir = Join-Path $PSScriptRoot "results"
 if (-not (Test-Path $resultDir)) {
     New-Item -Path $resultDir -ItemType Directory | Out-Null
 }
 $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
 $summaryPath = Join-Path $resultDir ("concurrent_pressure_summary_" + $stamp + ".json")
 $runtimeGateSummaryPath = $null
 if ($runtimeGateSummary -ne $null) {
     $runtimeGateSummaryPath = Join-Path $resultDir ("runtime_gate_summary_" + $stamp + ".json")
     $runtimeGateSummary | ConvertTo-Json -Depth 8 | Set-Content -Path $runtimeGateSummaryPath
 }
 $batchMs = @()
 $pressureElapsedMs = 0.0
 if ($pressure) {
     $batchMs = @($pressure.batch_ms)
     $pressureElapsedMs = [double]$pressure.pressure_elapsed_ms
 }
 function Pct([double[]]$arr, [double]$p) {
     if ($arr.Count -eq 0) { return 0.0 }
     $sorted = @($arr | Sort-Object)
     $rank = [math]::Ceiling($p * $sorted.Count)
     $idx = [Math]::Min([Math]::Max($rank - 1, 0), $sorted.Count - 1)
     return [double]$sorted[$idx]
 }
 $batchP50 = [math]::Round((Pct -arr $batchMs -p 0.50), 3)
 $batchP95 = [math]::Round((Pct -arr $batchMs -p 0.95), 3)
 $batchMax = if ($batchMs.Count -gt 0) { [math]::Round((($batchMs | Measure-Object -Maximum).Maximum), 3) } else { 0.0 }
 $opsEst = [double]($RuntimePressureBatches * $RuntimePressureBatchSize)
 $tpsEst = if ($pressureElapsedMs -gt 0) { [math]::Round(($opsEst * 1000.0) / $pressureElapsedMs, 3) } else { 0.0 }
$whereFallbackScansMax = 0.0
$whereEqSidecarPlanDelta = 0.0
$maintenanceCheckpointTriggerDelta = 0.0
$maintenanceCheckpointVacuumEnqueueDelta = 0.0
$hybridMode = ""
$hybridModeSwitchCount = 0
$hybridLastSwitchReason = ""
$lsmAmpMinWindow = 0.0
$lsmScanP95Window = 0.0
if ($runtimeGateSummary -ne $null) {
    if ($runtimeGateSummary.PSObject.Properties.Name -contains "where_fallback_scans_max") {
        $whereFallbackScansMax = [double]$runtimeGateSummary.where_fallback_scans_max
    }
    if ($runtimeGateSummary.PSObject.Properties.Name -contains "where_plan_eq_sidecar_count_delta") {
        $whereEqSidecarPlanDelta = [double]$runtimeGateSummary.where_plan_eq_sidecar_count_delta
    }
    if ($runtimeGateSummary.PSObject.Properties.Name -contains "maintenance_checkpoint_trigger_count_delta") {
        $maintenanceCheckpointTriggerDelta = [double]$runtimeGateSummary.maintenance_checkpoint_trigger_count_delta
    }
    if ($runtimeGateSummary.PSObject.Properties.Name -contains "maintenance_checkpoint_vacuum_enqueue_count_delta") {
        $maintenanceCheckpointVacuumEnqueueDelta = [double]$runtimeGateSummary.maintenance_checkpoint_vacuum_enqueue_count_delta
    }
}
try {
    $lastRuntime = Get-Content -Path $runtimeJsonlPath | Select-Object -Last 1
    if (-not [string]::IsNullOrWhiteSpace($lastRuntime)) {
        $runtimeObj = $lastRuntime | ConvertFrom-Json
        if ($runtimeObj -and $runtimeObj.stats) {
            if ($runtimeObj.stats.PSObject.Properties.Name -contains "hybrid_mode") {
                $hybridMode = [string]$runtimeObj.stats.hybrid_mode
            }
            if ($runtimeObj.stats.PSObject.Properties.Name -contains "hybrid_mode_switch_count") {
                $hybridModeSwitchCount = [int]$runtimeObj.stats.hybrid_mode_switch_count
            }
            if ($runtimeObj.stats.PSObject.Properties.Name -contains "hybrid_last_switch_reason") {
                $hybridLastSwitchReason = [string]$runtimeObj.stats.hybrid_last_switch_reason
            }
            if ($runtimeObj.stats.PSObject.Properties.Name -contains "lsm_compaction_bytes_amp_efficiency_min_window") {
                $lsmAmpMinWindow = [double]$runtimeObj.stats.lsm_compaction_bytes_amp_efficiency_min_window
            }
            if ($runtimeObj.stats.PSObject.Properties.Name -contains "lsm_read_segments_scanned_p95_window") {
                $lsmScanP95Window = [double]$runtimeObj.stats.lsm_read_segments_scanned_p95_window
            }
        }
    }
} catch {
    # Keep summary generation resilient; runtime gate handles strict checks.
}
 [ordered]@{
     benchmark_profile = $BenchmarkProfile
     timestamp = (Get-Date).ToString("o")
     build_dir = $BuildDir
     jobs = $Jobs
     repeat_until_fail = $RepeatUntilFail
     runtime_stats_raw_jsonl = $runtimeJsonlPath
     runtime_stats_jsonl = $runtimeJsonlPath
     runtime_gate_summary_json = $runtimeGateSummaryPath
     runtime_jsonl_append_mode = [bool]$AppendRuntimeJsonl
     runtime_gate_enabled = [bool]$RunRuntimeGate
     runtime_pressure_batches = $RuntimePressureBatches
     runtime_pressure_batch_size = $RuntimePressureBatchSize
     runtime_sample_every_batches = $RuntimeSampleEveryBatches
     runtime_progress_every_batches = $RuntimeProgressEveryBatches
     runtime_progress_every_rows = $RuntimeProgressEveryRows
     runtime_lsm_segment_target_bytes = $RuntimeLsmSegmentTargetBytes
     runtime_sidecar_invalidate_every_n = $RuntimeSidecarInvalidateEveryN
     runtime_sidecar_invalidate_async = [bool]$RuntimeSidecarInvalidateAsync
     runtime_quiet_session_log = [bool]$RuntimeQuietSessionLog
     runtime_use_bulk_insert_fast = [bool]$RuntimeUseBulkInsertFast
     runtime_lsm_compaction_async = [bool]$RuntimeLsmCompactionAsync
     runtime_lsm_compaction_workers = $RuntimeLsmCompactionWorkers
     runtime_lsm_compaction_reap_budget = $RuntimeLsmCompactionReapBudget
     runtime_lsm_l0_compact_trigger = $RuntimeLsmL0CompactTrigger
     runtime_lsm_l0_compact_batch = $RuntimeLsmL0CompactBatch
     runtime_lsm_flush_trigger_multiplier = $RuntimeLsmFlushTriggerMultiplier
     runtime_walsync_mode = $RuntimeWalSyncMode
     runtime_walsync_normal_interval_ms = $RuntimeWalSyncNormalIntervalMs
     runtime_compaction_policy = if ($BenchmarkProfile -eq "leveldb-like") { "size_tiered" } else { "leveled_lite" }
     runtime_hybrid_mode = $hybridMode
     runtime_hybrid_mode_switch_count = $hybridModeSwitchCount
     runtime_hybrid_last_switch_reason = $hybridLastSwitchReason
     runtime_lsm_amp_efficiency_min_window = $lsmAmpMinWindow
     runtime_lsm_scan_p95_window = $lsmScanP95Window
     runtime_run_id = $pressureRunId
     runtime_pressure_elapsed_ms = $pressureElapsedMs
     runtime_pressure_ops_est = [int]$opsEst
     runtime_pressure_tps_est = $tpsEst
     runtime_pressure_batch_ms_p50 = $batchP50
     runtime_pressure_batch_ms_p95 = $batchP95
     runtime_pressure_batch_ms_max = $batchMax
    where_fallback_scans_max = $whereFallbackScansMax
    where_plan_eq_sidecar_count_delta = $whereEqSidecarPlanDelta
    maintenance_checkpoint_trigger_count_delta = $maintenanceCheckpointTriggerDelta
    maintenance_checkpoint_vacuum_enqueue_count_delta = $maintenanceCheckpointVacuumEnqueueDelta
     min_vacuum_efficiency = $MinVacuumEfficiency
     max_conflict_rate = $MaxConflictRate
     min_vacuum_efficiency_p50 = $MinVacuumEfficiencyP50
     max_conflict_rate_p95 = $MaxConflictRateP95
     max_txn_begin_lock_conflict_delta = $MaxTxnBeginLockConflictDelta
     max_wal_compact_delta = $MaxWalCompactDelta
     max_vacuum_compact_failure_delta = $MaxVacuumCompactFailureDelta
     min_vacuum_compact_reclaimed_bytes_delta = $MinVacuumCompactReclaimedBytesDelta
     max_vacuum_queue_depth_peak = $MaxVacuumQueueDepthPeak
     max_wal_recovery_last_elapsed_ms = $MaxWalRecoveryLastElapsedMs
     max_lock_deadlock_detect_delta = $MaxLockDeadlockDetectDelta
     max_lock_deadlock_victim_delta = $MaxLockDeadlockVictimDelta
     max_lock_wait_max_ms_delta = $MaxLockWaitMaxMsDelta
     max_scheduler_throttle_delta = $MaxSchedulerThrottleDelta
     min_wal_group_commit_batch_commits_delta = $MinWalGroupCommitBatchCommitsDelta
     max_lsm_segment_count = $MaxLsmSegmentCount
     min_lsm_memtable_flush_delta = $MinLsmMemtableFlushDelta
     max_lsm_read_segments_scanned_p95 = $MaxLsmReadSegmentsScannedP95
     min_lsm_compaction_bytes_amp_efficiency = $MinLsmCompactionBytesAmpEfficiency
     runtime_gate_summary = $runtimeGateSummary
     status = "passed"
 } | ConvertTo-Json -Depth 5 | Set-Content -Path $summaryPath
 Write-Host ("Concurrent pressure summary: {0}" -f $summaryPath)
 Write-Host ("PRESSURE_TPS_LATENCY tps_est={0} batch_ms_p50={1} batch_ms_p95={2} batch_ms_max={3} elapsed_ms={4}" -f $tpsEst, $batchP50, $batchP95, $batchMax, $pressureElapsedMs)

Write-Host "Concurrent pressure done."
