Set-StrictMode -Version Latest

function Get-NewdbPressureProfileThresholds {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("newdb-default", "leveldb-like", "innodb-like", "hybrid-balanced")]
        [string]$Profile,

        # Allows callers under rust_gui resources to point at repo root explicitly.
        [string]$ProjectRoot = ""
    )

    $scriptsRoot = $PSScriptRoot
    if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
        $ProjectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
    }
    $jsonPath = Join-Path $ProjectRoot "scripts/ci/pressure_profile_thresholds.json"
    if (-not (Test-Path -LiteralPath $jsonPath)) {
        throw "threshold config not found: $jsonPath"
    }
    $raw = Get-Content -Path $jsonPath -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        throw "threshold config empty: $jsonPath"
    }
    $cfg = $raw | ConvertFrom-Json
    if (-not $cfg -or -not $cfg.schema_version -or $cfg.schema_version -ne "newdb.ci_thresholds.v1") {
        throw "threshold config schema mismatch: $jsonPath"
    }
    if (-not $cfg.profiles -or -not $cfg.profiles.$Profile) {
        throw "threshold profile missing: profile=$Profile path=$jsonPath"
    }
    return $cfg.profiles.$Profile
}

function Apply-NewdbPressureProfileThresholdsIfUnset {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("newdb-default", "leveldb-like", "innodb-like", "hybrid-balanced")]
        [string]$Profile,

        # Callers provide their own parameter variables; we fill only when value < 0.
        [ref]$MaxVacuumCompactFailureDelta,
        [ref]$MaxVacuumQueueDepthPeak,
        [ref]$MaxWalRecoveryLastElapsedMs,
        [ref]$MaxLockDeadlockDetectDelta,
        [ref]$MaxLockDeadlockVictimDelta,
        [ref]$MaxLockWaitMaxMsDelta,
        [ref]$MaxSchedulerThrottleDelta,
        [ref]$MinWalGroupCommitBatchCommitsDelta,
        [ref]$MaxLsmSegmentCount,
        [ref]$MinLsmMemtableFlushDelta,
        [ref]$MaxLsmReadSegmentsScannedP95,
        [ref]$MinLsmCompactionBytesAmpEfficiency,

        [string]$ProjectRoot = ""
    )

    $t = Get-NewdbPressureProfileThresholds -Profile $Profile -ProjectRoot $ProjectRoot

    if ($MaxVacuumCompactFailureDelta.Value -lt 0.0) { $MaxVacuumCompactFailureDelta.Value = [double]$t.max_vacuum_compact_failure_delta }
    if ($MaxVacuumQueueDepthPeak.Value -lt 0.0) { $MaxVacuumQueueDepthPeak.Value = [double]$t.max_vacuum_queue_depth_peak }
    if ($MaxWalRecoveryLastElapsedMs.Value -lt 0.0) { $MaxWalRecoveryLastElapsedMs.Value = [double]$t.max_wal_recovery_last_elapsed_ms }
    if ($MaxLockDeadlockDetectDelta.Value -lt 0.0) { $MaxLockDeadlockDetectDelta.Value = [double]$t.max_lock_deadlock_detect_delta }
    if ($MaxLockDeadlockVictimDelta.Value -lt 0.0) { $MaxLockDeadlockVictimDelta.Value = [double]$t.max_lock_deadlock_victim_delta }
    if ($MaxLockWaitMaxMsDelta.Value -lt 0.0) { $MaxLockWaitMaxMsDelta.Value = [double]$t.max_lock_wait_max_ms_delta }
    if ($MaxSchedulerThrottleDelta.Value -lt 0.0) { $MaxSchedulerThrottleDelta.Value = [double]$t.max_scheduler_throttle_delta }
    if ($MinWalGroupCommitBatchCommitsDelta.Value -lt 0.0) { $MinWalGroupCommitBatchCommitsDelta.Value = [double]$t.min_wal_group_commit_batch_commits_delta }
    if ($MaxLsmSegmentCount.Value -lt 0.0) { $MaxLsmSegmentCount.Value = [double]$t.max_lsm_segment_count }
    if ($MinLsmMemtableFlushDelta.Value -lt 0.0) { $MinLsmMemtableFlushDelta.Value = [double]$t.min_lsm_memtable_flush_delta }
    if ($MaxLsmReadSegmentsScannedP95.Value -lt 0.0) { $MaxLsmReadSegmentsScannedP95.Value = [double]$t.max_lsm_read_segments_scanned_p95 }
    if ($MinLsmCompactionBytesAmpEfficiency.Value -lt 0.0) { $MinLsmCompactionBytesAmpEfficiency.Value = [double]$t.min_lsm_compaction_bytes_amp_efficiency }
}

