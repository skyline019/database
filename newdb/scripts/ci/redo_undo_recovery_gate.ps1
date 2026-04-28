param(
    [string]$BuildDir = "build_mingw",
    [string]$TestsExe = "newdb_tests.exe",
    [string]$OutputDir = "",
    [ValidateSet("lite","strict","both")]
    [string]$IdempotentMode = "both"
)

$ErrorActionPreference = "Stop"

function Run-Step {
    param([string]$Cmd)
    Write-Host ">> $Cmd"
    Invoke-Expression $Cmd
    if ($LASTEXITCODE -ne 0) {
        throw "step failed: $Cmd (exit=$LASTEXITCODE)"
    }
}

$exe = Join-Path $BuildDir $TestsExe
if (-not (Test-Path $exe)) {
    throw "test binary not found: $exe"
}

function Run-RecoveryGateVariant {
    param([string]$Mode)
    $old = $env:NEWDB_REDO_IDEMPOTENT_MODE
    $env:NEWDB_REDO_IDEMPOTENT_MODE = $Mode
    try {
        Run-Step "& `"$exe`" `"--gtest_filter=DemoTxnWal.WalV1PayloadCarriesBeforeAfterAndOpSeq:DemoTxnWal.RecoveryIsIdempotentAcrossRestarts:DemoTxnWal.SavepointRollbackToKeepsEarlierOps:DemoTxnWal.RecoverToLsnSkipsLaterCommittedOps:DemoTxnWal.RecoverToTimeSkipsLaterCommittedOps:WalManager.CheckpointWritesBeginAndEndMarkers`""
    } finally {
        if ($null -eq $old) {
            Remove-Item Env:NEWDB_REDO_IDEMPOTENT_MODE -ErrorAction SilentlyContinue
        } else {
            $env:NEWDB_REDO_IDEMPOTENT_MODE = $old
        }
    }
}

# Lite vs strict tiered gate
if ($IdempotentMode -eq "lite") {
    Run-RecoveryGateVariant "lite"
} elseif ($IdempotentMode -eq "strict") {
    Run-RecoveryGateVariant "strict"
} else {
    Run-RecoveryGateVariant "lite"
    Run-RecoveryGateVariant "strict"
}

# Crash injection matrix (mode-independent)
Run-Step "& `"$exe`" `"--gtest_filter=DemoTxnWal.WalCrashInjectionPointsReturnFailure:DemoTxnWal.WalCrashInjectionStrictMatrixAllCombinations`""

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "scripts/results"
}
if (-not (Test-Path -LiteralPath $OutputDir)) {
    New-Item -Path $OutputDir -ItemType Directory | Out-Null
}

$crashPoints = @(
    "commit_before_write",
    "commit_after_write",
    "flush_before",
    "flush_after",
    "checkpoint_between_begin_end",
    "rotate_before",
    "rotate_after"
)
$matrix = @()
foreach ($point in $crashPoints) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $old = $env:NEWDB_WAL_CRASH_MATRIX_POINT
    $env:NEWDB_WAL_CRASH_MATRIX_POINT = $point
    & "$exe" "--gtest_filter=DemoTxnWal.WalCrashInjectionSinglePointFromEnv" | Out-Null
    $exitCode = $LASTEXITCODE
    $sw.Stop()
    if ($null -eq $old) {
        Remove-Item Env:NEWDB_WAL_CRASH_MATRIX_POINT -ErrorAction SilentlyContinue
    } else {
        $env:NEWDB_WAL_CRASH_MATRIX_POINT = $old
    }
    $row = [ordered]@{
        point = $point
        pass = ($exitCode -eq 0)
        elapsed_ms = [int]$sw.ElapsedMilliseconds
    }
    $matrix += [pscustomobject]$row
    if ($exitCode -ne 0) {
        throw "crash matrix single-point failed: point=$point exit_code=$exitCode"
    }
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$jsonPath = Join-Path $OutputDir ("redo_undo_crash_matrix_" + $stamp + ".json")
$payload = [ordered]@{
    schema_version = "newdb.redo_undo_crash_matrix.v1"
    ts_ms = [int64]([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
    gate = "redo_undo_recovery_gate"
    summary = [ordered]@{
        total = $matrix.Count
        passed = @($matrix | Where-Object { $_.pass }).Count
        failed = @($matrix | Where-Object { -not $_.pass }).Count
    }
    points = $matrix
}
($payload | ConvertTo-Json -Depth 6) | Set-Content -Path $jsonPath -Encoding UTF8
Write-Host ("REDO_UNDO_CRASH_MATRIX_JSON {0}" -f $jsonPath)
Write-Host "redo/undo recovery gate passed"
