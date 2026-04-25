param(
    [string]$BuildDir = "build_mingw",
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_regression",
    [int]$CtestJobs = 8,
    [int]$Runs = 1,
    [int]$SoakMinutes = 30,
    [int]$SleepSecondsBetweenRuns = 30,
    [switch]$ContinueOnFailure,
    [switch]$SkipBuildAndCoreGates,
    [string]$HighPressureSizesCsv = "100000,300000,1000000",
    [int]$HighPressureQueryLoops = 2,
    [int]$HighPressureTxnPerMode = 40,
    [int]$HighPressureBuildChunkSize = 20000,
    [double]$MaxHighPressureQueryAvgMs = 300.0,
    [double]$MaxHighPressureQueryAvgMs300K = 8000.0,
    [double]$MaxHighPressureQueryAvgMs1M = 20000.0,
    [double]$MaxHighPressureQueryAvgMs3M = 45000.0,
    [string]$TelemetryEnvironment = "nightly",
    [string]$TelemetryProfile = "soak"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)

if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}
$isWin = ($env:OS -eq "Windows_NT")
if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($isWin) { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path $BuildDir $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}

if ($Runs -lt 1) {
    throw "Runs must be >= 1"
}
if ($SoakMinutes -lt 0) {
    throw "SoakMinutes must be >= 0"
}

$resultDir = Join-Path (Split-Path -Parent $scriptsRoot) "results"
if (-not (Test-Path $resultDir)) {
    New-Item -Path $resultDir -ItemType Directory | Out-Null
}
$nightlyTrendPath = Join-Path $resultDir "nightly_soak_trend.jsonl"

function Parse-SummaryPath([string]$text) {
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "==>\s+Summary JSON:\s+(.+)$") {
            return $matches[1].Trim()
        }
    }
    return $null
}

function Parse-TelemetryPath([string]$text) {
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "==>\s+Telemetry JSONL:\s+(.+)$") {
            return $matches[1].Trim()
        }
    }
    return $null
}

function Invoke-PythonScript {
    param(
        [string]$ScriptPath,
        [string[]]$ScriptArgs
    )
    $pythonCandidates = @(
        @("python3"),
        @("py", "-3"),
        @("python")
    )
    foreach ($candidate in $pythonCandidates) {
        $cmd = $candidate[0]
        $prefix = @()
        if ($candidate.Length -gt 1) {
            $prefix = $candidate[1..($candidate.Length - 1)]
        }
        & $cmd @prefix $ScriptPath @ScriptArgs
        if ($LASTEXITCODE -eq 0) {
            return
        }
    }
    throw "failed to execute python script: $ScriptPath"
}

Write-Host ("Nightly soak start: runs={0} soak_minutes={1}" -f $Runs, $SoakMinutes)

for ($i = 1; $i -le $Runs; $i++) {
    Write-Host ("==> Nightly run {0}/{1}" -f $i, $Runs)
    $args = @(
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $scriptsRoot "test_loop.ps1"),
        "-BuildDir", $BuildDir,
        "-DemoExe", $DemoExe,
        "-DataDir", $DataDir,
        "-Table", $Table,
        "-CtestJobs", $CtestJobs,
        "-EnforcePerf",
        "-RunHighPressure",
        "-RunConcurrentMillion",
        "-HighPressureSizesCsv", $HighPressureSizesCsv,
        "-HighPressureQueryLoops", $HighPressureQueryLoops,
        "-HighPressureTxnPerMode", $HighPressureTxnPerMode,
        "-HighPressureBuildChunkSize", $HighPressureBuildChunkSize,
        "-SoakMinutes", $SoakMinutes,
        "-MaxHighPressureQueryAvgMs", $MaxHighPressureQueryAvgMs,
        "-MaxHighPressureQueryAvgMs300K", $MaxHighPressureQueryAvgMs300K,
        "-MaxHighPressureQueryAvgMs1M", $MaxHighPressureQueryAvgMs1M,
        "-MaxHighPressureQueryAvgMs3M", $MaxHighPressureQueryAvgMs3M,
        "-TelemetryEnvironment", $TelemetryEnvironment,
        "-TelemetryProfile", $TelemetryProfile
    )
    if ($SkipBuildAndCoreGates) {
        $args += "-SkipBuildAndCoreGates"
    }

    $started = (Get-Date).ToString("o")
    $runOutput = & powershell @args | Out-String
    $exitCode = $LASTEXITCODE
    Write-Host $runOutput

    $summaryPath = Parse-SummaryPath $runOutput
    $telemetryPath = Parse-TelemetryPath $runOutput
    $perf = $null
    $soak = $null
    if ($summaryPath -and (Test-Path -LiteralPath $summaryPath)) {
        Invoke-PythonScript -ScriptPath (Join-Path (Split-Path -Parent $scriptsRoot) "validate/validate_perf_summary.py") -ScriptArgs @($summaryPath)
        $summary = Get-Content -Path $summaryPath -Raw | ConvertFrom-Json
        $perf = $summary.perf
        $soak = $summary.soak
    }
    if ($telemetryPath -and (Test-Path -LiteralPath $telemetryPath)) {
        Invoke-PythonScript -ScriptPath (Join-Path (Split-Path -Parent $scriptsRoot) "validate/validate_telemetry_event.py") -ScriptArgs @($telemetryPath)
    }

    $record = [ordered]@{
        schema_version = "newdb.nightly_soak_trend.v1"
        timestamp = (Get-Date).ToString("o")
        run_index = $i
        runs_total = $Runs
        started_at = $started
        status = if ($exitCode -eq 0) { "passed" } else { "failed" }
        exit_code = $exitCode
        summary = $summaryPath
        telemetry = $telemetryPath
        txn_normal_avg_ms = if ($perf) { $perf.txn_normal_avg_ms } else { $null }
        query_avg_ms_max = if ($perf) { $perf.query_avg_ms_max } else { $null }
        hp_max_query_avg_ms = if ($perf) { $perf.hp_max_query_avg_ms } else { $null }
        cm_tps_min = if ($perf) { $perf.cm_tps_min } else { $null }
        where_policy_rejects = if ($perf) { $perf.where_policy_rejects } else { $null }
        where_policy_fallbacks = if ($perf) { $perf.where_policy_fallbacks } else { $null }
        runtime_samples = if ($perf) { $perf.runtime_samples } else { $null }
        runtime_vacuum_efficiency_p50 = if ($perf) { $perf.runtime_vacuum_efficiency_p50 } else { $null }
        runtime_conflict_rate_p95 = if ($perf) { $perf.runtime_conflict_rate_p95 } else { $null }
        runtime_txn_begin_lock_conflict_delta = if ($perf) { $perf.runtime_txn_begin_lock_conflict_delta } else { $null }
        runtime_wal_compact_delta = if ($perf) { $perf.runtime_wal_compact_delta } else { $null }
        runtime_run_id = if ($perf) { $perf.runtime_run_id } else { $null }
        soak_repeat = if ($soak) { $soak.repeat } else { $null }
        soak_summary = if ($soak) { $soak.summary } else { $null }
    }
    ($record | ConvertTo-Json -Compress) | Add-Content -Path $nightlyTrendPath
    Write-Host ("Nightly trend append: {0}" -f $nightlyTrendPath)

    $testLoopTrendPath = Join-Path $resultDir "test_loop_trend.jsonl"
    $dashboardPath = Join-Path $resultDir "runtime_trend_dashboard.json"
    Invoke-PythonScript -ScriptPath (Join-Path $scriptsRoot "runtime_trend_rollup.py") -ScriptArgs @(
        "--test-loop-trend", $testLoopTrendPath,
        "--nightly-trend", $nightlyTrendPath,
        "--output", $dashboardPath
    )
    Invoke-PythonScript -ScriptPath (Join-Path (Split-Path -Parent $scriptsRoot) "validate/validate_runtime_trend_dashboard.py") -ScriptArgs @($dashboardPath)
    Write-Host ("Nightly dashboard refresh: {0}" -f $dashboardPath)

    if ($exitCode -ne 0 -and -not $ContinueOnFailure) {
        throw "Nightly run failed at index=$i exit_code=$exitCode"
    }
    if ($i -lt $Runs -and $SleepSecondsBetweenRuns -gt 0) {
        Start-Sleep -Seconds $SleepSecondsBetweenRuns
    }
}

Write-Host ("Nightly soak done. trend={0}" -f $nightlyTrendPath)
