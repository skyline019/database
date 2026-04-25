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
    [string]$TelemetryProfile = "soak",
    [bool]$RequireNightlySamplesForDashboard = $true,
    [double]$MaxLatestNightlyAgeHours = 48.0,
    [string]$MaxDashboardHealthTier = "warning",
    [double]$WarnMaxQueryAvgMs = 100.0,
    [double]$CriticalMaxQueryAvgMs = 300.0,
    [double]$WarnMinCmTps = 25000.0,
    [double]$CriticalMinCmTps = 15000.0,
    [double]$WarnMinNightlyPassRate = 0.90,
    [double]$CriticalMinNightlyPassRate = 0.70,
    [switch]$LiteProfile
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

function Invoke-PythonScriptCapture {
    param(
        [string]$ScriptPath,
        [string[]]$ScriptArgs
    )
    $pythonCandidates = @(
        @("python3"),
        @("py", "-3"),
        @("python")
    )
    $lastOutput = ""
    foreach ($candidate in $pythonCandidates) {
        $cmd = $candidate[0]
        $prefix = @()
        if ($candidate.Length -gt 1) {
            $prefix = $candidate[1..($candidate.Length - 1)]
        }
        $out = (& $cmd @prefix $ScriptPath @ScriptArgs 2>&1 | Out-String)
        if ($LASTEXITCODE -eq 0) {
            return [pscustomobject]@{
                ok = $true
                exit_code = 0
                output = $out.Trim()
            }
        }
        $lastOutput = $out
    }
    return [pscustomobject]@{
        ok = $false
        exit_code = 1
        output = $lastOutput.Trim()
    }
}

function Load-JsonObject {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return $null
    }
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    try {
        return (Get-Content -Path $Path -Raw | ConvertFrom-Json)
    } catch {
        return $null
    }
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
    if ($LiteProfile) {
        # P13: stable nightly sample mode, avoid high-pressure branches.
        $args += "-RunConcurrentPressure"
    } else {
        $args += @(
            "-EnforcePerf",
            "-RunHighPressure",
            "-RunConcurrentMillion"
        )
    }
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

    $testLoopTrendPath = Join-Path $resultDir "test_loop_trend.jsonl"
    $dashboardPath = Join-Path $resultDir "runtime_trend_dashboard.json"
    $hasExistingNightlySamples = $false
    if (Test-Path -LiteralPath $nightlyTrendPath) {
        $nightlyRaw = (Get-Content -Path $nightlyTrendPath -Raw)
        if (-not [string]::IsNullOrWhiteSpace($nightlyRaw)) {
            $hasExistingNightlySamples = $true
        }
    }

    $rollupArgs = @(
        "--test-loop-trend", $testLoopTrendPath,
        "--nightly-trend", $nightlyTrendPath,
        "--output", $dashboardPath,
        "--max-latest-nightly-age-hours", "$MaxLatestNightlyAgeHours",
        "--max-health-tier", "$MaxDashboardHealthTier",
        "--warn-max-query-avg-ms", "$WarnMaxQueryAvgMs",
        "--critical-max-query-avg-ms", "$CriticalMaxQueryAvgMs",
        "--warn-min-cm-tps", "$WarnMinCmTps",
        "--critical-min-cm-tps", "$CriticalMinCmTps",
        "--warn-min-nightly-pass-rate", "$WarnMinNightlyPassRate",
        "--critical-min-nightly-pass-rate", "$CriticalMinNightlyPassRate"
    )
    $bootstrapBypassRequireNightly = $RequireNightlySamplesForDashboard -and (-not $hasExistingNightlySamples)
    if ($RequireNightlySamplesForDashboard -and (-not $bootstrapBypassRequireNightly)) {
        $rollupArgs += "--require-nightly-samples"
    }
    $rollup = Invoke-PythonScriptCapture -ScriptPath (Join-Path $scriptsRoot "runtime_trend_rollup.py") -ScriptArgs $rollupArgs
    $dashboardQualityGateStatus = if ($rollup.ok) { "passed" } else { "failed" }
    $dashboardQualityGateReason = if ($rollup.ok) { "" } else { $rollup.output }
    if ($bootstrapBypassRequireNightly) {
        $dashboardQualityGateReason = @(
            "bootstrap_bypass=require-nightly-samples",
            "reason=no_existing_nightly_samples",
            "note=first-run allows seed append before strict gate"
        ) -join "; "
    }
    if (-not [string]::IsNullOrWhiteSpace($rollup.output)) {
        Write-Host $rollup.output
    }
    if ($rollup.ok) {
        Invoke-PythonScript -ScriptPath (Join-Path (Split-Path -Parent $scriptsRoot) "validate/validate_runtime_trend_dashboard.py") -ScriptArgs @($dashboardPath)
        Write-Host ("Nightly dashboard refresh: {0}" -f $dashboardPath)
    }
    $dashboardHealthTier = $null
    $dashboardHealthReasons = $null
    $dashboardObj = Load-JsonObject -Path $dashboardPath
    if ($dashboardObj -and $dashboardObj.health) {
        $dashboardHealthTier = [string]$dashboardObj.health.tier
        if ($dashboardObj.health.reasons) {
            $dashboardHealthReasons = (($dashboardObj.health.reasons | ForEach-Object { "$_" }) -join " | ")
        }
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
        dashboard_path = $dashboardPath
        dashboard_quality_gate_status = $dashboardQualityGateStatus
        dashboard_quality_gate_reason = $dashboardQualityGateReason
        dashboard_health_tier = $dashboardHealthTier
        dashboard_health_reasons = $dashboardHealthReasons
    }
    ($record | ConvertTo-Json -Compress) | Add-Content -Path $nightlyTrendPath
    Write-Host ("Nightly trend append: {0}" -f $nightlyTrendPath)

    if ((-not $rollup.ok) -and -not $ContinueOnFailure) {
        throw "Nightly dashboard quality gate failed at index=${i}: $dashboardQualityGateReason"
    }
    if ($exitCode -ne 0 -and -not $ContinueOnFailure) {
        throw "Nightly run failed at index=$i exit_code=$exitCode"
    }
    if ($i -lt $Runs -and $SleepSecondsBetweenRuns -gt 0) {
        Start-Sleep -Seconds $SleepSecondsBetweenRuns
    }
}

Write-Host ("Nightly soak done. trend={0}" -f $nightlyTrendPath)
