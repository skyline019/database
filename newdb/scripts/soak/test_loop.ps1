param(
    [string]$BuildDir = "build_mingw",
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_regression",
    [int]$CtestJobs = 8,
    [switch]$EnforcePerf,
    [double]$MaxTxnAvgMsFull = 50.0,
    [double]$MaxTxnAvgMsNormal = 20.0,
    [double]$MaxQueryAvgMs = 50.0,
    [double]$MaxVischkRegressionPct = 15.0,
    [int]$TxnBenchTxns = 80,
    [int]$QueryBenchLoops = 20,
    [int]$QueryBenchWarmupLoops = 5,
    [int]$EqCacheBenchPassQueries = 80,
    [int]$EqInvalidationBenchRows = 900,
    [int]$EqInvalidationBenchLoops = 50,
    [switch]$RunHighPressure,
    [switch]$RunConcurrentPressure,
    [switch]$RunConcurrentMillion,
    [switch]$RunRecoveryGate = $true,
    [switch]$RunLockGate = $true,
    [switch]$SkipBuildAndCoreGates,
    [int]$SoakMinutes = 0,
    [string]$HighPressureSizesCsv = "100000,300000",
    [int]$HighPressureQueryLoops = 2,
    [int]$HighPressureTxnPerMode = 40,
    [int]$HighPressureBuildChunkSize = 20000,
    [double]$MaxHighPressureQueryAvgMs = 300.0,
    [double]$MaxHighPressureQueryAvgMs300K = 8000.0,
    [double]$MaxHighPressureQueryAvgMs1M = 20000.0,
    [double]$MaxHighPressureQueryAvgMs3M = 45000.0,
    [double]$MaxHighPressureTxnAvgMsNormal = 40.0,
    [double]$MinHighPressureBuildTps = 2000.0,
    [int]$ConcurrentRepeatUntilFail = 30,
    [string]$ConcurrentMillionThreadsCsv = "8,16,32",
    [int]$ConcurrentMillionTotalOps = 1000000,
    [double]$MinConcurrentMillionTps = 30000.0,
    [double]$MinEqSidecarCacheHitRate = 0.70,
    [double]$MaxDecodeDeltaAllowed = 0.0,
    [int]$MaxWherePolicyRejects = 1000000,
    [int]$MaxWhereFallbackScans = 1000000,
    [int]$BenchPrepRows = 1000,
    [int]$BenchPrepChunkSize = 500,
    [bool]$TimestampIsolatedDataDir = $true,
    [string]$TelemetryEnvironment = "dev",
    [string]$TelemetryProfile = "default"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
$scriptsBase = Split-Path -Parent $scriptsRoot

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

$baseDataDir = $DataDir
if (-not (Test-Path $baseDataDir)) {
    New-Item -Path $baseDataDir -ItemType Directory | Out-Null
}
if ($TimestampIsolatedDataDir) {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $DataDir = Join-Path $baseDataDir ("run_" + $stamp)
    New-Item -Path $DataDir -ItemType Directory -Force | Out-Null
    Write-Host ("==> DataDir timestamp isolation enabled: {0}" -f $DataDir)
} else {
    Write-Host ("==> DataDir timestamp isolation disabled: {0}" -f $DataDir)
}

function Exec([string]$label, [scriptblock]$cmd) {
    Write-Host "==> $label"
    & $cmd
    if ($LASTEXITCODE -ne 0) {
        throw "$label failed: exit_code=$LASTEXITCODE"
    }
}

function Parse-TxnBenchAvg([string]$text, [string]$mode) {
    # Accepts Format-Table output like: "full  80  1234  15.4  5180"
    $lines = $text -split "`r?`n"
    foreach ($ln in $lines) {
        $trim = $ln.Trim()
        if ($trim -match "^(full|normal|off)\s+\d+\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s*$") {
            $m = $matches[1]
            if ($m -ne $mode) { continue }
            return [double]$matches[3]
        }
    }
    return $null
}

function Parse-QueryBenchAvgs([string]$text) {
    # Matches: "avg_ms=12.345"
    $avgs = New-Object System.Collections.Generic.List[double]
    $lines = $text -split "`r?`n"
    foreach ($ln in $lines) {
        if ($ln -match "avg_ms=(\d+(\.\d+)?)") {
            $avgs.Add([double]$matches[1])
        }
    }
    return $avgs
}

function Parse-WherePolicySummary([string]$text) {
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "WHERE_POLICY_SUMMARY\s+reject_count=(\d+)\s+fallback_count=(\d+)") {
            return [pscustomobject]@{
                reject_count = [int]$matches[1]
                fallback_count = [int]$matches[2]
            }
        }
    }
    return [pscustomobject]@{
        reject_count = 0
        fallback_count = 0
    }
}

function Parse-DecodeDeltaMax([string]$text) {
    $max = 0.0
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "decode_delta=(\d+)") {
            $val = [double]$matches[1]
            if ($val -gt $max) { $max = $val }
        }
    }
    return $max
}

function Parse-ConcurrentPressureSummaryPath([string]$text) {
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "Concurrent pressure summary:\s+(.+)$") {
            return $matches[1].Trim()
        }
    }
    return $null
}

function Load-JsonFile([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path)) {
        return $null
    }
    if (-not (Test-Path -LiteralPath $path)) {
        return $null
    }
    try {
        return (Get-Content -Path $path -Raw | ConvertFrom-Json)
    } catch {
        return $null
    }
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

function Resolve-HighPressureQueryGate([double]$rows) {
    if ($rows -ge 3000000.0) { return $MaxHighPressureQueryAvgMs3M }
    if ($rows -ge 1000000.0) { return $MaxHighPressureQueryAvgMs1M }
    if ($rows -ge 300000.0) { return $MaxHighPressureQueryAvgMs300K }
    return $MaxHighPressureQueryAvgMs
}

function Parse-EqCacheHits([string]$text) {
    $hits = 0
    $queries = 0
    foreach ($ln in ($text -split "`r?`n")) {
        if ($ln -match "\[COUNT\].*covering sidecar") { $hits++ }
        if ($ln -match "\[COUNT\]" -or $ln -match "\[AVG\]" -or $ln -match "\[SUM\]") { $queries++ }
    }
    return [pscustomobject]@{ hits = $hits; queries = $queries }
}

function Ensure-BenchTable {
    param(
        [string]$DemoExe,
        [string]$DataDir,
        [string]$Table,
        [int]$Rows,
        [int]$ChunkSize
    )
    if ($Rows -lt 1) {
        throw "BenchPrepRows must be >= 1"
    }
    if ($ChunkSize -lt 1) {
        throw "BenchPrepChunkSize must be >= 1"
    }
    $bin = Join-Path $DataDir ($Table + ".bin")
    Write-Host ("==> Preparing bench table (reset): {0}" -f $bin)
    Write-Host ("[PREP] mode=BULKINSERTFAST rows={0} chunk_size={1}" -f $Rows, $ChunkSize)
    $scriptPath = Join-Path $env:TEMP ("bench_prep_" + $Table + "_" + [guid]::NewGuid().ToString("N") + ".mdb")
    $verifyPath = Join-Path $env:TEMP ("bench_verify_" + $Table + "_" + [guid]::NewGuid().ToString("N") + ".mdb")
    $lines = New-Object System.Collections.Generic.List[string]
    $swTotal = [System.Diagnostics.Stopwatch]::StartNew()
    $lines.Add("DROP TABLE($Table)")
    $lines.Add("CREATE TABLE($Table)")
    $lines.Add("USE($Table)")
    $lines.Add("DEFATTR(name:string,dept:string,age:int,salary:int)")
    $departments = @("ENG", "HR", "SALES", "OPS")
    $startId = 1
    $chunkIdx = 0
    while ($startId -le $Rows) {
        $count = [Math]::Min($ChunkSize, $Rows - $startId + 1)
        $dept = $departments[$chunkIdx % $departments.Count]
        $lines.Add("BULKINSERTFAST($startId,$count,$dept)")
        $startId += $count
        $chunkIdx++
    }
    $genMs = $swTotal.ElapsedMilliseconds
    Write-Host ("[PREP] generated chunks={0} gen_ms={1}" -f $chunkIdx, $genMs)
    $lines | Set-Content -Path $scriptPath

    $swRun = [System.Diagnostics.Stopwatch]::StartNew()
    $prepOut = & $DemoExe --data-dir $DataDir --table $Table --run-mdb $scriptPath 2>&1 | Out-String
    $swRun.Stop()
    if ($LASTEXITCODE -ne 0) {
        throw "Bench prep run-mdb failed: exit_code=$LASTEXITCODE"
    }
    Write-Host ("[PREP] run_ms={0}" -f [math]::Round($swRun.Elapsed.TotalMilliseconds, 2))
    $bulkLines = @($prepOut -split "`r?`n" | Where-Object { $_ -match "\[BULKINSERTFAST\]" -or $_ -match "\[BULKINSERT\]" })
    if ($bulkLines.Count -gt 0) {
        Write-Host ("[PREP] bulk_command_logs={0}" -f $bulkLines.Count)
    }

    @(
        "USE($Table)",
        "COUNT",
        "FIND(1)",
        "FIND($Rows)",
        "FIND($($Rows + 1))"
    ) | Set-Content -Path $verifyPath
    $swVerify = [System.Diagnostics.Stopwatch]::StartNew()
    $verifyOut = & $DemoExe --data-dir $DataDir --table $Table --run-mdb $verifyPath 2>&1 | Out-String
    $swVerify.Stop()
    if ($LASTEXITCODE -ne 0) {
        throw "Bench prep verify failed: exit_code=$LASTEXITCODE"
    }
    $m = [regex]::Match($verifyOut, "\[COUNT\].*rows=(\d+)")
    if (-not $m.Success) {
        throw "Bench prep verify cannot parse COUNT rows output."
    }
    $actualRows = [int]$m.Groups[1].Value
    Write-Host ("[PREP] verify_rows={0} expected_rows={1} verify_ms={2}" -f $actualRows, $Rows, [math]::Round($swVerify.Elapsed.TotalMilliseconds, 2))
    if ($actualRows -ne $Rows) {
        throw "Bench prep row count mismatch: expected=$Rows actual=$actualRows"
    }
    if (-not [regex]::IsMatch($verifyOut, "\[FIND\]\s+id=1\b")) {
        throw "Bench prep verify failed: FIND(1) missing."
    }
    if (-not [regex]::IsMatch($verifyOut, "\[FIND\]\s+id=$Rows\b")) {
        throw "Bench prep verify failed: FIND($Rows) missing."
    }
    if (-not [regex]::IsMatch($verifyOut, "\[FIND\]\s+id=$($Rows + 1)\s+not found")) {
        throw "Bench prep verify failed: FIND($($Rows + 1)) should be not found."
    }
    Write-Host ("[PREP] verify_find_ok first=1 last={0} missing={1}" -f $Rows, ($Rows + 1))

    $swTotal.Stop()
    Write-Host ("[PREP] total_ms={0}" -f [math]::Round($swTotal.Elapsed.TotalMilliseconds, 2))

    Remove-Item -Force $scriptPath -ErrorAction SilentlyContinue
    Remove-Item -Force $verifyPath -ErrorAction SilentlyContinue
}

if (-not (Test-Path -LiteralPath $BuildDir)) {
    throw "BuildDir not found: $BuildDir"
}

if (-not $SkipBuildAndCoreGates) {
    Exec "Build (cmake --build)" { cmake --build "$BuildDir" -j $CtestJobs }
    Exec "ctest (full suite)" { ctest --test-dir "$BuildDir" --output-on-failure -j $CtestJobs }
    Exec "CI microbench gate" { python (Join-Path $scriptsBase "ci/ci_bench_gate.py") "$BuildDir" }
} else {
    Write-Host "==> Skip build/core gates enabled; reuse current binaries."
}

$runStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runId = "testloop_" + $runStamp
$resultDir = Join-Path $scriptsBase "results"
if (-not (Test-Path $resultDir)) {
    New-Item -Path $resultDir -ItemType Directory | Out-Null
}
$summaryPath = Join-Path $resultDir ("test_loop_summary_" + $runStamp + ".json")
$telemetryPath = Join-Path $resultDir ("telemetry_events_" + $runStamp + ".jsonl")

function Add-TelemetryEvent {
    param(
        [string]$Phase,
        [hashtable]$Metrics
    )
    $evt = [ordered]@{
        schema_version = "newdb.telemetry.v1"
        timestamp = (Get-Date).ToString("o")
        run_id = $runId
        source = "test_loop"
        env = $TelemetryEnvironment
        build = $BuildDir
        profile = $TelemetryProfile
        phase = $Phase
        metrics = $Metrics
    }
    ($evt | ConvertTo-Json -Compress) | Add-Content -Path $telemetryPath
}

Ensure-BenchTable -DemoExe $DemoExe -DataDir $DataDir -Table $Table -Rows $BenchPrepRows -ChunkSize $BenchPrepChunkSize
Add-TelemetryEvent -Phase "prep" -Metrics @{
    bench_prep_rows = $BenchPrepRows
    bench_prep_chunk_size = $BenchPrepChunkSize
}

$perf = [ordered]@{
    txn_full_avg_ms = $null
    txn_normal_avg_ms = $null
    query_avg_ms_max = $null
    vischk_delta_pct = $null
    hp_max_query_avg_ms = $null
    hp_max_query_rows = $null
    hp_normal_avg_txn_ms = $null
    hp_min_build_tps = $null
    cm_tps_min = $null
    where_policy_rejects = 0
    where_policy_fallbacks = 0
    runtime_samples = $null
    runtime_vacuum_efficiency_p50 = $null
    runtime_conflict_rate_p95 = $null
    runtime_txn_begin_lock_conflict_delta = $null
    runtime_wal_compact_delta = $null
    runtime_run_id = $null
    soak_repeat = $null
    soak_summary = $null
}

Write-Host "==> Perf: txn_write_bench.ps1 (txns=$TxnBenchTxns)"
$txnOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/txn_write_bench.ps1") `
    -DemoExe $DemoExe -DataDir $DataDir -Table $Table -Txns $TxnBenchTxns | Out-String
Write-Host $txnOut
$perf.txn_full_avg_ms = Parse-TxnBenchAvg $txnOut "full"
$perf.txn_normal_avg_ms = Parse-TxnBenchAvg $txnOut "normal"
Add-TelemetryEvent -Phase "txn_bench" -Metrics @{
    txn_full_avg_ms = $perf.txn_full_avg_ms
    txn_normal_avg_ms = $perf.txn_normal_avg_ms
}

Write-Host "==> Perf: query_bench.ps1 (loops=$QueryBenchLoops warmup=$QueryBenchWarmupLoops)"
$queryOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/query_bench.ps1") `
    -DemoExe $DemoExe -DataDir $DataDir -Table $Table -Loops $QueryBenchLoops -WarmupLoops $QueryBenchWarmupLoops | Out-String
Write-Host $queryOut
$queryAvgs = Parse-QueryBenchAvgs $queryOut
$wherePolicyStats = Parse-WherePolicySummary $queryOut
$perf.where_policy_rejects = $wherePolicyStats.reject_count
$perf.where_policy_fallbacks = $wherePolicyStats.fallback_count
Write-Host ("[WHERE] rejects={0} fallbacks={1}" -f $perf.where_policy_rejects, $perf.where_policy_fallbacks)
if ($queryAvgs.Count -gt 0) {
    $perf.query_avg_ms_max = ($queryAvgs | Measure-Object -Maximum).Maximum
}
Add-TelemetryEvent -Phase "query_bench" -Metrics @{
    query_avg_ms_max = $perf.query_avg_ms_max
    where_policy_rejects = $perf.where_policy_rejects
    where_policy_fallbacks = $perf.where_policy_fallbacks
}

Write-Host "==> Perf: visibility checkpoint delta"
$oldVis = $env:NEWDB_VISCHK
$env:NEWDB_VISCHK = "off"
$queryOutOff = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/query_bench.ps1") `
    -DemoExe $DemoExe -DataDir $DataDir -Table $Table -Loops ([math]::Max(5, [int]($QueryBenchLoops / 2))) -WarmupLoops ([math]::Max(2, [int]($QueryBenchWarmupLoops / 2))) | Out-String
$offAvgs = Parse-QueryBenchAvgs $queryOutOff
$env:NEWDB_VISCHK = "on"
$queryOutOn = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/query_bench.ps1") `
    -DemoExe $DemoExe -DataDir $DataDir -Table $Table -Loops ([math]::Max(5, [int]($QueryBenchLoops / 2))) -WarmupLoops ([math]::Max(2, [int]($QueryBenchWarmupLoops / 2))) | Out-String
$onAvgs = Parse-QueryBenchAvgs $queryOutOn
if ($null -ne $oldVis) {
    $env:NEWDB_VISCHK = $oldVis
} else {
    Remove-Item Env:NEWDB_VISCHK -ErrorAction SilentlyContinue
}
if ($offAvgs.Count -gt 0 -and $onAvgs.Count -gt 0) {
    $offMax = ($offAvgs | Measure-Object -Maximum).Maximum
    $onMax = ($onAvgs | Measure-Object -Maximum).Maximum
    if ($offMax -gt 0) {
        $perf.vischk_delta_pct = [math]::Round((($onMax - $offMax) / $offMax) * 100.0, 3)
    } else {
        $perf.vischk_delta_pct = 0.0
    }
}

Write-Host "==> Perf: eq_sidecar_cache_bench.ps1 (PassQueries=$EqCacheBenchPassQueries)"
$eqCacheOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/eq_sidecar_cache_bench.ps1") `
    -DemoExe $DemoExe -DataDir $DataDir -Table $Table -PassQueries $EqCacheBenchPassQueries | Out-String
Write-Host $eqCacheOut
$eqCacheStats = Parse-EqCacheHits $eqCacheOut
Add-TelemetryEvent -Phase "eq_cache_bench" -Metrics @{
    eq_hits = $eqCacheStats.hits
    eq_queries = $eqCacheStats.queries
}

Write-Host "==> Perf: eq_sidecar_invalidation_bench.ps1 (Rows=$EqInvalidationBenchRows Loops=$EqInvalidationBenchLoops)"
Exec "eq_sidecar_invalidation_bench" {
    powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/eq_sidecar_invalidation_bench.ps1") `
        -DemoExe $DemoExe -DataDir $DataDir -Table ($Table + "_eqinv") -Rows $EqInvalidationBenchRows -Loops $EqInvalidationBenchLoops
}

if ($RunHighPressure) {
    Write-Host "==> Perf: million_scale_bench.ps1 (high pressure)"
    $hpOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/million_scale_bench.ps1") `
        -DemoExe $DemoExe -DataDir $DataDir -SizesCsv $HighPressureSizesCsv -QueryLoops $HighPressureQueryLoops `
        -TxnPerMode $HighPressureTxnPerMode -BuildChunkSize $HighPressureBuildChunkSize | Out-String
    Write-Host $hpOut
    $csvMatch = [regex]::Match($hpOut, "csv=([^\r\n]+)")
    if ($csvMatch.Success) {
        $hpCsv = $csvMatch.Groups[1].Value.Trim()
        if (Test-Path -LiteralPath $hpCsv) {
            $hpRows = Import-Csv -Path $hpCsv
            if ($hpRows.Count -gt 0) {
                $maxRows = ($hpRows | Measure-Object -Property rows -Maximum).Maximum
                $buildRows = @($hpRows | Where-Object { $_.phase -eq "build" })
                $queryRows = @($hpRows | Where-Object { $_.phase -eq "query" -and [double]$_.rows -eq [double]$maxRows })
                $txnNormalRows = @($hpRows | Where-Object { $_.phase -eq "txn" -and $_.metric -eq "WALSYNC normal" -and [double]$_.rows -eq [double]$maxRows })
                if ($buildRows.Count -gt 0) {
                    $perf.hp_min_build_tps = ($buildRows | Measure-Object -Property tps -Minimum).Minimum
                    Write-Host ("[HP] build_min_tps={0}" -f $perf.hp_min_build_tps)
                }
                if ($queryRows.Count -gt 0) {
                    $perf.hp_max_query_avg_ms = ($queryRows | Measure-Object -Property avg_ms -Maximum).Maximum
                    $perf.hp_max_query_rows = $maxRows
                    Write-Host ("[HP] max_query_avg_ms={0}" -f $perf.hp_max_query_avg_ms)
                }
                if ($txnNormalRows.Count -gt 0) {
                    $perf.hp_normal_avg_txn_ms = ($txnNormalRows | Measure-Object -Property avg_ms -Maximum).Maximum
                    Write-Host ("[HP] normal_avg_txn_ms={0}" -f $perf.hp_normal_avg_txn_ms)
                }
                Add-TelemetryEvent -Phase "high_pressure" -Metrics @{
                    hp_max_query_avg_ms = $perf.hp_max_query_avg_ms
                    hp_max_query_rows = $perf.hp_max_query_rows
                    hp_min_build_tps = $perf.hp_min_build_tps
                    hp_normal_avg_txn_ms = $perf.hp_normal_avg_txn_ms
                }
            }
        } else {
            Write-Host "WARN: high-pressure csv file not found: $hpCsv"
        }
    } else {
        Write-Host "WARN: cannot locate high-pressure csv output path."
    }
    $hpDecodeMax = Parse-DecodeDeltaMax $hpOut
    Write-Host ("[HP] decode_delta_max={0}" -f $hpDecodeMax)
    if ($hpDecodeMax -gt $MaxDecodeDeltaAllowed) {
        throw "High-pressure decode delta too high: $hpDecodeMax > $MaxDecodeDeltaAllowed"
    }
}

if ($RunConcurrentPressure) {
    Write-Host "==> Perf: concurrent_pressure_bench.ps1 (repeat=$ConcurrentRepeatUntilFail)"
    $cpOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/concurrent_pressure_bench.ps1") `
        -BuildDir $BuildDir -Jobs $CtestJobs -RepeatUntilFail $ConcurrentRepeatUntilFail -RunRuntimeGate | Out-String
    Write-Host $cpOut
    if ($LASTEXITCODE -ne 0) {
        throw "concurrent_pressure_bench failed: exit_code=$LASTEXITCODE"
    }
    $cpSummaryPath = Parse-ConcurrentPressureSummaryPath $cpOut
    $cpSummary = Load-JsonFile $cpSummaryPath
    if ($cpSummary -and $cpSummary.runtime_gate_summary) {
        $perf.runtime_samples = $cpSummary.runtime_gate_summary.samples
        $perf.runtime_vacuum_efficiency_p50 = $cpSummary.runtime_gate_summary.vacuum_efficiency_p50
        $perf.runtime_conflict_rate_p95 = $cpSummary.runtime_gate_summary.conflict_rate_p95
        $perf.runtime_txn_begin_lock_conflict_delta = $cpSummary.runtime_gate_summary.txn_begin_lock_conflict_delta
        $perf.runtime_wal_compact_delta = $cpSummary.runtime_gate_summary.wal_compact_delta
        $perf.runtime_run_id = $cpSummary.runtime_run_id
        Add-TelemetryEvent -Phase "concurrent_pressure" -Metrics @{
            runtime_samples = $perf.runtime_samples
            runtime_vacuum_efficiency_p50 = $perf.runtime_vacuum_efficiency_p50
            runtime_conflict_rate_p95 = $perf.runtime_conflict_rate_p95
            runtime_txn_begin_lock_conflict_delta = $perf.runtime_txn_begin_lock_conflict_delta
            runtime_wal_compact_delta = $perf.runtime_wal_compact_delta
            runtime_run_id = $perf.runtime_run_id
            summary = $cpSummaryPath
        }
    }
}

if ($RunLockGate) {
    Write-Host "==> Gate: txn file lock"
    Exec "txn file lock gate" {
        ctest --test-dir "$BuildDir" --output-on-failure -R "TxnFileLock.SecondCoordinatorCannotAcquireSameTableLock"
    }
}

if ($RunRecoveryGate) {
    Write-Host "==> Gate: WAL recovery"
    Exec "wal recovery gate" {
        ctest --test-dir "$BuildDir" --output-on-failure -R "WalSegment.RotateAndRecoverAcrossSegments|WalManager.CheckpointTruncatesFileKeepsLsnMonotonic|DemoTxnWal.RecoveryIsIdempotentAcrossRestarts"
    }
}

if ($RunConcurrentMillion) {
    Write-Host "==> Perf: concurrent_million_scale_bench.ps1"
    $cmOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/concurrent_million_scale_bench.ps1") `
        -BuildDir $BuildDir -DataDir $DataDir -ThreadsCsv $ConcurrentMillionThreadsCsv `
        -TotalOps $ConcurrentMillionTotalOps | Out-String
    Write-Host $cmOut
    $cmCsvMatch = [regex]::Match($cmOut, "csv=([^\r\n]+)")
    if ($cmCsvMatch.Success) {
        $cmCsv = $cmCsvMatch.Groups[1].Value.Trim()
        if (Test-Path -LiteralPath $cmCsv) {
            $cmRows = @(Import-Csv -Path $cmCsv)
            if ($cmRows.Count -gt 0) {
                $perf.cm_tps_min = ($cmRows | Measure-Object -Property tps -Minimum).Minimum
                Add-TelemetryEvent -Phase "concurrent_million" -Metrics @{
                    cm_tps_min = $perf.cm_tps_min
                }
            }
        }
    }
}

if ($SoakMinutes -gt 0) {
    $repeat = [math]::Max(1, $SoakMinutes * 6)
    Write-Host ("==> Soak: concurrent_pressure_bench.ps1 (minutes={0}, repeat={1})" -f $SoakMinutes, $repeat)
    $soakOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/concurrent_pressure_bench.ps1") `
        -BuildDir $BuildDir -Jobs $CtestJobs -RepeatUntilFail $repeat | Out-String
    Write-Host $soakOut
    if ($LASTEXITCODE -ne 0) {
        throw "soak concurrent pressure failed: exit_code=$LASTEXITCODE"
    }
    $perf.soak_repeat = $repeat
    $perf.soak_summary = Parse-ConcurrentPressureSummaryPath $soakOut
    $soakSummary = Load-JsonFile $perf.soak_summary
    if ($soakSummary -and $soakSummary.runtime_gate_summary) {
        $perf.runtime_samples = $soakSummary.runtime_gate_summary.samples
        $perf.runtime_vacuum_efficiency_p50 = $soakSummary.runtime_gate_summary.vacuum_efficiency_p50
        $perf.runtime_conflict_rate_p95 = $soakSummary.runtime_gate_summary.conflict_rate_p95
        $perf.runtime_txn_begin_lock_conflict_delta = $soakSummary.runtime_gate_summary.txn_begin_lock_conflict_delta
        $perf.runtime_wal_compact_delta = $soakSummary.runtime_gate_summary.wal_compact_delta
        $perf.runtime_run_id = $soakSummary.runtime_run_id
    }
    Add-TelemetryEvent -Phase "soak" -Metrics @{
        soak_minutes = $SoakMinutes
        soak_repeat = $perf.soak_repeat
        runtime_samples = $perf.runtime_samples
        runtime_vacuum_efficiency_p50 = $perf.runtime_vacuum_efficiency_p50
        runtime_conflict_rate_p95 = $perf.runtime_conflict_rate_p95
        runtime_txn_begin_lock_conflict_delta = $perf.runtime_txn_begin_lock_conflict_delta
        runtime_wal_compact_delta = $perf.runtime_wal_compact_delta
    }
}

Write-Host "==> Perf summary"
$perf | Format-List
$summary = [ordered]@{
    schema_version = "newdb.perf_summary.v1"
    timestamp = (Get-Date).ToString("o")
    data_dir = $DataDir
    table = $Table
    perf = $perf
    eq_cache = [ordered]@{
        hits = $eqCacheStats.hits
        queries = $eqCacheStats.queries
    }
    soak = [ordered]@{
        minutes = $SoakMinutes
        repeat = $perf.soak_repeat
        summary = $perf.soak_summary
    }
    telemetry = [ordered]@{
        schema_version = "newdb.telemetry.v1"
        run_id = $runId
        events_path = $telemetryPath
    }
}
$summary | ConvertTo-Json -Depth 6 | Set-Content -Path $summaryPath
Write-Host ("==> Summary JSON: {0}" -f $summaryPath)
Write-Host ("==> Telemetry JSONL: {0}" -f $telemetryPath)
Add-TelemetryEvent -Phase "summary" -Metrics @{
    summary_path = $summaryPath
    trend_path = (Join-Path $resultDir "test_loop_trend.jsonl")
}
$trendPath = Join-Path $resultDir "test_loop_trend.jsonl"
([ordered]@{
    timestamp = (Get-Date).ToString("o")
    summary = $summaryPath
    txn_normal_avg_ms = $perf.txn_normal_avg_ms
    query_avg_ms_max = $perf.query_avg_ms_max
    where_policy_rejects = $perf.where_policy_rejects
    where_policy_fallbacks = $perf.where_policy_fallbacks
    runtime_samples = $perf.runtime_samples
    runtime_vacuum_efficiency_p50 = $perf.runtime_vacuum_efficiency_p50
    runtime_conflict_rate_p95 = $perf.runtime_conflict_rate_p95
    runtime_txn_begin_lock_conflict_delta = $perf.runtime_txn_begin_lock_conflict_delta
    runtime_wal_compact_delta = $perf.runtime_wal_compact_delta
    runtime_run_id = $perf.runtime_run_id
    cm_tps_min = $perf.cm_tps_min
    soak_repeat = $perf.soak_repeat
    soak_summary = $perf.soak_summary
} | ConvertTo-Json -Compress) | Add-Content -Path $trendPath
Write-Host ("==> Trend JSONL: {0}" -f $trendPath)

$nightlyTrendPath = Join-Path $resultDir "nightly_soak_trend.jsonl"
$dashboardPath = Join-Path $resultDir "runtime_trend_dashboard.json"
Invoke-PythonScript -ScriptPath (Join-Path $scriptsRoot "runtime_trend_rollup.py") -ScriptArgs @(
    "--test-loop-trend", $trendPath,
    "--nightly-trend", $nightlyTrendPath,
    "--output", $dashboardPath
)
Invoke-PythonScript -ScriptPath (Join-Path $scriptsBase "validate/validate_runtime_trend_dashboard.py") -ScriptArgs @($dashboardPath)
Write-Host ("==> Runtime trend dashboard: {0}" -f $dashboardPath)

if ($EnforcePerf) {
    $failed = $false
    if ($perf.txn_full_avg_ms -ne $null -and $perf.txn_full_avg_ms -gt $MaxTxnAvgMsFull) {
        Write-Host ("PERF FAIL: txn_full_avg_ms={0} > {1}" -f $perf.txn_full_avg_ms, $MaxTxnAvgMsFull)
        $failed = $true
    }
    if ($perf.txn_normal_avg_ms -ne $null -and $perf.txn_normal_avg_ms -gt $MaxTxnAvgMsNormal) {
        Write-Host ("PERF FAIL: txn_normal_avg_ms={0} > {1}" -f $perf.txn_normal_avg_ms, $MaxTxnAvgMsNormal)
        $failed = $true
    }
    if ($perf.query_avg_ms_max -ne $null -and $perf.query_avg_ms_max -gt $MaxQueryAvgMs) {
        Write-Host ("PERF FAIL: query_avg_ms_max={0} > {1}" -f $perf.query_avg_ms_max, $MaxQueryAvgMs)
        $failed = $true
    }
    if ($perf.vischk_delta_pct -ne $null -and $perf.vischk_delta_pct -gt $MaxVischkRegressionPct) {
        Write-Host ("PERF FAIL: vischk_delta_pct={0} > {1}" -f $perf.vischk_delta_pct, $MaxVischkRegressionPct)
        $failed = $true
    }
    if ($perf.hp_max_query_avg_ms -ne $null -and $perf.hp_max_query_avg_ms -gt $MaxHighPressureQueryAvgMs) {
        $hpRows = 0.0
        if ($perf.hp_max_query_rows -ne $null) {
            $hpRows = [double]$perf.hp_max_query_rows
        }
        $hpQueryGate = Resolve-HighPressureQueryGate $hpRows
        if ($perf.hp_max_query_avg_ms -gt $hpQueryGate) {
            Write-Host ("PERF FAIL: hp_max_query_avg_ms={0} > {1} (rows={2})" -f $perf.hp_max_query_avg_ms, $hpQueryGate, $perf.hp_max_query_rows)
            $failed = $true
        }
    }
    if ($perf.hp_normal_avg_txn_ms -ne $null -and $perf.hp_normal_avg_txn_ms -gt $MaxHighPressureTxnAvgMsNormal) {
        Write-Host ("PERF FAIL: hp_normal_avg_txn_ms={0} > {1}" -f $perf.hp_normal_avg_txn_ms, $MaxHighPressureTxnAvgMsNormal)
        $failed = $true
    }
    if ($perf.hp_min_build_tps -ne $null -and $perf.hp_min_build_tps -lt $MinHighPressureBuildTps) {
        Write-Host ("PERF FAIL: hp_min_build_tps={0} < {1}" -f $perf.hp_min_build_tps, $MinHighPressureBuildTps)
        $failed = $true
    }
    if ($perf.cm_tps_min -ne $null -and $perf.cm_tps_min -lt $MinConcurrentMillionTps) {
        Write-Host ("PERF FAIL: cm_tps_min={0} < {1}" -f $perf.cm_tps_min, $MinConcurrentMillionTps)
        $failed = $true
    }
    if ($perf.where_policy_rejects -gt $MaxWherePolicyRejects) {
        Write-Host ("PERF FAIL: where_policy_rejects={0} > {1}" -f $perf.where_policy_rejects, $MaxWherePolicyRejects)
        $failed = $true
    }
    if ($perf.where_policy_fallbacks -gt $MaxWhereFallbackScans) {
        Write-Host ("PERF FAIL: where_policy_fallbacks={0} > {1}" -f $perf.where_policy_fallbacks, $MaxWhereFallbackScans)
        $failed = $true
    }
    if ($eqCacheStats.queries -gt 0) {
        $hitRate = [double]$eqCacheStats.hits / [double]$eqCacheStats.queries
        Write-Host ("[EQ] hit_rate={0:P1}" -f $hitRate)
        if ($hitRate -lt $MinEqSidecarCacheHitRate) {
            Write-Host ("PERF FAIL: eq sidecar hit_rate={0:P1} < {1:P1}" -f $hitRate, $MinEqSidecarCacheHitRate)
            $failed = $true
        }
    }
    if ($failed) {
        exit 3
    }
}

Write-Host "All tests done."
