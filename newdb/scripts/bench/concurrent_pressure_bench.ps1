param(
    [string]$BuildDir = "build_mingw",
    [int]$Jobs = 8,
    [int]$RepeatUntilFail = 30,
    [string]$RuntimeStatsJsonl = "",
    [switch]$AppendRuntimeJsonl = $false,
    [switch]$RunRuntimeGate = $false,
    [int]$RuntimePressureBatches = 12,
    [int]$RuntimePressureBatchSize = 40,
    [double]$MinVacuumEfficiency = -1.0,
    [double]$MaxConflictRate = -1.0
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
        [int]$BatchSize
    )

    if ($Batches -le 0 -or $BatchSize -le 0) {
        throw "runtime pressure batches/batch-size must be > 0"
    }

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
    if ($matched.Count -lt 2) {
        throw "need at least 2 SHOW TUNING JSON lines in $log"
    }
    $first = $matched[0].Line
    $last = $matched[$matched.Count - 1].Line
    $ts0 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $ts1 = $ts0 + 1
    $jsonBefore = "{""ts_ms"":$ts0,""label"":""pressure_before"",""stats"":" + $first + "}"
    $jsonAfter = "{""ts_ms"":$ts1,""label"":""pressure_after"",""stats"":" + $last + "}"
    Add-Content -Path $OutJsonlPath -Value $jsonBefore
    Add-Content -Path $OutJsonlPath -Value $jsonAfter

    # Script-level regression guard: ensure the expected before/after labels exist.
    $beforeCnt = @(Select-String -Path $OutJsonlPath -Pattern '"label":"pressure_before"').Count
    $afterCnt = @(Select-String -Path $OutJsonlPath -Pattern '"label":"pressure_after"').Count
    if ($beforeCnt -lt 1 -or $afterCnt -lt 1) {
        throw "runtime snapshot jsonl missing pressure_before/pressure_after labels"
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
$runtimeWs = Join-Path $env:TEMP ("newdb_runtime_capture_" + [Guid]::NewGuid().ToString("N"))
New-Item -Path $runtimeWs -ItemType Directory | Out-Null
try {
    Invoke-RuntimePressureAndCapture `
        -DemoExe $demoExe `
        -WorkspaceDir $runtimeWs `
        -OutJsonlPath $runtimeJsonlPath `
        -Batches $RuntimePressureBatches `
        -BatchSize $RuntimePressureBatchSize

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
    $reportCmd = @("--input", $runtimeJsonlPath, "--last-n", "2")
    if ($MinVacuumEfficiency -ge 0.0) {
        $reportCmd += @("--min-vacuum-efficiency", "$MinVacuumEfficiency")
    }
    if ($MaxConflictRate -ge 0.0) {
        $reportCmd += @("--max-conflict-rate", "$MaxConflictRate")
    }
    & $reportExe @reportCmd
    if ($LASTEXITCODE -ne 0) {
        throw "runtime gate failed: exit_code=$LASTEXITCODE"
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
     min_vacuum_efficiency = $MinVacuumEfficiency
     max_conflict_rate = $MaxConflictRate
     status = "passed"
 } | ConvertTo-Json -Depth 5 | Set-Content -Path $summaryPath
 Write-Host ("Concurrent pressure summary: {0}" -f $summaryPath)

Write-Host "Concurrent pressure done."
