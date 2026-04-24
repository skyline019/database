param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_regression",
    [int]$Loops = 20,
    [int]$WarmupLoops = 5
)

$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($env:OS -eq "Windows_NT") { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path (Join-Path $projectRoot "build_mingw") $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}

# Keep tracing opt-in to avoid skewing perf gate measurements.
if (-not $env:NEWDB_QUERY_TRACE) {
    $env:NEWDB_QUERY_TRACE = "0"
}
if (-not $env:NEWDB_LAZY_HEAP) {
    $env:NEWDB_LAZY_HEAP = "0"
}

$commands = @(
    "WHERE(dept,=,ENG,AND,age,>,20)",
    "COUNT(dept,=,ENG,AND,age,>,20)",
    "AVG(salary,WHERE,dept,=,ENG,AND,age,>,20)"
)

function Invoke-DemoAndCollectPolicy {
    param(
        [string]$ScriptPath
    )
    $out = & $DemoExe --data-dir $DataDir --table $Table --run-mdb $ScriptPath 2>&1 | Out-String
    $rejectCount = ([regex]::Matches($out, "\[WHERE_POLICY\] reject")).Count
    $fallbackCount = ([regex]::Matches($out, "\[WHERE_POLICY\] fallback")).Count
    return [ordered]@{
        output = $out
        reject_count = $rejectCount
        fallback_count = $fallbackCount
    }
}

Write-Host "Query bench start: loops=$Loops warmup=$WarmupLoops table=$Table"
$totalReject = 0
$totalFallback = 0
foreach ($cmd in $commands) {
    $warmupScript = Join-Path $env:TEMP ("query_bench_warmup_" + [guid]::NewGuid().ToString("N") + ".mdb")
    $warmupLines = New-Object System.Collections.Generic.List[string]
    for ($w = 0; $w -lt $WarmupLoops; $w++) { $warmupLines.Add($cmd) }
    $warmupLines | Set-Content -Path $warmupScript
    $null = Invoke-DemoAndCollectPolicy -ScriptPath $warmupScript
    Remove-Item -Force $warmupScript

    $benchScript = Join-Path $env:TEMP ("query_bench_run_" + [guid]::NewGuid().ToString("N") + ".mdb")
    $benchLines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $Loops; $i++) { $benchLines.Add($cmd) }
    $benchLines | Set-Content -Path $benchScript
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $policyStats = Invoke-DemoAndCollectPolicy -ScriptPath $benchScript
    $sw.Stop()
    Remove-Item -Force $benchScript
    $avgMs = [math]::Round($sw.Elapsed.TotalMilliseconds / [math]::Max($Loops, 1), 3)
    $totalReject += [int]$policyStats.reject_count
    $totalFallback += [int]$policyStats.fallback_count
    Write-Host ("CMD={0}`n  total_ms={1}`n  avg_ms={2}" -f $cmd, [math]::Round($sw.Elapsed.TotalMilliseconds, 3), $avgMs)
}
Write-Host ("WHERE_POLICY_SUMMARY reject_count={0} fallback_count={1}" -f $totalReject, $totalFallback)
Write-Host "Query bench done."
