param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_regression",
    [int]$PassQueries = 60
)

$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($env:OS -eq "Windows_NT") { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path (Join-Path $projectRoot "build_mingw") $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}

$env:NEWDB_QUERY_TRACE = "1"

if ($PassQueries -lt 10) {
    throw "PassQueries must be >= 10"
}

$suffix = [guid]::NewGuid().ToString("N")
$scriptPath = Join-Path $env:TEMP ("eq_sidecar_cache_bench_" + $suffix + ".mdb")
$logPath = Join-Path $env:TEMP ("eq_sidecar_cache_bench_" + $suffix + ".log")

$lines = New-Object System.Collections.Generic.List[string]
for ($i = 0; $i -lt $PassQueries; $i++) {
    $v = "A$($i.ToString('D4'))"
    $lines.Add("WHERE(dept,=,$v)")
}
for ($i = 0; $i -lt $PassQueries; $i++) {
    $v = "B$($i.ToString('D4'))"
    $lines.Add("WHERE(dept,=,$v)")
}
$lines | Set-Content -Path $scriptPath

$sw = [System.Diagnostics.Stopwatch]::StartNew()
& $DemoExe --data-dir $DataDir --table $Table --run-mdb $scriptPath 2>&1 | Set-Content -Path $logPath
$sw.Stop()

$trace = Select-String -Path $logPath -Pattern "mode=eq_sidecar .*elapsed_us=(\d+)"
$elapsedUs = @()
foreach ($m in $trace) {
    $g = [regex]::Match($m.Line, "elapsed_us=(\d+)")
    if ($g.Success) {
        $elapsedUs += [double]$g.Groups[1].Value
    }
}

$take = [math]::Min($PassQueries, [math]::Floor($elapsedUs.Count / 2))
if ($take -le 0) {
    throw "No eq_sidecar trace records captured. Check NEWDB_QUERY_TRACE and query path."
}

$first = $elapsedUs[0..($take - 1)]
$second = $elapsedUs[$take..($take * 2 - 1)]
$firstAvg = ($first | Measure-Object -Average).Average
$secondAvg = ($second | Measure-Object -Average).Average
$improve = 0.0
if ($firstAvg -gt 0) {
    $improve = (($firstAvg - $secondAvg) / $firstAvg) * 100.0
}

Write-Host "eq_sidecar cache bench:"
Write-Host ("  table={0} pass_queries={1}" -f $Table, $PassQueries)
Write-Host ("  process_total_ms={0}" -f [math]::Round($sw.Elapsed.TotalMilliseconds, 3))
Write-Host ("  eq_sidecar_samples={0}" -f $elapsedUs.Count)
Write-Host ("  first_pass_avg_us={0}" -f [math]::Round($firstAvg, 3))
Write-Host ("  second_pass_avg_us={0}" -f [math]::Round($secondAvg, 3))
Write-Host ("  pass2_improvement_pct={0}" -f [math]::Round($improve, 2))
Write-Host ("  log={0}" -f $logPath)

Remove-Item -Force $scriptPath
