param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_eqinv_bench",
    [int]$Rows = 900,
    [int]$Loops = 40
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

if ($Rows -lt 100) {
    throw "Rows must be >= 100 for meaningful signal."
}
if ($Loops -lt 10) {
    throw "Loops must be >= 10 for stable averages."
}

$suffix = [guid]::NewGuid().ToString("N")
$prepScript = Join-Path $env:TEMP ("eq_sidecar_invalidation_prep_" + $suffix + ".mdb")
$benchUnrelated = Join-Path $env:TEMP ("eq_sidecar_invalidation_unrelated_" + $suffix + ".mdb")
$benchRelated = Join-Path $env:TEMP ("eq_sidecar_invalidation_related_" + $suffix + ".mdb")
$logPath = Join-Path $env:TEMP ("eq_sidecar_invalidation_bench_" + $suffix + ".log")

$prep = New-Object System.Collections.Generic.List[string]
$prep.Add("DROP TABLE($Table)")
$prep.Add("CREATE TABLE($Table)")
$prep.Add("USE($Table)")
$prep.Add("DEFATTR(name:string,dept:string,age:int,salary:int)")
for ($i = 1; $i -le $Rows; $i++) {
    $dept = "D$($i.ToString('D4'))"
    $age = 20 + ($i % 30)
    $salary = 8000 + ($i % 5000)
    $prep.Add("INSERT($i,u$i,$dept,$age,$salary)")
}
$prep | Set-Content -Path $prepScript

function Reset-BenchTable {
    & $DemoExe --data-dir $DataDir --table $Table --run-mdb $prepScript | Out-Null
}

function Run-Scenario([string]$kind, [string]$scriptPath) {
    $cmds = New-Object System.Collections.Generic.List[string]
    $cmds.Add("USE($Table)")
    for ($i = 1; $i -le $Loops; $i++) {
        $id = (($i - 1) % $Rows) + 1
        $queryId = ((($i - 1 + 400) % $Rows) + 1)
        if ($queryId -le $Loops) {
            $queryId = $queryId + $Loops
            if ($queryId -gt $Rows) {
                $queryId = $Rows
            }
        }
        $qv = "D$($queryId.ToString('D4'))"
        if ($kind -eq "unrelated") {
            $cmds.Add("SETATTR($id,age,$(50 + ($i % 40)))")
        } else {
            $cmds.Add("SETATTR($id,dept,X$($i.ToString('D4')))")
        }
        $cmds.Add("WHERE(dept,=,$qv)")
    }
    $cmds | Set-Content -Path $scriptPath
    & $DemoExe --data-dir $DataDir --table $Table --run-mdb $scriptPath 2>&1
}

Reset-BenchTable
$outUnrelated = Run-Scenario -kind "unrelated" -scriptPath $benchUnrelated
Reset-BenchTable
$outRelated = Run-Scenario -kind "related" -scriptPath $benchRelated

($outUnrelated + $outRelated) | Set-Content -Path $logPath

function Extract-EqSidecarAvgUs([object[]]$lines) {
    $samples = @()
    foreach ($line in $lines) {
        $text = [string]$line
        if ($text -notmatch "mode=eq_sidecar .*elapsed_us=(\d+)") { continue }
        $g = [regex]::Match($text, "elapsed_us=(\d+)")
        if ($g.Success) {
            $samples += [double]$g.Groups[1].Value
        }
    }
    if ($samples.Count -eq 0) {
        return [pscustomobject]@{ Avg = 0.0; Count = 0 }
    }
    $avg = ($samples | Measure-Object -Average).Average
    return [pscustomobject]@{ Avg = $avg; Count = $samples.Count }
}

$u = Extract-EqSidecarAvgUs -lines $outUnrelated
$r = Extract-EqSidecarAvgUs -lines $outRelated
if ($u.Count -eq 0 -or $r.Count -eq 0) {
    throw "No eq_sidecar trace records captured for one or both scenarios."
}
$deltaPct = (($r.Avg - $u.Avg) / [math]::Max($u.Avg, 1.0)) * 100.0

Write-Host "eq_sidecar invalidation bench:"
Write-Host ("  table={0} rows={1} loops={2}" -f $Table, $Rows, $Loops)
Write-Host ("  unrelated_avg_eq_sidecar_us={0} (samples={1})" -f [math]::Round($u.Avg, 3), $u.Count)
Write-Host ("  related_avg_eq_sidecar_us={0} (samples={1})" -f [math]::Round($r.Avg, 3), $r.Count)
Write-Host ("  related_vs_unrelated_delta_pct={0}" -f [math]::Round($deltaPct, 2))
Write-Host ("  log={0}" -f $logPath)

Remove-Item -Force $prepScript
Remove-Item -Force $benchUnrelated
Remove-Item -Force $benchRelated
