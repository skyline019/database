param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [string]$Table = "qa_regression",
    [int]$Txns = 80
)

$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($env:OS -eq "Windows_NT") { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path (Join-Path $projectRoot "build_mingw") $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}

$modes = @("full", "normal", "off")

function Run-TxnBench([string]$mode, [int]$txns) {
    $scriptPath = Join-Path $env:TEMP ("txn_mode_bulk_" + $mode + "_" + [guid]::NewGuid().ToString("N") + ".mdb")
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("WALSYNC $mode")
    $baseId = [int](Get-Date -UFormat %s)
    $baseId = $baseId * 1000
    if ($mode -eq "normal") { $baseId += 200000000 }
    elseif ($mode -eq "off") { $baseId += 400000000 }
    for ($i = 0; $i -lt $txns; $i++) {
        $id = $baseId + $i
        $lines.Add("BEGIN")
        $lines.Add("INSERT($id,bench_$mode`_$i,ENG,30,10000)")
        $lines.Add("COMMIT")
    }
    $lines | Set-Content -Path $scriptPath

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $DemoExe --data-dir $DataDir --table $Table --run-mdb $scriptPath | Out-Null
    $sw.Stop()
    Remove-Item -Force $scriptPath
    $totalMs = [math]::Round($sw.Elapsed.TotalMilliseconds, 3)
    $avgMs = [math]::Round($sw.Elapsed.TotalMilliseconds / [math]::Max($txns, 1), 3)
    $tps = [math]::Round(($txns * 1000.0) / [math]::Max($sw.Elapsed.TotalMilliseconds, 1), 3)
    return [pscustomobject]@{
        Mode = $mode
        Txns = $txns
        TotalMs = $totalMs
        AvgTxnMs = $avgMs
        TPS = $tps
    }
}

Write-Host "Txn write bench start: txns=$Txns table=$Table"
$results = @()
foreach ($mode in $modes) {
    $results += Run-TxnBench -mode $mode -txns $Txns
}

Write-Host "Txn write bench results:"
$results | Format-Table -AutoSize
Write-Host "Txn write bench done."
