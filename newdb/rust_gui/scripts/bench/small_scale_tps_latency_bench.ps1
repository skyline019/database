param(
    [string]$DemoExe = "",
    [string]$DataDir = "e:\temp\newdb_small_tps_latency",
    [string]$SizesCsv = "1000,5000,10000",
    [int]$QueryWarmupLoops = 1,
    [int]$QueryLoops = 5,
    [int]$TxnPerMode = 30,
    [int]$BuildChunkSize = 1000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
$millionBench = Join-Path $scriptsRoot "million_scale_bench.ps1"
if (-not (Test-Path $millionBench)) {
    throw "million_scale_bench.ps1 not found: $millionBench"
}

if ([string]::IsNullOrWhiteSpace($DemoExe)) {
    $exeName = if ($env:OS -eq "Windows_NT") { "newdb_demo.exe" } else { "newdb_demo" }
    $DemoExe = Join-Path (Join-Path $projectRoot "build_mingw") $exeName
} elseif (-not [System.IO.Path]::IsPathRooted($DemoExe)) {
    $DemoExe = Join-Path $projectRoot $DemoExe
}
if (-not (Test-Path $DemoExe)) {
    throw "DemoExe not found: $DemoExe"
}

if (-not (Test-Path $DataDir)) {
    New-Item -Path $DataDir -ItemType Directory -Force | Out-Null
}

Write-Host ("Small-scale TPS/latency bench start. sizes={0}" -f $SizesCsv)
$benchOut = & powershell -ExecutionPolicy Bypass -File $millionBench `
    -DemoExe $DemoExe `
    -DataDir $DataDir `
    -SizesCsv $SizesCsv `
    -QueryWarmupLoops $QueryWarmupLoops `
    -QueryLoops $QueryLoops `
    -TxnPerMode $TxnPerMode `
    -BuildChunkSize $BuildChunkSize 2>&1 | Out-String

Write-Host $benchOut
if ($LASTEXITCODE -ne 0) {
    throw "million_scale_bench failed: exit_code=$LASTEXITCODE"
}

$summaryLine = $null
foreach ($ln in ($benchOut -split "`r?`n")) {
    if ($ln -like "NEWDB_PERF_SUMMARY *") {
        $summaryLine = $ln.Substring("NEWDB_PERF_SUMMARY ".Length)
        break
    }
}
if ([string]::IsNullOrWhiteSpace($summaryLine)) {
    throw "failed to parse NEWDB_PERF_SUMMARY from million_scale_bench output"
}
$summaryObj = $summaryLine | ConvertFrom-Json

$csvPath = [string]$summaryObj.result_csv
if (-not [System.IO.Path]::IsPathRooted($csvPath)) {
    $csvPath = Join-Path $projectRoot $csvPath
}
if (-not (Test-Path $csvPath)) {
    throw "result csv not found: $csvPath"
}

$rows = @(Import-Csv -Path $csvPath)
$queryRows = @($rows | Where-Object { $_.phase -eq "query" })
$txnRows = @($rows | Where-Object { $_.phase -eq "txn" })

function Get-Percentile([double[]]$arr, [double]$p) {
    if ($arr.Count -eq 0) { return 0.0 }
    $sorted = @($arr | Sort-Object)
    $rank = [math]::Ceiling($p * $sorted.Count)
    $idx = [Math]::Min([Math]::Max($rank - 1, 0), $sorted.Count - 1)
    return [double]$sorted[$idx]
}

$queryAvgMs = @($queryRows | ForEach-Object { [double]$_.avg_ms })
$txnAvgMs = @($txnRows | ForEach-Object { [double]$_.avg_ms })
$txnTps = @($txnRows | ForEach-Object { [double]$_.tps })

$result = [ordered]@{
    schema_version = "newdb.small_tps_latency.v1"
    timestamp = (Get-Date).ToString("o")
    sizes_csv = $SizesCsv
    query_loops = $QueryLoops
    txn_per_mode = $TxnPerMode
    query_avg_ms_p50 = [math]::Round((Get-Percentile -arr $queryAvgMs -p 0.5), 3)
    query_avg_ms_p95 = [math]::Round((Get-Percentile -arr $queryAvgMs -p 0.95), 3)
    txn_avg_ms_p50 = [math]::Round((Get-Percentile -arr $txnAvgMs -p 0.5), 3)
    txn_avg_ms_p95 = [math]::Round((Get-Percentile -arr $txnAvgMs -p 0.95), 3)
    txn_tps_avg = [math]::Round((($txnTps | Measure-Object -Average).Average), 3)
    upstream_summary = $summaryObj
    source_csv = $csvPath
}

$outDir = Join-Path $scriptsRoot "results"
if (-not (Test-Path $outDir)) {
    New-Item -Path $outDir -ItemType Directory | Out-Null
}
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outJson = Join-Path $outDir ("small_tps_latency_bench_" + $stamp + ".json")
$result | ConvertTo-Json -Depth 8 | Set-Content -Path $outJson

Write-Host ("Small-scale TPS/latency bench done. json={0}" -f $outJson)
Write-Host ("NEWDB_SMALL_TPS_LATENCY " + ($result | ConvertTo-Json -Depth 8 -Compress))
