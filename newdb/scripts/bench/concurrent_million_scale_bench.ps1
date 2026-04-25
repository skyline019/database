param(
    [string]$BuildDir = "build_mingw",
    [string]$DataDir = "e:\temp\newdb_test_isolation",
    [int[]]$ThreadsSet = @(8, 16, 32),
    [string]$ThreadsCsv = "",
    [int]$TotalOps = 1000000,
    [UInt64]$SegmentBytes = 8388608
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}

$exe = Join-Path $BuildDir "newdb_concurrent_perf.exe"
if (-not (Test-Path $exe)) {
    throw "newdb_concurrent_perf.exe not found: $exe"
}
if (-not [string]::IsNullOrWhiteSpace($ThreadsCsv)) {
    $ThreadsSet = @($ThreadsCsv.Split(",") | ForEach-Object { [int]($_.Trim()) } | Where-Object { $_ -gt 0 })
}
if ($ThreadsSet.Count -eq 0) {
    throw "ThreadsSet is empty; pass -ThreadsCsv like '8,16,32' or non-empty -ThreadsSet"
}
$baseDir = $DataDir
if (-not (Test-Path $baseDir)) {
    New-Item -Path $baseDir -ItemType Directory | Out-Null
}
$leafName = [System.IO.Path]::GetFileName($baseDir.TrimEnd('\','/'))
if ($leafName -match "^run_\d{8}_\d{6}$") {
    $DataDir = $baseDir
} else {
    $runStamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $DataDir = Join-Path $baseDir ("run_" + $runStamp)
    New-Item -Path $DataDir -ItemType Directory -Force | Out-Null
}
Write-Host ("Concurrent million bench data dir: {0}" -f $DataDir)

$outDir = Join-Path $PSScriptRoot "results"
if (-not (Test-Path $outDir)) { New-Item -Path $outDir -ItemType Directory | Out-Null }
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$csv = Join-Path $outDir ("concurrent_million_bench_" + $stamp + ".csv")
$json = Join-Path $outDir ("concurrent_million_bench_" + $stamp + ".json")

$rows = @()
Write-Host ("Concurrent million bench start: ops={0} threads={1}" -f $TotalOps, ($ThreadsSet -join ","))
foreach ($th in $ThreadsSet) {
    $db = "conc_million_${stamp}_t${th}"
    $wal = Join-Path $DataDir ($db + ".wal")
    Get-ChildItem -Path $DataDir -Filter ($db + ".wal*") -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
    $out = & $exe --data-dir $DataDir --db-name $db --threads $th --total-ops $TotalOps --segment-bytes $SegmentBytes 2>&1 | Out-String
    Write-Host $out
    if ($LASTEXITCODE -ne 0) {
        throw "concurrent perf failed for threads=$th exit=$LASTEXITCODE"
    }
    $elapsed = 0.0
    $tps = 0.0
    $walBytes = 0.0
    foreach ($ln in ($out -split "`r?`n")) {
        if ($ln -match "^\s*elapsed_ms=(\d+(\.\d+)?)\s*$") { $elapsed = [double]$matches[1] }
        if ($ln -match "^\s*tps=(\d+(\.\d+)?)\s*$") { $tps = [double]$matches[1] }
        if ($ln -match "^\s*wal_bytes=(\d+(\.\d+)?)\s*$") { $walBytes = [double]$matches[1] }
    }
    $rows += [pscustomobject]@{
        threads = $th
        total_ops = $TotalOps
        elapsed_ms = $elapsed
        tps = $tps
        wal_bytes = $walBytes
    }
}

$rows | Export-Csv -Path $csv -NoTypeInformation -Encoding UTF8
$rows | ConvertTo-Json -Depth 5 | Set-Content -Path $json
Write-Host ("Concurrent million bench done. csv={0}" -f $csv)
Write-Host ("Concurrent million bench done. json={0}" -f $json)
