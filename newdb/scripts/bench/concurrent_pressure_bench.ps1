param(
    [string]$BuildDir = "build_mingw",
    [int]$Jobs = 8,
    [int]$RepeatUntilFail = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}

Write-Host "Concurrent pressure start: repeat=$RepeatUntilFail jobs=$Jobs"

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
     status = "passed"
 } | ConvertTo-Json -Depth 5 | Set-Content -Path $summaryPath
 Write-Host ("Concurrent pressure summary: {0}" -f $summaryPath)

Write-Host "Concurrent pressure done."
