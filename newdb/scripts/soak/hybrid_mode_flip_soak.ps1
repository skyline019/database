param(
    [string]$BuildDir = "build_mingw",
    [int]$Rounds = 100
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptsRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}

$testsExe = Join-Path $BuildDir "newdb_tests.exe"
if (-not (Test-Path -LiteralPath $testsExe)) {
    throw "newdb_tests.exe not found: $testsExe"
}

# Keep script param and gtest behavior aligned.
[Environment]::SetEnvironmentVariable("NEWDB_HYBRID_SOAK_ROUNDS", [string]$Rounds, "Process")
try {
    & $testsExe "--gtest_filter=DemoTxnWal.HybridAdaptiveAlternatingSignalsAreCappedByDwellWindow"
    if ($LASTEXITCODE -ne 0) {
        throw "hybrid mode flip soak gtest failed: exit_code=$LASTEXITCODE"
    }
} finally {
    [Environment]::SetEnvironmentVariable("NEWDB_HYBRID_SOAK_ROUNDS", $null, "Process")
}

Write-Host ("HYBRID_MODE_FLIP_SOAK: PASSED rounds={0}" -f $Rounds)
