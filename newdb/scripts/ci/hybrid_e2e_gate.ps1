param(
    [string]$BuildDir = "build_mingw",
    [int]$Jobs = 8,
    [switch]$SkipPressure,
    [int]$PressureRepeat = 1,
    [string]$PressureProfile = "newdb-default"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptsRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
$scriptsBase = Split-Path -Parent $scriptsRoot

if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $projectRoot $BuildDir
}

function Resolve-NewdbTestsExe {
    param([string]$RootBuildDir)
    $direct = Join-Path $RootBuildDir "newdb_tests.exe"
    if (Test-Path -LiteralPath $direct) {
        return $direct
    }
    foreach ($cfg in @("RelWithDebInfo", "Release", "Debug", "MinSizeRel")) {
        $candidate = Join-Path (Join-Path $RootBuildDir $cfg) "newdb_tests.exe"
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    throw "newdb_tests.exe not found under $RootBuildDir (expected flat or MSVC RelWithDebInfo/Release/Debug/MinSizeRel subdir)"
}

$newdbTestsExe = Resolve-NewdbTestsExe $BuildDir

function Exec([string]$label, [scriptblock]$cmd) {
    Write-Host "==> $label"
    & $cmd
    if ($LASTEXITCODE -ne 0) {
        throw "$label failed: exit_code=$LASTEXITCODE"
    }
}

Exec "Build" { cmake --build "$BuildDir" -j $Jobs }

Exec "Focused gates (DemoLsmLite/Txn/Wal)" {
    & $newdbTestsExe --gtest_filter="DemoLsmLite.*:Txn*:*Wal*"
}

Exec "ctest full" {
    ctest --test-dir "$BuildDir" --output-on-failure -j $Jobs
}

if (-not $SkipPressure) {
    $pressureOut = ""
    Exec "concurrent pressure + runtime gate" {
        $pressureOut = & powershell -ExecutionPolicy Bypass -File (Join-Path $scriptsBase "bench/concurrent_pressure_bench.ps1") `
            -BuildDir $BuildDir -Jobs $Jobs -RepeatUntilFail $PressureRepeat -RunRuntimeGate `
            -BenchmarkProfile $PressureProfile `
            -MinLsmCompactionBytesAmpEfficiency 0 | Out-String
        Write-Host $pressureOut
    }

    $summaryLine = $null
    foreach ($line in ($pressureOut -split "`r?`n")) {
        if ($line -match "Concurrent pressure summary:\s+(.+)$") {
            $summaryLine = $matches[1].Trim()
        }
        if (-not $summaryLine -and $line.Contains("Concurrent pressure summary:")) {
            $summaryLine = ($line.Split(":", 2)[1]).Trim()
        }
    }
    if (-not $summaryLine) {
        $candidate = Get-ChildItem -Path (Join-Path $scriptsBase "bench/results") -Filter "concurrent_pressure_summary_*.json" |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if ($candidate) {
            $summaryLine = $candidate.FullName
        }
    }
    if (-not $summaryLine) {
        throw "cannot resolve concurrent pressure summary path from output"
    }
    if (-not (Test-Path -LiteralPath $summaryLine)) {
        throw "concurrent pressure summary not found: $summaryLine"
    }

    $summary = Get-Content -Path $summaryLine -Raw | ConvertFrom-Json
    if ($summary.status -ne "passed") {
        throw "runtime gate summary status is not passed: $($summary.status)"
    }
    Write-Host "runtime gate summary passed"

    $runtimeRawJsonl = $null
    if ($summary.PSObject.Properties.Name -contains "runtime_stats_raw_jsonl") {
        $runtimeRawJsonl = [string]$summary.runtime_stats_raw_jsonl
    } elseif ($summary.PSObject.Properties.Name -contains "runtime_stats_jsonl") {
        $runtimeRawJsonl = [string]$summary.runtime_stats_jsonl
        Write-Host "legacy-format fallback: runtime_stats_jsonl"
    }
    if (-not $runtimeRawJsonl) {
        throw "runtime_stats_raw_jsonl missing in summary: $summaryLine"
    }
    if (-not [System.IO.Path]::IsPathRooted($runtimeRawJsonl)) {
        $runtimeRawJsonl = Join-Path $projectRoot $runtimeRawJsonl
    }
    if (-not (Test-Path -LiteralPath $runtimeRawJsonl)) {
        throw "runtime raw jsonl not found: $runtimeRawJsonl"
    }
    Exec "runtime stats schema validate" {
        python (Join-Path $scriptsBase "validate/validate_runtime_stats.py") $runtimeRawJsonl
    }
}

Write-Host "HYBRID_E2E_GATE: PASSED"

