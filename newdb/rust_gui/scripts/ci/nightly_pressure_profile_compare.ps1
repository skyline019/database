param(
    [string]$BuildDir = "build_nightly",
    [int]$Jobs = 8,
    [int]$RuntimePressureBatches = 16,
    [int]$RuntimePressureBatchSize = 500,
    [int]$RuntimeSampleEveryBatches = 2,
    [double]$MinLeveldbOverInnodbTpsRatio = 1.05,
    [double]$MaxInnodbOverLeveldbP95Ratio = 1.15,
    [int]$MaxHybridModeFlips = 3
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptsRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptsRoot)
$benchScript = Join-Path $projectRoot "scripts/bench/concurrent_pressure_bench.ps1"
$redoUndoGate = Join-Path $projectRoot "scripts/ci/redo_undo_recovery_gate.ps1"
$mergeCrashMatrixScript = Join-Path $projectRoot "scripts/soak/merge_crash_matrix_into_dashboard.py"
$dashboardPath = Join-Path $projectRoot "scripts/results/runtime_trend_dashboard.json"
$resultDir = Join-Path $projectRoot "scripts/results"
if (-not (Test-Path $resultDir)) {
    New-Item -Path $resultDir -ItemType Directory | Out-Null
}

function Invoke-PythonScript {
    param(
        [string]$ScriptPath,
        [string[]]$ArgsList
    )
    $candidates = @(
        @("python3"),
        @("py", "-3"),
        @("python")
    )
    foreach ($c in $candidates) {
        $cmd = $c[0]
        $prefix = @()
        if ($c.Length -gt 1) {
            $prefix = $c[1..($c.Length - 1)]
        }
        & $cmd @prefix $ScriptPath @ArgsList
        if ($LASTEXITCODE -eq 0) {
            return
        }
    }
    throw "failed to execute python script: $ScriptPath"
}

function Resolve-RedoUndoCrashMatrixPath([string]$text, [string]$searchDir) {
    if (-not [string]::IsNullOrWhiteSpace($text)) {
        foreach ($ln in ($text -split "`r?`n")) {
            if ($ln -match "REDO_UNDO_CRASH_MATRIX_JSON\s+(.+)$") {
                $p = $matches[1].Trim()
                if (Test-Path -LiteralPath $p) {
                    return $p
                }
            }
        }
    }
    $cand = Get-ChildItem -Path $searchDir -Filter "redo_undo_crash_matrix_*.json" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($cand) {
        return $cand.FullName
    }
    return $null
}

function Exec-Step {
    param(
        [string]$Name,
        [scriptblock]$Body
    )
    Write-Host "==> $Name"
    & $Body
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed: exit_code=$LASTEXITCODE"
    }
}

function Run-Profile {
    param(
        [string]$Profile,
        [string]$WalMode,
        [int]$WalNormalMs
    )
    $before = @(
        Get-ChildItem -Path $resultDir -Filter "concurrent_pressure_summary_*.json" -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName
    )
    $out = (& powershell -ExecutionPolicy Bypass -File $benchScript `
        -BuildDir $BuildDir `
        -Jobs $Jobs `
        -RepeatUntilFail 1 `
        -RuntimePressureBatches $RuntimePressureBatches `
        -RuntimePressureBatchSize $RuntimePressureBatchSize `
        -RuntimeSampleEveryBatches $RuntimeSampleEveryBatches `
        -RuntimeProgressEveryBatches 1 `
        -RuntimeProgressEveryRows 100 `
        -RuntimeLsmSegmentTargetBytes 256 `
        -RuntimeSidecarInvalidateEveryN 16 `
        -RuntimeSidecarInvalidateAsync `
        -RuntimeQuietSessionLog `
        -RuntimeUseBulkInsertFast `
        -RuntimeLsmCompactionAsync `
        -RuntimeLsmCompactionWorkers 2 `
        -RuntimeLsmCompactionReapBudget 4 `
        -RuntimeLsmL0CompactTrigger 8 `
        -RuntimeLsmL0CompactBatch 12 `
        -RuntimeLsmFlushTriggerMultiplier 2 `
        -BenchmarkProfile $Profile `
        -RuntimeWalSyncMode $WalMode `
        -RuntimeWalSyncNormalIntervalMs $WalNormalMs | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "concurrent pressure failed for profile=$Profile"
    }
    if (-not [string]::IsNullOrWhiteSpace($out)) {
        foreach ($ln in ($out -split "`r?`n")) {
            if ($ln -match "Concurrent pressure summary:\s+(.+)$") {
                $summaryPath = $matches[1].Trim()
                if (Test-Path -LiteralPath $summaryPath) {
                    try {
                        $summaryObj = Get-Content -Path $summaryPath -Raw | ConvertFrom-Json
                        if ($summaryObj.benchmark_profile -eq $Profile) {
                            return $summaryPath
                        }
                    } catch {
                    }
                }
            }
        }
    }
    $after = Get-ChildItem -Path $resultDir -Filter "concurrent_pressure_summary_*.json" |
        Sort-Object LastWriteTime -Descending
    foreach ($f in $after) {
        if ($before -notcontains $f.FullName) {
            try {
                $summaryObj = Get-Content -Path $f.FullName -Raw | ConvertFrom-Json
                if ($summaryObj.benchmark_profile -eq $Profile) {
                    return $f.FullName
                }
            } catch {
            }
        }
    }
    throw "cannot locate new summary for profile=$Profile"
}

$levelSummary = Run-Profile -Profile "leveldb-like" -WalMode "normal" -WalNormalMs 20
$innodbSummary = Run-Profile -Profile "innodb-like" -WalMode "full" -WalNormalMs 20
$hybridSummary = Run-Profile -Profile "hybrid-balanced" -WalMode "normal" -WalNormalMs 20

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$compareOut = Join-Path $resultDir ("pressure_profile_compare_" + $stamp + ".json")
$cmpPy = Join-Path $scriptsRoot "pressure_profile_compare.py"
Invoke-PythonScript -ScriptPath $cmpPy -ArgsList @(
    "--leveldb-summary", $levelSummary,
    "--innodb-summary", $innodbSummary,
    "--hybrid-summary", $hybridSummary,
    "--output", $compareOut,
    "--min-tps-ratio", "$MinLeveldbOverInnodbTpsRatio",
    "--max-p95-ratio", "$MaxInnodbOverLeveldbP95Ratio",
    "--max-hybrid-flips", "$MaxHybridModeFlips"
)
$cmp = Get-Content -Path $compareOut -Raw | ConvertFrom-Json
if ($cmp.verdict -ne "ok") {
    throw "pressure profile compare failed: verdict=$($cmp.verdict)"
}

Write-Host ("PRESSURE_PROFILE_COMPARE_JSON {0}" -f $compareOut)

$gateOut = ""
Exec-Step "redo/undo crash matrix gate" {
    $gateOut = (& powershell -ExecutionPolicy Bypass -File $redoUndoGate -BuildDir $BuildDir | Out-String)
    Write-Host $gateOut
}

$crashMatrixPath = Resolve-RedoUndoCrashMatrixPath -text $gateOut -searchDir $resultDir
if (-not $crashMatrixPath) {
    throw "cannot locate redo_undo_crash_matrix json"
}
if (Test-Path -LiteralPath $dashboardPath) {
    Invoke-PythonScript -ScriptPath $mergeCrashMatrixScript -ArgsList @(
        "--dashboard", $dashboardPath,
        "--crash-matrix", $crashMatrixPath
    )
    Write-Host ("RUNTIME_TREND_DASHBOARD_UPDATED_WITH_CRASH_MATRIX {0}" -f $dashboardPath)
} else {
    Write-Host ("runtime_trend_dashboard.json not found, skip merge: {0}" -f $dashboardPath)
}
