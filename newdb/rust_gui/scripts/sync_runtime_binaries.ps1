param(
    [string]$BuildDir = "..\build_mingw",
    [string]$OutDir = ".\src-tauri\bin",
    [string]$ScriptsOutDir = ".\src-tauri\resources\scripts",
    [string]$GuiScriptsDir = ".\scripts"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not [System.IO.Path]::IsPathRooted($BuildDir)) {
    $BuildDir = Join-Path $repoRoot $BuildDir
}
if (-not [System.IO.Path]::IsPathRooted($OutDir)) {
    $OutDir = Join-Path $repoRoot $OutDir
}
if (-not [System.IO.Path]::IsPathRooted($ScriptsOutDir)) {
    $ScriptsOutDir = Join-Path $repoRoot $ScriptsOutDir
}
if (-not [System.IO.Path]::IsPathRooted($GuiScriptsDir)) {
    $GuiScriptsDir = Join-Path $repoRoot $GuiScriptsDir
}
if (-not (Test-Path -LiteralPath $BuildDir)) {
    throw "BuildDir not found: $BuildDir"
}
New-Item -Path $OutDir -ItemType Directory -Force | Out-Null
New-Item -Path $ScriptsOutDir -ItemType Directory -Force | Out-Null
New-Item -Path $GuiScriptsDir -ItemType Directory -Force | Out-Null

$required = @(
    "newdb_demo.exe",
    "newdb_perf.exe",
    "newdb_runtime_report.exe",
    "libnewdb.dll"
)

foreach ($name in $required) {
    if ($name -ieq "libnewdb.dll") {
        $dllCandidates = @(
            (Join-Path $BuildDir "libnewdb.dll"),
            (Join-Path $BuildDir "newdb.dll")
        )
        $dllSrc = $dllCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
        if (-not $dllSrc) {
            throw ("missing required runtime artifact: {0}" -f ($dllCandidates -join " | "))
        }
        Copy-Item -LiteralPath $dllSrc -Destination (Join-Path $OutDir "libnewdb.dll") -Force
        Write-Host ("[SYNC] {0} <= {1}" -f "libnewdb.dll", (Split-Path -Leaf $dllSrc))
        continue
    }
    $src = Join-Path $BuildDir $name
    if (-not (Test-Path -LiteralPath $src)) {
        throw "missing required runtime artifact: $src"
    }
    Copy-Item -LiteralPath $src -Destination (Join-Path $OutDir $name) -Force
    Write-Host ("[SYNC] {0}" -f $name)
}

$optional = @(
    "libgcc_s_seh-1.dll",
    "libwinpthread-1.dll",
    "libstdc++-6.dll"
)
foreach ($name in $optional) {
    $src = Join-Path $BuildDir $name
    if (Test-Path -LiteralPath $src) {
        Copy-Item -LiteralPath $src -Destination (Join-Path $OutDir $name) -Force
        Write-Host ("[SYNC] {0}" -f $name)
    } else {
        Write-Host ("[SYNC][WARN] optional artifact not found: {0}" -f $src)
    }
}

Write-Host ("[SYNC] output dir: {0}" -f $OutDir)

$sourceScriptsRoot = Join-Path $repoRoot "..\scripts"
if (-not (Test-Path -LiteralPath $sourceScriptsRoot)) {
    throw "Scripts root not found: $sourceScriptsRoot"
}
$sourceScriptsRoot = (Resolve-Path -LiteralPath $sourceScriptsRoot).Path

function Test-SkipScriptRelativePath {
    param([string]$RelativePath)
    $norm = $RelativePath -replace "\\", "/"
    return $norm.StartsWith("results/") -or
        $norm.Contains("/results/") -or
        $norm.StartsWith("__pycache__/") -or
        $norm.Contains("/__pycache__/") -or
        $norm.EndsWith(".pyc")
}

function Remove-MirroredGeneratedDirs {
    param([string]$RootDir)
    if (-not (Test-Path -LiteralPath $RootDir)) {
        return
    }
    Get-ChildItem -Path $RootDir -Directory -Recurse -Force -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -in @("results", "__pycache__") } |
        ForEach-Object {
            Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
            Write-Host ("[SYNC][CLEAN] {0}" -f $_.FullName)
        }
}

Remove-MirroredGeneratedDirs -RootDir $GuiScriptsDir
Remove-MirroredGeneratedDirs -RootDir $ScriptsOutDir

Get-ChildItem -Path $sourceScriptsRoot -Recurse -File | ForEach-Object {
    $src = $_.FullName
    $relative = $src.Substring($sourceScriptsRoot.Length).TrimStart('\', '/')
    if (Test-SkipScriptRelativePath -RelativePath $relative) {
        return
    }
    foreach ($targetRoot in @($GuiScriptsDir, $ScriptsOutDir)) {
        $dst = Join-Path $targetRoot $relative
        $dstParent = Split-Path -Parent $dst
        New-Item -Path $dstParent -ItemType Directory -Force | Out-Null
        Copy-Item -LiteralPath $src -Destination $dst -Force
    }
    Write-Host ("[SYNC][SCRIPT] {0}" -f $relative)
}

$resultsDir = Join-Path $ScriptsOutDir "results"
New-Item -Path $resultsDir -ItemType Directory -Force | Out-Null
$sourceResultsDir = Join-Path $sourceScriptsRoot "results"

$seedDashboard = Join-Path $resultsDir "runtime_trend_dashboard.json"
$sourceDashboard = Join-Path $sourceResultsDir "runtime_trend_dashboard.json"
if (Test-Path -LiteralPath $sourceDashboard) {
    Copy-Item -LiteralPath $sourceDashboard -Destination $seedDashboard -Force
    Write-Host "[SYNC][RESULTS] copied runtime_trend_dashboard.json"
} elseif (-not (Test-Path -LiteralPath $seedDashboard)) {
    @"
{
  "schema_version": "newdb.runtime_trend_dashboard.v1",
  "generated_at": "",
  "overview": {
    "total_rows": 0,
    "latest_test_loop_timestamp": null,
    "latest_nightly_timestamp": null,
    "latest_runtime_run_id": null
  },
  "sources": {
    "test_loop_trend_path": "",
    "nightly_soak_trend_path": "",
    "test_loop_rows": 0,
    "nightly_rows": 0
  },
  "nightly_status": {
    "total": 0,
    "passed": 0,
    "failed": 0,
    "pass_rate": null
  },
  "data_quality": {
    "has_nightly_samples": false,
    "latest_nightly_age_hours": null
  },
  "secondary_metrics": {
    "dashboard_quality_gate_passed_count": 0,
    "dashboard_quality_gate_failed_count": 0
  },
  "runtime_metrics": {
    "vacuum_efficiency_p50": {"count": 0, "min": null, "max": null, "avg": null},
    "conflict_rate_p95": {"count": 0, "min": null, "max": null, "avg": null},
    "txn_begin_lock_conflict_delta": {"count": 0, "min": null, "max": null, "avg": null},
    "wal_compact_delta": {"count": 0, "min": null, "max": null, "avg": null}
  },
  "perf_metrics": {
    "txn_normal_avg_ms": {"count": 0, "min": null, "max": null, "avg": null},
    "query_avg_ms_max": {"count": 0, "min": null, "max": null, "avg": null},
    "cm_tps_min": {"count": 0, "min": null, "max": null, "avg": null},
    "hp_max_query_avg_ms": {"count": 0, "min": null, "max": null, "avg": null}
  },
  "health": {
    "tier": "healthy",
    "reasons": [],
    "latest_query_avg_ms": null,
    "latest_cm_tps_min": null,
    "latest_hp_max_query_avg_ms": null,
    "latest_txn_normal_avg_ms": null
  },
  "recent_runs": []
}
"@ | Set-Content -LiteralPath $seedDashboard -Encoding UTF8
    Write-Host "[SYNC][RESULTS] seeded runtime_trend_dashboard.json"
}

$jsonlSeeds = @(
    "test_loop_trend.jsonl",
    "nightly_soak_trend.jsonl"
)
foreach ($file in $jsonlSeeds) {
    $path = Join-Path $resultsDir $file
    $srcPath = Join-Path $sourceResultsDir $file
    if (Test-Path -LiteralPath $srcPath) {
        Copy-Item -LiteralPath $srcPath -Destination $path -Force
        Write-Host ("[SYNC][RESULTS] copied {0}" -f $file)
    } elseif (-not (Test-Path -LiteralPath $path)) {
        New-Item -Path $path -ItemType File | Out-Null
        Write-Host ("[SYNC][RESULTS] seeded {0}" -f $file)
    }
}

Write-Host ("[SYNC] scripts output dir: {0}" -f $ScriptsOutDir)
Write-Host ("[SYNC] gui scripts dir: {0}" -f $GuiScriptsDir)
