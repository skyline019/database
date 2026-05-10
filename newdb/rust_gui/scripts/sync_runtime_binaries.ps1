# Syncs CMake-built EXEs/DLLs into `src-tauri/bin` for Tauri bundle + dev.
# Optional: when the tree was built with NEWDB_C_API_PLUGIN_BACKEND + NEWDB_BUILD_CLI_BACKEND_PLUGIN,
# copies `newdb_cli_backend.dll` (from `newdb_cli_backend.dll` or MinGW `libnewdb_cli_backend.dll`) so the
# GUI can auto-set NEWDB_CLI_BACKEND_PATH beside `libnewdb.dll`.
param(
    # Default: typical MinGW tree (`newdb/build-mingw`). Plugin CI preset often uses `..\build-clean-plugin`.
    [string]$BuildDir = "..\build-mingw",
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
$BuildDir = [System.IO.Path]::GetFullPath($BuildDir)
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

function Resolve-BuildArtifactPath {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string[]]$LeafNames
    )
    $configDirs = @("", "bin", "Release", "RelWithDebInfo", "Debug", "MinSizeRel")
    foreach ($leaf in $LeafNames) {
        foreach ($sub in $configDirs) {
            $p = if ($sub) { Join-Path $Root (Join-Path $sub $leaf) } else { Join-Path $Root $leaf }
            try {
                $full = [System.IO.Path]::GetFullPath($p)
            } catch {
                continue
            }
            if (Test-Path -LiteralPath $full) {
                return $full
            }
        }
    }
    return $null
}
New-Item -Path $OutDir -ItemType Directory -Force | Out-Null
New-Item -Path $ScriptsOutDir -ItemType Directory -Force | Out-Null
New-Item -Path $GuiScriptsDir -ItemType Directory -Force | Out-Null
$OutDir = [System.IO.Path]::GetFullPath($OutDir)
$ScriptsOutDir = [System.IO.Path]::GetFullPath($ScriptsOutDir)
$GuiScriptsDir = [System.IO.Path]::GetFullPath($GuiScriptsDir)

$required = @(
    @{ Dest = "newdb_demo.exe"; Leaves = @("newdb_demo.exe") },
    @{ Dest = "newdb_perf.exe"; Leaves = @("newdb_perf.exe") },
    @{ Dest = "newdb_runtime_report.exe"; Leaves = @("newdb_runtime_report.exe") },
    @{ Dest = "libnewdb.dll"; Leaves = @("libnewdb.dll", "newdb.dll") },
    @{ Dest = "libgtest_capi.dll"; Leaves = @("libgtest_capi.dll", "gtest_capi.dll") }
)

foreach ($entry in $required) {
    $destName = $entry.Dest
    $src = Resolve-BuildArtifactPath -Root $BuildDir -LeafNames $entry.Leaves
    if (-not $src) {
        throw "missing required runtime artifact '${destName}' under $BuildDir (tried leaf names: $($entry.Leaves -join ', '); subdirs: build root, bin, Release, RelWithDebInfo, Debug, MinSizeRel)"
    }
    Copy-Item -LiteralPath $src -Destination (Join-Path $OutDir $destName) -Force
    if ((Split-Path -Leaf $src) -ine $destName) {
        Write-Host ("[SYNC] {0} <= {1}" -f $destName, $src)
    } else {
        Write-Host ("[SYNC] {0}" -f $destName)
    }
}

# When GoogleTest is built as DLLs, copy peer DLLs from gtest_capi's build directory first, then build root / bin.
$capiBuilt = Resolve-BuildArtifactPath -Root $BuildDir -LeafNames @("libgtest_capi.dll", "gtest_capi.dll")
$peerSearchDirs = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
if ($capiBuilt) {
    [void]$peerSearchDirs.Add([System.IO.Path]::GetDirectoryName($capiBuilt))
}
[void]$peerSearchDirs.Add($BuildDir)
[void]$peerSearchDirs.Add((Join-Path $BuildDir "bin"))
$optionalPeerDlls = @(
    "libgtest.dll",
    "libgtest_main.dll",
    "libgmock.dll",
    "libgmock_main.dll",
    "gtest.dll",
    "gtest_main.dll",
    "gmock.dll",
    "gmock_main.dll"
)
foreach ($name in $optionalPeerDlls) {
    $src = $null
    foreach ($d in $peerSearchDirs) {
        if (-not $d -or -not (Test-Path -LiteralPath $d)) { continue }
        $p = Join-Path $d $name
        if (Test-Path -LiteralPath $p) {
            $src = $p
            break
        }
    }
    if (-not $src) {
        $src = Resolve-BuildArtifactPath -Root $BuildDir -LeafNames @($name)
    }
    if ($src) {
        Copy-Item -LiteralPath $src -Destination (Join-Path $OutDir $name) -Force
        Write-Host ("[SYNC] {0} (peer test DLL)" -f $name)
    }
}

$optional = @(
    "libgcc_s_seh-1.dll",
    "libwinpthread-1.dll",
    "libstdc++-6.dll",
    "libssp-0.dll"
)
foreach ($name in $optional) {
    $src = Resolve-BuildArtifactPath -Root $BuildDir -LeafNames @($name)
    if ($src) {
        Copy-Item -LiteralPath $src -Destination (Join-Path $OutDir $name) -Force
        Write-Host ("[SYNC] {0}" -f $name)
    } else {
        Write-Host ("[SYNC][WARN] optional MinGW runtime not under build tree: {0} (OK if fully static-linked)" -f $name)
    }
}

$cliPlugin = Resolve-BuildArtifactPath -Root $BuildDir -LeafNames @("newdb_cli_backend.dll", "libnewdb_cli_backend.dll")
if ($cliPlugin) {
    $destCli = Join-Path $OutDir "newdb_cli_backend.dll"
    Copy-Item -LiteralPath $cliPlugin -Destination $destCli -Force
    Write-Host ("[SYNC] newdb_cli_backend.dll <= {0}" -f $cliPlugin)
} else {
    Write-Host "[SYNC][SKIP] newdb_cli_backend.dll (plugin backend) not in build tree — OK for default full_embed builds"
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
