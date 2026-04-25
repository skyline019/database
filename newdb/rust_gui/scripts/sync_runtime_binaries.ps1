param(
    [string]$BuildDir = "..\build_mingw",
    [string]$OutDir = ".\src-tauri\bin"
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
if (-not (Test-Path -LiteralPath $BuildDir)) {
    throw "BuildDir not found: $BuildDir"
}
New-Item -Path $OutDir -ItemType Directory -Force | Out-Null

$required = @(
    "newdb_demo.exe",
    "newdb_perf.exe",
    "newdb_runtime_report.exe",
    "libnewdb.dll"
)

foreach ($name in $required) {
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
