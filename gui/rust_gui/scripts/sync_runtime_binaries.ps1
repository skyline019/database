# Sync StructDB runtime artifacts from a CMake build tree into gui/rust_gui/src-tauri/bin.
# Visual Studio multi-config places outputs under e.g. build\src\c_api\RelWithDebInfo\ — not build\RelWithDebInfo\.
# This script picks one file per leaf by configuration priority (default: RelWithDebInfo).
#
# Examples:
#   powershell -File .\gui\rust_gui\scripts\sync_runtime_binaries.ps1 -BuildDir E:\db\StructDB\build
#   powershell -File .\gui\rust_gui\scripts\sync_runtime_binaries.ps1 -BuildDir E:\db\StructDB\build -Configuration Release

param(
    [Parameter(Mandatory = $true)]
    [string]$BuildDir,
    [ValidateSet("RelWithDebInfo", "Release", "Debug", "MinSizeRel")]
    [string]$Configuration = "RelWithDebInfo"
)

$ErrorActionPreference = "Stop"
$resolvedRoot = (Resolve-Path $BuildDir).Path
$dest = Join-Path $PSScriptRoot "..\src-tauri\bin"
New-Item -ItemType Directory -Force -Path $dest | Out-Null

function Rank-Path([string]$fullPath, [string]$preferred) {
    $p = $fullPath.Replace("/", "\")
    $marker = "\$preferred\"
    if ($p.Contains($marker)) {
        return 0
    }
    # Prefer any explicit config folder over unknown layout
    if ($p -match '\\(RelWithDebInfo|Release|Debug|MinSizeRel)\\') {
        return 10
    }
    return 20
}

function Find-Best-Under([string]$dir, [string]$leaf, [string]$preferredConfig) {
    if (-not (Test-Path -LiteralPath $dir)) {
        return $null
    }
    $all = @(Get-ChildItem -LiteralPath $dir -Recurse -File -Filter $leaf -ErrorAction SilentlyContinue)
    if ($all.Count -eq 0) {
        return $null
    }
    $ranked = $all | ForEach-Object {
        [PSCustomObject]@{
            Path = $_.FullName
            Rank = (Rank-Path $_.FullName $preferredConfig)
        }
    } | Sort-Object Rank, Path
    return $ranked[0].Path
}

function Copy-IfFound([string]$leaf) {
    $srcPath = Find-Best-Under $resolvedRoot $leaf $Configuration
    if (-not $srcPath) {
        Write-Warning "sync: not found under ${resolvedRoot}: $leaf (Configuration=$Configuration)"
        return
    }
    $dstPath = Join-Path $dest $leaf
    Copy-Item -LiteralPath $srcPath -Destination $dstPath -Force
    Write-Host "sync: $leaf <= $srcPath"
}

Write-Host "sync: root=$resolvedRoot preferred_config=$Configuration -> $dest"
Copy-IfFound "structdb_capi_shared.dll"
Copy-IfFound "structdb_app.exe"

Write-Host "sync: done -> $dest"
