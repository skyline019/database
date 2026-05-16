# Configure StructDB (optional), sync capi/app into src-tauri/bin, then npm build + tauri build.
param(
    [switch]$SkipCMake,
    [string]$SyncBuildDir = "",
    [string]$CmakeSourceDir = "",
    [string]$CmakeBinaryDir = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$sync = Join-Path $PSScriptRoot "sync_runtime_binaries.ps1"

if (-not $SkipCMake) {
    $src = if ($CmakeSourceDir) { $CmakeSourceDir } else { $repoRoot }
    $bin = if ($CmakeBinaryDir) { $CmakeBinaryDir } else { Join-Path $repoRoot "build" }
    cmake "-S" $src "-B" $bin "-DSTRUCTDB_BUILD_CAPI_SHARED=ON"
    cmake "--build" $bin "--config" "RelWithDebInfo" "--target" "structdb_capi_shared" "structdb_app"
    # VS: binaries under build\<proj>\RelWithDebInfo\ — sync from whole tree with config priority.
    & $sync -BuildDir $bin -Configuration "RelWithDebInfo"
}
else {
    if ([string]::IsNullOrWhiteSpace($SyncBuildDir)) {
        throw "With -SkipCMake, pass -SyncBuildDir <CMake build root, e.g. E:\db\StructDB\build>"
    }
    & $sync -BuildDir $SyncBuildDir -Configuration "RelWithDebInfo"
}

Push-Location (Join-Path $PSScriptRoot "..")
try {
    npm run build
    npx --yes tauri build
}
finally {
    Pop-Location
}
