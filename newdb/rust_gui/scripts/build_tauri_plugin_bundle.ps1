# Plugin C API 发行顺序：CMake（plugin 开关）-> sync_runtime_binaries.ps1 -> npm run tauri:build
#Requires -Version 5.1
param(
    # newdb 源码根（含 CMakeLists.txt）；默认为本脚本上两级（rust_gui/scripts -> rust_gui -> newdb）
    [string]$NewdbSourceDir = "",
    # MSVC 多配置生成目录（cmake -B）；默认 <NewdbSourceDir>/build_tauri_plugin_vs
    [string]$CmakeBinaryDir = "",
    [ValidateSet("RelWithDebInfo", "Release", "Debug", "MinSizeRel")]
    [string]$Configuration = "RelWithDebInfo",
    # 已自行完成 CMake 构建时指定产物根（MSVC 下为 .../build/RelWithDebInfo，MinGW/Ninja 常为单根目录），仅执行 sync + tauri
    [switch]$SkipCMake,
    # 与 -SkipCMake 配合：显式传给 sync 的 BuildDir（未指定时对 MSVC 使用 <CmakeBinaryDir>/<Configuration>）
    [string]$SyncBuildDir = "",
    [switch]$SkipTauri
)

# No Set-StrictMode here: Windows PowerShell 5.1 can treat unpassed [string] defaults oddly with Latest.
$ErrorActionPreference = "Stop"

$rustGuiRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if (-not $NewdbSourceDir) {
    $NewdbSourceDir = [System.IO.Path]::GetFullPath((Join-Path $rustGuiRoot ".."))
}
else {
    $NewdbSourceDir = [System.IO.Path]::GetFullPath($NewdbSourceDir)
}
if (-not (Test-Path -LiteralPath (Join-Path $NewdbSourceDir "CMakeLists.txt"))) {
    throw "NewdbSourceDir does not look like newdb root (missing CMakeLists.txt): $NewdbSourceDir"
}

if (-not $CmakeBinaryDir) {
    $CmakeBinaryDir = Join-Path $NewdbSourceDir "build_tauri_plugin_vs"
}
else {
    $CmakeBinaryDir = [System.IO.Path]::GetFullPath($CmakeBinaryDir)
}

$syncScript = Join-Path $rustGuiRoot "scripts\sync_runtime_binaries.ps1"
$outBin = Join-Path $rustGuiRoot "src-tauri\bin"
$scriptsOut = Join-Path $rustGuiRoot "src-tauri\resources\scripts"
$guiScripts = Join-Path $rustGuiRoot "scripts"

if (-not $SkipCMake) {
    if ($env:OS -ne "Windows_NT") {
        throw "Non-Windows: use cmake --preset plugin-shared-rel, build targets gtest_capi newdb_cli_backend newdb_shared newdb_demo newdb_perf newdb_runtime_report, then run this script with -SkipCMake -SyncBuildDir pointing at the CMake artifact root."
    }
    $cmakeArgs = @(
        "-S", $NewdbSourceDir,
        "-B", $CmakeBinaryDir,
        "-G", "Visual Studio 17 2022",
        "-A", "x64",
        "-DNEWDB_BUILD_TESTS=ON",
        "-DNEWDB_BUILD_GUI=OFF",
        "-DNEWDB_BUILD_SHARED=ON",
        "-DNEWDB_C_API_PLUGIN_BACKEND=ON",
        "-DNEWDB_BUILD_CLI_BACKEND_PLUGIN=ON"
    )
    & cmake @cmakeArgs
    if ($LASTEXITCODE -ne 0) { throw "cmake configure failed: $LASTEXITCODE" }
    $buildArgs = @(
        "--build", $CmakeBinaryDir,
        "--config", $Configuration,
        "--target", "gtest_capi", "newdb_cli_backend", "newdb_shared", "newdb_demo", "newdb_perf", "newdb_runtime_report"
    )
    & cmake @buildArgs
    if ($LASTEXITCODE -ne 0) { throw "cmake build failed: $LASTEXITCODE" }
}

if ($SyncBuildDir) {
    $bd = [System.IO.Path]::GetFullPath($SyncBuildDir)
}
elseif ($SkipCMake) {
    throw "-SkipCMake requires -SyncBuildDir (directory passed to sync_runtime_binaries.ps1 -BuildDir)."
}
else {
    $bd = Join-Path $CmakeBinaryDir $Configuration
}

if (-not (Test-Path -LiteralPath $bd)) {
    throw "Sync build dir not found: $bd"
}

& $syncScript -BuildDir $bd -OutDir $outBin -ScriptsOutDir $scriptsOut -GuiScriptsDir $guiScripts
if ($LASTEXITCODE -ne 0) { throw "sync_runtime_binaries.ps1 failed: $LASTEXITCODE" }

$backendDll = Join-Path $outBin "newdb_cli_backend.dll"
if (-not (Test-Path -LiteralPath $backendDll)) {
    throw ("After sync, missing required file: $backendDll (build with NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON).")
}

if (-not $SkipTauri) {
    Push-Location $rustGuiRoot
    try {
        & npm run tauri:build
        if ($LASTEXITCODE -ne 0) { throw ('npm run tauri:build failed: ' + $LASTEXITCODE) }
    }
    finally {
        Pop-Location
    }
}

Write-Host "[build_tauri_plugin_bundle] done (SkipTauri=$SkipTauri)"
