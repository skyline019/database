param(
    [string]$BuildDir = "../build",
    [string]$BundleDir = "../build/bundle"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
$buildPath = (Resolve-Path (Join-Path $scriptDir $BuildDir)).Path

$bundlePath = Join-Path $scriptDir $BundleDir
New-Item -ItemType Directory -Force -Path $bundlePath | Out-Null
$bundlePath = (Resolve-Path $bundlePath).Path

$cache = Join-Path $buildPath "CMakeCache.txt"
$mingwBin = $null
if (Test-Path $cache) {
    $cc = Select-String -Path $cache -Pattern "^CMAKE_CXX_COMPILER:FILEPATH=(.+)$" | Select-Object -First 1
    if ($cc) {
        $compiler = $cc.Matches[0].Groups[1].Value
        $maybeBin = Split-Path -Parent $compiler
        if (Test-Path $maybeBin) { $mingwBin = $maybeBin }
    }
}

$dllNames = @(
    "libgtest_capi.dll",
    "gtest_capi.dll",
    "libgtest_capi_samples.dll",
    "gtest_capi_samples.dll",
    "libgtest.dll",
    "gtest.dll",
    "libgtest_main.dll",
    "gtest_main.dll",
    "libstdc++-6.dll",
    "libgcc_s_seh-1.dll",
    "libwinpthread-1.dll"
)

foreach ($name in $dllNames) {
    $candidates = @(
        (Join-Path $buildPath $name),
        (Join-Path (Join-Path $buildPath "Debug") $name),
        (Join-Path (Join-Path $buildPath "Release") $name),
        (Join-Path (Join-Path $buildPath "bin") $name)
    )
    $candidates += @(
        (Join-Path (Join-Path (Join-Path $buildPath "bin") "Debug") $name),
        (Join-Path (Join-Path (Join-Path $buildPath "bin") "Release") $name)
    )
    if ($mingwBin) {
        $candidates += (Join-Path $mingwBin $name)
    }
    $src = $null
    foreach ($c in $candidates) {
        if (Test-Path $c) { $src = $c; break }
    }
    if ($src) {
        Copy-Item -LiteralPath $src -Destination (Join-Path $bundlePath $name) -Force
        Write-Host "[BUNDLE] $name <= $src"
    }
}

Write-Host "[DONE] bundle dir: $bundlePath"
