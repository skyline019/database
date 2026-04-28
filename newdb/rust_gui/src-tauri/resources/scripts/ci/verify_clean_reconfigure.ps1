param(
    [string]$BuildDir = "build_ci_clean",
    [string]$BuildType = "RelWithDebInfo",
    [string]$BuildConfig = "RelWithDebInfo",
    [string]$Generator = "MinGW Makefiles",
    [int]$ConfigureRetries = 3,
    [int]$RetrySleepSeconds = 5,
    [switch]$ReleaseGrade,
    [switch]$SkipGuiGate,
    [switch]$SkipSemanticGate,
    [switch]$SkipBenchGate
)

# Usage quick reference:
# - Fast local iteration (skip optional heavy gates as needed):
#   powershell -ExecutionPolicy Bypass -File scripts/ci/verify_clean_reconfigure.ps1 -BuildDir build_ci_local -SkipBenchGate
# - Release-grade verification (all gates required; skip flags forbidden):
#   powershell -ExecutionPolicy Bypass -File scripts/ci/verify_clean_reconfigure.ps1 -ReleaseGrade -BuildDir build_ci_release

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$buildPath = Join-Path $repoRoot $BuildDir
$isMultiConfig = $Generator -match "Visual Studio|Xcode|Multi-Config"

function Invoke-PythonScript {
    param(
        [string]$ScriptPath,
        [string[]]$ScriptArgs
    )
    $pythonCandidates = @(
        @("python3"),
        @("py", "-3"),
        @("python")
    )
    foreach ($candidate in $pythonCandidates) {
        $cmd = $candidate[0]
        $prefix = @()
        if ($candidate.Length -gt 1) {
            $prefix = $candidate[1..($candidate.Length - 1)]
        }
        & $cmd @prefix $ScriptPath @ScriptArgs
        if ($LASTEXITCODE -eq 0) {
            return
        }
    }
    throw "failed to execute python script: $ScriptPath"
}

Write-Host "[verify_clean_reconfigure] repo: $repoRoot"
Write-Host "[verify_clean_reconfigure] build dir: $buildPath"
if ($ReleaseGrade -and ($SkipSemanticGate -or $SkipGuiGate -or $SkipBenchGate)) {
    throw "ReleaseGrade mode forbids SkipSemanticGate/SkipGuiGate/SkipBenchGate."
}
if ($SkipSemanticGate -or $SkipGuiGate -or $SkipBenchGate) {
    Write-Warning "[verify_clean_reconfigure] one or more gates are skipped; this run is NOT release-grade."
} else {
    Write-Host "[verify_clean_reconfigure] full gate mode enabled (release-grade verification)"
}

if (Test-Path -LiteralPath $buildPath) {
    Write-Host "[verify_clean_reconfigure] removing existing build dir"
    Remove-Item -LiteralPath $buildPath -Recurse -Force
}

if ($ConfigureRetries -lt 1) {
    throw "ConfigureRetries must be >= 1"
}

$configured = $false
for ($attempt = 1; $attempt -le $ConfigureRetries; $attempt++) {
    Write-Host "[verify_clean_reconfigure] configuring from clean directory (attempt $attempt/$ConfigureRetries)"
    $configureArgs = @("-S", $repoRoot, "-B", $buildPath, "-G", $Generator, "-DCMAKE_CXX_SCAN_FOR_MODULES=OFF")
    if (-not $isMultiConfig) {
        $configureArgs += "-DCMAKE_BUILD_TYPE=$BuildType"
    }
    & cmake @configureArgs
    if ($LASTEXITCODE -eq 0) {
        $configured = $true
        break
    }
    if ($attempt -lt $ConfigureRetries) {
        Write-Warning "[verify_clean_reconfigure] configure failed (exit=$LASTEXITCODE), retrying in $RetrySleepSeconds sec"
        Start-Sleep -Seconds $RetrySleepSeconds
    }
}
if (-not $configured) { throw "cmake configure failed after $ConfigureRetries attempt(s)" }

Write-Host "[verify_clean_reconfigure] building"
if ($isMultiConfig) {
    & cmake --build $buildPath --config $BuildConfig --parallel
} else {
    & cmake --build $buildPath --parallel
}
if ($LASTEXITCODE -ne 0) { throw "cmake build failed: $LASTEXITCODE" }

Write-Host "[verify_clean_reconfigure] running ctest"
if ($isMultiConfig) {
    & ctest --test-dir $buildPath --build-config $BuildConfig --output-on-failure
} else {
    & ctest --test-dir $buildPath --output-on-failure
}
if ($LASTEXITCODE -ne 0) { throw "ctest failed: $LASTEXITCODE" }

if (-not $SkipSemanticGate) {
    Write-Host "[verify_clean_reconfigure] running semantic gate (C API + MDB mismatch contracts)"
    $semanticFilter = "CApi.*MismatchReturnsExecutionFailedWithCapiPrefix:DemoMdb.*Mismatch*StopsScriptAndKeepsPreviousData"
    & (Join-Path $buildPath "newdb_tests.exe") "--gtest_filter=$semanticFilter"
    if ($LASTEXITCODE -ne 0) { throw "semantic gate failed: $LASTEXITCODE" }
}

if (-not $SkipGuiGate) {
    $guiRoot = Join-Path $repoRoot "rust_gui"
    $tauriRoot = Join-Path $guiRoot "src-tauri"
    if ((Test-Path -LiteralPath $guiRoot) -and (Test-Path -LiteralPath $tauriRoot)) {
        Write-Host "[verify_clean_reconfigure] running rust_gui gates (cargo test --lib + npm test + npm run build)"
        Push-Location $tauriRoot
        try {
            & cargo test --lib
            if ($LASTEXITCODE -ne 0) { throw "cargo test --lib failed: $LASTEXITCODE" }
        } finally {
            Pop-Location
        }

        Push-Location $guiRoot
        try {
            if (-not (Test-Path -LiteralPath (Join-Path $guiRoot "node_modules"))) {
                Write-Host "[verify_clean_reconfigure] node_modules missing, running npm ci"
                & npm ci
                if ($LASTEXITCODE -ne 0) { throw "npm ci failed: $LASTEXITCODE" }
            }
            & npm test
            if ($LASTEXITCODE -ne 0) { throw "npm test failed: $LASTEXITCODE" }
            & npm run build
            if ($LASTEXITCODE -ne 0) { throw "npm run build failed: $LASTEXITCODE" }
        } finally {
            Pop-Location
        }
    } else {
        Write-Warning "[verify_clean_reconfigure] rust_gui paths missing; skip GUI gate"
    }
}

if (-not $SkipBenchGate) {
    Write-Host "[verify_clean_reconfigure] running bench gate"
    $benchArgs = @($BuildDir)
    if ($isMultiConfig) {
        $benchArgs += @("--build-config", $BuildConfig)
    }
    Invoke-PythonScript -ScriptPath (Join-Path $repoRoot "scripts/ci/ci_bench_gate.py") -ScriptArgs $benchArgs
}

Write-Host "[verify_clean_reconfigure] success"
