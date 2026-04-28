param(
    [switch]$Quiet
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$tauriDir = Join-Path $repoRoot "src-tauri"

Set-Location $tauriDir
if (-not $Quiet) {
    Write-Host "==> cargo run -- --e2e-undo-redo-check"
}
cargo run -- --e2e-undo-redo-check
if ($LASTEXITCODE -ne 0) {
    throw "e2e undo/redo check failed: exit=$LASTEXITCODE"
}
if (-not $Quiet) {
    Write-Host "e2e undo/redo check passed"
}

