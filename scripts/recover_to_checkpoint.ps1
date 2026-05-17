#requires -version 5.1
<#
.SYNOPSIS
  将 data_dir 恢复到指定 checkpoint_seq（破坏性；须无运行中的 structdb 进程）。

.PARAMETER DataDir
  引擎 data_dir（含 checkpoint.chain、wal.log）。

.PARAMETER CheckpointSeq
  目标 checkpoint 序号（须存在于 checkpoint.chain）。

.PARAMETER BuildDir
  构建目录，用于定位 structdb_app。
#>
param(
  [Parameter(Mandatory = $true)][string]$DataDir,
  [Parameter(Mandatory = $true)][ulong]$CheckpointSeq,
  [string]$BuildDir = "build",
  [string]$Config = "Release"
)

$ErrorActionPreference = "Stop"
$dataDir = (Resolve-Path -LiteralPath $DataDir).Path
$chain = Join-Path $dataDir "checkpoint.chain"
if (-not (Test-Path -LiteralPath $chain)) {
  throw "checkpoint.chain not found under $dataDir (run flush_memtable first)"
}

$app = Join-Path $BuildDir "src/app/$Config/structdb_app.exe"
if (-not (Test-Path -LiteralPath $app)) {
  $app = Join-Path $BuildDir "structdb_app.exe"
}
if (-not (Test-Path -LiteralPath $app)) {
  throw "structdb_app not found under $BuildDir (build with --target structdb_app)"
}

Write-Host "Recover data_dir=$dataDir to checkpoint_seq=$CheckpointSeq"
Write-Host "Ensure no StructDB engine holds this directory open."

& $app --data-dir $dataDir --recover-to-checkpoint-seq $CheckpointSeq
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "OK. Restart engine/session and verify COUNT / SHOW SNAPSHOT (see Docs/phases/PHASE43.md)."
