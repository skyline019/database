#Requires -Version 5.1
<#
.SYNOPSIS
  运行 structdb_tests 中与六/七/八期事务链相关的 GTest 子集（与 Docs/TESTING_TXN_CHAIN.md §3 一致）。

.PARAMETER BuildConfig
  CMake 配置名，默认 Release。

.PARAMETER BuildDir
  构建目录，默认 <仓库根>/build。

.NOTES
  七期补充（耐久 / undo / WAL，见 TESTING_TXN_CHAIN.md §4）可另跑：
  --gtest_filter="EmbedClient.MultiKeyBatchFsyncSurvivesRestart:Engine.WalAutoTrimAfterFlushFromConfig:Engine.UndoAutoTruncateAfterFlushFromConfig:StorageEngine.VersionedUndo*:StorageEngine.EmbedWalBatch*:StorageEngine.WalTrim*:StorageEngine.RebuildUndoStack*:StorageEngine.UndoTruncate*"
  八期 4C 仅 undo 截断（见 TESTING_TXN_CHAIN.md §5）：
  --gtest_filter="StorageEngine.UndoTruncate*:Engine.UndoAutoTruncateAfterFlushFromConfig"
  九期 compaction / I/O 骨架（见 TESTING_TXN_CHAIN.md §6）：
  --gtest_filter="StorageEngine.Compaction*:Infra.IoBackend*:CheckpointState.UndoPrefix*:StorageEngine.Phase10*"
  十期 checkpoint v2 / undo 前缀（见 TESTING_TXN_CHAIN.md §7）：
  --gtest_filter="StorageEngine.Phase10*:CheckpointState.UndoPrefix*"
  十一期 L0 阈值自动 compaction（见 TESTING_TXN_CHAIN.md §8）：
  --gtest_filter="StorageEngine.Phase11*:Engine.L0AutoCompactAfterFlushFromConfig"
  十二期 MANIFEST L0/L1（见 TESTING_TXN_CHAIN.md §9）：
  --gtest_filter="StorageEngine.Phase12*:Manifest.Phase12*:Engine.L1CompactOutputFromConfig"
  三十一期 PHASE31 边界矩阵子集（见 TESTING_TXN_CHAIN.md §13）：
  --gtest_filter="StorageEngine.Phase31*:Engine.Phase31*:EmbedClient.Phase31*:Engine.Phase24*"
#>
param(
  [string]$BuildConfig = "Release",
  [string]$BuildDir = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $BuildDir) {
  $BuildDir = Join-Path $repoRoot "build"
}

$exe = Join-Path $BuildDir "tests" $BuildConfig "structdb_tests.exe"
if (-not (Test-Path -LiteralPath $exe)) {
  Write-Error "未找到 $exe 。请先 cmake --build （STRUCTDB_BUILD_TESTS=ON）。"
}

$filter = 'Mdb.TxnChain*:Mdb.Repl*:Mdb.WhereUpdateWhereTxn:Mdb.EngineKvReadSeqVisibility:Mdb.TwoEmbedSessions*'
& $exe "--gtest_filter=$filter"
exit $LASTEXITCODE
