$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$bench = "E:\db\DB\newdb\scripts\bench\concurrent_pressure_bench.ps1"
$cmp1  = "E:\db\DB\newdb\scripts\bench\compare_tail_stage_triplet.py"
$cmp2  = "E:\db\DB\newdb\scripts\bench\analyze_tail_stage_ranking.py"

$root  = "E:\temp\newdb_bench\matrix_workers2_default_" + (Get-Date -Format "yyyyMMdd_HHmmss")
New-Item -ItemType Directory -Path $root -Force | Out-Null

$segments = @(128,256,512)
$n = 16

# 你的正式档位（long-run 对齐口径）
$Batches = 64
$BatchSize = 500
$SampleEvery = 2
$RowsProgress = 50
$Jobs = 16
$RepeatUntilFail = 2

# 固定当前推荐 LSM 参数（workers=2）
# 注意：这里避免使用任何短变量名（可能受不可见字符/输入法污染），直接用常量。
Write-Host ("LSM_DEFAULT workers={0} reap={1} trigger={2} batch={3} mult={4}" -f 2, 4, 8, 12, 2)

$rows = @()

foreach ($seg in $segments) {
  $tag = "seg{0}_n{1}_w{2}" -f $seg, $n, 2
  $jsonl = Join-Path $root ("runtime_stats_{0}.jsonl" -f $tag)

  Write-Host ""
  Write-Host "=== RUN $tag ==="

  $args = @{
    BuildDir = "E:\db\DB\newdb\build_mingw"
    RuntimeStatsJsonl = $jsonl
    RuntimePressureBatches = [int]$Batches
    RuntimePressureBatchSize = [int]$BatchSize
    RuntimeSampleEveryBatches = [int]$SampleEvery
    RuntimeProgressEveryBatches = 1
    RuntimeProgressEveryRows = [int]$RowsProgress
    RuntimeLsmSegmentTargetBytes = [int]$seg
    RuntimeSidecarInvalidateEveryN = [int]$n
    RuntimeSidecarInvalidateAsync = $true
    RuntimeQuietSessionLog = $true
    RuntimeUseBulkInsertFast = $true
    RuntimeLsmCompactionAsync = $true
    RuntimeLsmCompactionWorkers = 2
    RuntimeLsmCompactionReapBudget = 4
    RuntimeLsmL0CompactTrigger = 8
    RuntimeLsmL0CompactBatch = 12
    RuntimeLsmFlushTriggerMultiplier = 2
    RuntimeEchoProgress = $true
    Jobs = [int]$Jobs
    RepeatUntilFail = [int]$RepeatUntilFail
  }
  & $bench @args

  if ($LASTEXITCODE -ne 0) { throw "bench failed for $tag" }

  $summary = Get-ChildItem "E:\db\DB\newdb\scripts\bench\results\concurrent_pressure_summary_*.json" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

  $s = Get-Content $summary.FullName -Raw | ConvertFrom-Json

  $rankTxt = & python $cmp2 --input $jsonl
  if ($LASTEXITCODE -ne 0) { throw "ranking failed for $tag" }

  $top = "unknown"
  $line = ($rankTxt -split "`n" | Select-Object -First 1)
  if ($line -match "top_stage=([a-zA-Z0-9_]+)") { $top = $Matches[1] }

  $rows += [pscustomobject]@{
    tag       = $tag
    seg       = $seg
    sidecar_n = $n
    workers   = 2
    top_stage = $top
    tps       = [double]$s.runtime_pressure_tps_est
    p95_ms    = [double]$s.runtime_pressure_batch_ms_p95
    jsonl     = $jsonl
    summary   = $summary.FullName
  }
}

Write-Host ""
Write-Host "=== TAIL ATTRIBUTION TRIPLET (workers=2) ==="
python $cmp1 `
  --jsonl-128 (Join-Path $root ("runtime_stats_seg128_n{0}_w{1}.jsonl" -f $n, 2)) `
  --jsonl-256 (Join-Path $root ("runtime_stats_seg256_n{0}_w{1}.jsonl" -f $n, 2)) `
  --jsonl-512 (Join-Path $root ("runtime_stats_seg512_n{0}_w{1}.jsonl" -f $n, 2)) `
  --top-k 6

Write-Host ""
Write-Host "=== FINAL METRICS (workers=2) ==="
$rows | Sort-Object seg | Select-Object tag, top_stage, tps, p95_ms, jsonl | Format-Table -AutoSize

Write-Host ""
Write-Host "ARTIFACT_ROOT=$root"
