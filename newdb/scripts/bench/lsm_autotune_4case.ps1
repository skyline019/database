param(
    [string]$BuildDir = "E:\db\DB\newdb\build_mingw",
    [string]$BenchScript = "E:\db\DB\newdb\scripts\bench\concurrent_pressure_bench.ps1",
    [int]$Jobs = 16,
    [int]$Batches = 64,
    [int]$BatchSize = 500,
    [int]$SampleEveryBatches = 4,
    [int]$SegmentTargetBytes = 256,
    [int]$SidecarInvalidateEveryN = 16
)

$ErrorActionPreference = "Stop"

$root = "E:\temp\newdb_bench\lsm_autotune_4case_" + (Get-Date -Format "yyyyMMdd_HHmmss")
New-Item -ItemType Directory -Path $root -Force | Out-Null

$cases = @(
    @{ name="c1_t8_b8_m2_w1";  trigger=8;  batch=8;  mult=2; workers=1; reap=2 },
    @{ name="c2_t8_b12_m2_w2"; trigger=8;  batch=12; mult=2; workers=2; reap=4 },
    @{ name="c3_t12_b8_m3_w2"; trigger=12; batch=8;  mult=3; workers=2; reap=4 },
    @{ name="c4_t12_b12_m3_w4";trigger=12; batch=12; mult=3; workers=4; reap=6 }
)

$rows = @()

foreach ($c in $cases) {
    $tag = $c.name
    $jsonl = Join-Path $root ("runtime_stats_{0}.jsonl" -f $tag)
    Write-Host ""
    Write-Host "=== RUN $tag ==="
    & $BenchScript `
        -BuildDir $BuildDir `
        -RuntimeStatsJsonl $jsonl `
        -RuntimePressureBatches $Batches `
        -RuntimePressureBatchSize $BatchSize `
        -RuntimeSampleEveryBatches $SampleEveryBatches `
        -RuntimeProgressEveryBatches 1 `
        -RuntimeProgressEveryRows 100 `
        -RuntimeLsmSegmentTargetBytes $SegmentTargetBytes `
        -RuntimeSidecarInvalidateEveryN $SidecarInvalidateEveryN `
        -RuntimeSidecarInvalidateAsync `
        -RuntimeQuietSessionLog `
        -RuntimeUseBulkInsertFast `
        -RuntimeLsmCompactionAsync `
        -RuntimeLsmCompactionWorkers $c.workers `
        -RuntimeLsmCompactionReapBudget $c.reap `
        -RuntimeLsmL0CompactTrigger $c.trigger `
        -RuntimeLsmL0CompactBatch $c.batch `
        -RuntimeLsmFlushTriggerMultiplier $c.mult `
        -RuntimeEchoProgress `
        -Jobs $Jobs `
        -RepeatUntilFail 2

    if ($LASTEXITCODE -ne 0) { throw "bench failed for $tag" }
    $summary = Get-ChildItem "E:\db\DB\newdb\scripts\bench\results\concurrent_pressure_summary_*.json" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    $s = Get-Content $summary.FullName -Raw | ConvertFrom-Json
    $rows += [pscustomobject]@{
        case = $tag
        tps = [double]$s.runtime_pressure_tps_est
        p95_ms = [double]$s.runtime_pressure_batch_ms_p95
        workers = [int]$c.workers
        trigger = [int]$c.trigger
        batch = [int]$c.batch
        mult = [int]$c.mult
        reap = [int]$c.reap
        jsonl = $jsonl
    }
}

$best = $rows | Sort-Object @{Expression="tps";Descending=$true}, @{Expression="p95_ms";Descending=$false} | Select-Object -First 1
Write-Host ""
Write-Host "=== LSM AUTOTUNE 4-CASE RESULT ==="
$rows | Sort-Object tps -Descending | Format-Table case,tps,p95_ms,workers,trigger,batch,mult,reap -AutoSize
Write-Host ("AUTO_BEST_LSM case={0} workers={1} trigger={2} batch={3} multiplier={4} reap={5} tps={6} p95_ms={7}" -f `
    $best.case, $best.workers, $best.trigger, $best.batch, $best.mult, $best.reap, $best.tps, $best.p95_ms)
Write-Host ("ARTIFACT_ROOT={0}" -f $root)
