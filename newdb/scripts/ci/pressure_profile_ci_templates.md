# Pressure Profile CI Command Templates

Use these templates for apples-to-apples CI checks under three durability profiles.

## Recommended default gate thresholds

`concurrent_pressure_bench.ps1` now auto-applies profile defaults when gate args are not explicitly set:

- `newdb-default`:
  - `max_vacuum_queue_depth_peak=6`
  - `max_wal_recovery_last_elapsed_ms=800`
  - `max_lock_wait_max_ms_delta=120`
  - `max_scheduler_throttle_delta=300`
  - `max_lsm_segment_count=80`
  - `max_lsm_read_segments_scanned_p95=20`
  - `min_lsm_compaction_bytes_amp_efficiency=0.45`
- `leveldb-like` (throughput-oriented):
  - `max_vacuum_queue_depth_peak=8`
  - `max_wal_recovery_last_elapsed_ms=1000`
  - `max_lock_wait_max_ms_delta=160`
  - `max_scheduler_throttle_delta=500`
  - `max_lsm_segment_count=96`
  - `max_lsm_read_segments_scanned_p95=24`
  - `min_lsm_compaction_bytes_amp_efficiency=0.40`
- `innodb-like` (durability-oriented):
  - `max_vacuum_queue_depth_peak=6`
  - `max_wal_recovery_last_elapsed_ms=600`
  - `max_lock_wait_max_ms_delta=100`
  - `max_scheduler_throttle_delta=250`
  - `max_lsm_segment_count=72`
  - `max_lsm_read_segments_scanned_p95=18`
  - `min_lsm_compaction_bytes_amp_efficiency=0.50`
- `hybrid-balanced` (adaptive mixed):
  - `max_vacuum_queue_depth_peak=6`
  - `max_wal_recovery_last_elapsed_ms=700`
  - `max_lock_wait_max_ms_delta=110`
  - `max_scheduler_throttle_delta=280`
  - `max_lsm_segment_count=76`
  - `max_lsm_read_segments_scanned_p95=19`
  - `min_lsm_compaction_bytes_amp_efficiency=0.48`

All three profiles also default to:
- `max_vacuum_compact_failure_delta=0`
- `min_lsm_memtable_flush_delta=1`
- `min_wal_group_commit_batch_commits_delta` is **disabled by default** (`-1`).\n+  Enable it only when you explicitly turn on group commit (e.g. `GROUPCOMMIT window_ms ...`) and want a hard gate:\n+  - example: `-MinWalGroupCommitBatchCommitsDelta 1` (or higher)

## newdb-default (balanced baseline)

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\bench\concurrent_pressure_bench.ps1" `
  -BuildDir build_nightly `
  -Jobs 8 `
  -RepeatUntilFail 1 `
  -RuntimePressureBatches 16 `
  -RuntimePressureBatchSize 500 `
  -RuntimeSampleEveryBatches 2 `
  -RuntimeLsmSegmentTargetBytes 256 `
  -RuntimeSidecarInvalidateEveryN 16 `
  -RuntimeSidecarInvalidateAsync `
  -RuntimeQuietSessionLog `
  -RuntimeUseBulkInsertFast `
  -RuntimeLsmCompactionAsync `
  -RuntimeLsmCompactionWorkers 2 `
  -RuntimeLsmCompactionReapBudget 4 `
  -RuntimeLsmL0CompactTrigger 8 `
  -RuntimeLsmL0CompactBatch 12 `
  -RuntimeLsmFlushTriggerMultiplier 2 `
  -BenchmarkProfile newdb-default `
  -RuntimeWalSyncMode normal `
  -RuntimeWalSyncNormalIntervalMs 20
```

## LevelDB-like (throughput-oriented)

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\bench\concurrent_pressure_bench.ps1" `
  -BuildDir build_nightly `
  -Jobs 8 `
  -RepeatUntilFail 1 `
  -RuntimePressureBatches 16 `
  -RuntimePressureBatchSize 500 `
  -RuntimeSampleEveryBatches 2 `
  -RuntimeLsmSegmentTargetBytes 256 `
  -RuntimeSidecarInvalidateEveryN 16 `
  -RuntimeSidecarInvalidateAsync `
  -RuntimeQuietSessionLog `
  -RuntimeUseBulkInsertFast `
  -RuntimeLsmCompactionAsync `
  -RuntimeLsmCompactionWorkers 2 `
  -RuntimeLsmCompactionReapBudget 4 `
  -RuntimeLsmL0CompactTrigger 8 `
  -RuntimeLsmL0CompactBatch 12 `
  -RuntimeLsmFlushTriggerMultiplier 2 `
  -BenchmarkProfile leveldb-like `
  -RuntimeWalSyncMode normal `
  -RuntimeWalSyncNormalIntervalMs 20
```

## InnoDB-like (durability-oriented)

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\bench\concurrent_pressure_bench.ps1" `
  -BuildDir build_nightly `
  -Jobs 8 `
  -RepeatUntilFail 1 `
  -RuntimePressureBatches 16 `
  -RuntimePressureBatchSize 500 `
  -RuntimeSampleEveryBatches 2 `
  -RuntimeLsmSegmentTargetBytes 256 `
  -RuntimeSidecarInvalidateEveryN 16 `
  -RuntimeSidecarInvalidateAsync `
  -RuntimeQuietSessionLog `
  -RuntimeUseBulkInsertFast `
  -RuntimeLsmCompactionAsync `
  -RuntimeLsmCompactionWorkers 2 `
  -RuntimeLsmCompactionReapBudget 4 `
  -RuntimeLsmL0CompactTrigger 8 `
  -RuntimeLsmL0CompactBatch 12 `
  -RuntimeLsmFlushTriggerMultiplier 2 `
  -BenchmarkProfile innodb-like `
  -RuntimeWalSyncMode full `
  -RuntimeWalSyncNormalIntervalMs 20
```

## Hybrid-balanced (mixed policy)

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\bench\concurrent_pressure_bench.ps1" `
  -BuildDir build_nightly `
  -Jobs 8 `
  -RepeatUntilFail 1 `
  -RuntimePressureBatches 16 `
  -RuntimePressureBatchSize 500 `
  -RuntimeSampleEveryBatches 2 `
  -RuntimeLsmSegmentTargetBytes 256 `
  -RuntimeSidecarInvalidateEveryN 16 `
  -RuntimeSidecarInvalidateAsync `
  -RuntimeQuietSessionLog `
  -RuntimeUseBulkInsertFast `
  -RuntimeLsmCompactionAsync `
  -RuntimeLsmCompactionWorkers 2 `
  -RuntimeLsmCompactionReapBudget 4 `
  -RuntimeLsmL0CompactTrigger 8 `
  -RuntimeLsmL0CompactBatch 12 `
  -RuntimeLsmFlushTriggerMultiplier 2 `
  -BenchmarkProfile hybrid-balanced `
  -RuntimeWalSyncMode normal `
  -RuntimeWalSyncNormalIntervalMs 20
```

## Nightly automated compare (recommended)

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\ci\nightly_pressure_profile_compare.ps1" `
  -BuildDir build_nightly `
  -Jobs 8 `
  -RuntimePressureBatches 16 `
  -RuntimePressureBatchSize 500 `
  -RuntimeSampleEveryBatches 2 `
  -MinLeveldbOverInnodbTpsRatio 1.05 `
  -MaxInnodbOverLeveldbP95Ratio 1.15
```
