# newdb Runtime Stats Schema

Schema name: `newdb.runtime_stats.v1`  
Format: JSON Lines (`.jsonl`), one snapshot per line.

## Top-level fields

- `schema_version` (string, required): must be `newdb.runtime_stats.v1`
- `ts_ms` (int, required): unix timestamp in milliseconds
- `label` (string, required): snapshot label, e.g. `pressure_before`, `pressure_sample_1`, `pressure_after`
- `run_id` (string, optional): run correlation id for windowed analysis
- `stats` (object, required): runtime counters/status payload

## `stats` object fields

- `walsync` (string): one of `off|normal|full`
- `normal_interval_ms` (int >= 0)
- `autovacuum` (bool)
- `vacuum_ops_threshold` (int >= 0)
- `vacuum_min_interval_sec` (int >= 0)
- `vacuum_trigger_count` (int >= 0)
- `vacuum_execute_count` (int >= 0)
- `vacuum_cooldown_skip_count` (int >= 0)
- `write_conflicts` (int >= 0)
- `txn_begin_lock_conflicts` (int >= 0): `BEGIN(table)` 阶段文件锁冲突次数
- `wal_compact_count` (int >= 0): WAL checkpoint+truncate 成功执行次数

## Notes

- Counters are cumulative within one runtime/session and should be compared via deltas.
- For CI/runtime gate isolation, prefer filtering by `run_id`, then applying optional `--last-n` windows.
- Validation script: `scripts/validate/validate_runtime_stats.py`.

