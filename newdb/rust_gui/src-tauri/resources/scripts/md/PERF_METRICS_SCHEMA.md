# newdb Perf Metrics Schema

Canonical schema for `scripts/soak/test_loop.ps1` summary output consumed by dashboards and soak analytics.

## Summary JSON

- `schema_version`: `newdb.perf_summary.v1`
- `timestamp`: ISO-8601 UTC/local timestamp
- `data_dir`: run data directory
- `table`: benchmark table name
- `perf`: metrics object

## Required `perf` Keys

- `txn_normal_avg_ms` (number or null)
- `query_avg_ms_max` (number or null)
- `where_policy_rejects` (non-negative integer)
- `where_policy_fallbacks` (non-negative integer)

## Optional `perf` Keys (current)

- `txn_full_avg_ms`
- `vischk_delta_pct`
- `hp_max_query_avg_ms`
- `hp_max_query_rows`
- `hp_normal_avg_txn_ms`
- `hp_min_build_tps`
- `cm_tps_min`
- `soak_repeat`
- `soak_summary`

## Validation

Use:

`python scripts/validate/validate_perf_summary.py <summary.json>`

The validator is intended for CI/soak automation to ensure the summary remains machine-consumable while scripts evolve.

## Telemetry Event JSONL

In addition to summary JSON, `test_loop.ps1` now emits `telemetry_events_<stamp>.jsonl`.

Event contract:

- `schema_version`: `newdb.telemetry.v1`
- `timestamp`
- `run_id`
- `source`: `test_loop` | `nightly_soak_runner`
- `env`: runtime environment label (e.g. `dev|ci|nightly|prod`)
- `build`: build directory / build identifier
- `profile`: benchmark profile label (e.g. `default|gate|stress`)
- `phase`: `prep|txn_bench|query_bench|eq_cache_bench|high_pressure|concurrent_million|soak|summary`
- `metrics`: phase-specific object

Validation:

`python scripts/validate/validate_telemetry_event.py <telemetry.jsonl>`

