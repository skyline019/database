# Write Path Tuning Runbook

This runbook captures the final write-path and transaction-path tuning profile for `newdb_demo`.

## Next Stage (Current) Tuning Delta

- Added in-process cache for equality sidecar index (`*.eqidx.<attr>`).
- Cache key includes sidecar path + `(data_sig, attr_sig)` so it auto-invalidates when data/schema changes.
- Benefit: repeated equality lookups in one process avoid repeated sidecar file parsing I/O.
- Added write-path invalidation hook: after successful `INSERT/UPDATE/DELETE/DELETEPK/SETATTR`, all `data_file.eqidx.*` files are removed and in-process eqidx cache entries are cleared.
- Benefit: avoids stale sidecar reads after writes and guarantees next equality lookup rebuilds from latest data.
- Refined invalidation granularity:
  - `UPDATE` and `SETATTR` now invalidate only affected attribute sidecars (`data_file.eqidx.<attr>`), and page index sidecars for the same attribute names (`data_file.idx.<attr>.asc|.desc`).
  - `RENATTR` invalidates both `old` and `new` attribute sidecars (eq + page by those names).
  - `DELATTR` invalidates deleted attribute sidecar (eq + page for that name).
  - `INSERT/DELETE/DELETEPK` still invalidate all eq and page sidecars for correctness.
- Benefit: reduces unnecessary sidecar rebuilds during attribute-local updates.

## Final Default Profile

- `WALSYNC normal 20`
- `AUTOVACUUM threshold 300`

Default behavior on fresh startup (no existing config files):

- WAL defaults to `normal` with `20ms` interval.
- AUTOVACUUM threshold defaults to `300` ops.

Persisted files under runtime data dir:

- `demodb.walsync.conf`
- `demodb.vacuum.conf`

## Runtime Commands

- Show unified tuning state:
  - `SHOW TUNING`
  - alias: `SHOW STATUS`
- Show WAL state:
  - `WALSYNC`
- Set WAL mode:
  - `WALSYNC full`
  - `WALSYNC normal 20`
  - `WALSYNC off`
- Show AUTOVACUUM state:
  - `AUTOVACUUM`
- Set AUTOVACUUM:
  - `AUTOVACUUM on 300`
  - `AUTOVACUUM off`
  - `AUTOVACUUM threshold 300`

## Benchmark Reproduction

Script:

- `scripts/bench/txn_write_bench.ps1`
- `scripts/bench/eq_sidecar_cache_bench.ps1`
- `scripts/bench/eq_sidecar_invalidation_bench.ps1`
- `scripts/bench/million_scale_bench.ps1`
- `scripts/soak/test_loop.ps1` (optional high-pressure stage via `-RunHighPressure`)

Example:

```powershell
powershell -ExecutionPolicy Bypass -File "e:\工程\数据库工程\new\scripts\bench\txn_write_bench.ps1" -Txns 120
```

Typical result pattern:

- `off` fastest
- `normal` close to `off` and much faster than `full`
- `full` slowest but strongest per-commit durability

Eq sidecar cache micro-benchmark:

```powershell
powershell -ExecutionPolicy Bypass -File "e:\工程\数据库工程\new\scripts\bench\eq_sidecar_cache_bench.ps1" -PassQueries 80
```

Notes:

- This benchmark focuses on `mode=eq_sidecar` trace records.
- On small tables, gain may be limited; larger data and larger sidecar files show clearer benefit.

Selective invalidation benchmark:

```powershell
powershell -ExecutionPolicy Bypass -File "e:\工程\数据库工程\new\scripts\bench\eq_sidecar_invalidation_bench.ps1" -Rows 900 -Loops 50
```

Interpretation:

- `unrelated_avg_eq_sidecar_us`: update non-query attr (`age`) then query by `dept`.
- `related_avg_eq_sidecar_us`: update query attr (`dept`) then query by `dept`.
- Expect `related` >= `unrelated`; this confirms selective invalidation keeps non-related sidecars hot.

Recent sample:

- `unrelated_avg_eq_sidecar_us=64.04`
- `related_avg_eq_sidecar_us=67.08`
- `related_vs_unrelated_delta_pct=4.75%`

## Regression Reproduction

Script:

- `scripts/txn_wal_vacuum_test.mdb`

Example:

```powershell
$work = Join-Path $env:TEMP "newdb_gui_build"
New-Item -Path $work -ItemType Directory -Force | Out-Null
Get-Content ".\scripts\txn_wal_vacuum_test.mdb" | `
  & (Join-Path $work "newdb_demo.exe") --data-dir $work --table qa_regression
```

Expected:

- `WALSYNC` set/query works.
- `AUTOVACUUM` set/query works.
- `SHOW TUNING` reports WAL + AUTOVACUUM together.

## Recommended Profiles by Scenario

- Balanced online workload (recommended default):
  - `WALSYNC normal 20`
  - `AUTOVACUUM on 300`
- Strong durability priority:
  - `WALSYNC full`
  - `AUTOVACUUM on 300`
- Batch import / disposable benchmark:
  - `WALSYNC off`
  - `AUTOVACUUM off` during load, then `VACUUM`, then re-enable.
  - Optional: set `NEWDB_WAL_COMPACT_BYTES` for WAL compaction during steady-state; it can be unset or a low value for bulk import if you want minimal WAL work mid-load, then run `VACUUM` and restore tuning.

## Big data: monitoring, WHERE plans, and layout

- **Storage snapshot** (in `newdb_demo`): `SHOW STORAGE` prints the workspace path, `demodb.wal` size, `demodb.wal_lsn` (high-water for sidecar binding), and total size/count of `*.bin` files. Use this in ops / dashboards to watch WAL growth and table file growth.
- **Heavy WHERE warning**: set `NEWDB_WHERE_WARN_HEAVY=1` to emit a stderr hint when a query uses a plan that can touch the full logical row count on large tables (`and_ordered_scan`, `fallback_scan`, or unconstrained `full_scan_all`). Optional `NEWDB_WHERE_WARN_HEAVY_MIN_ROWS` (default `10000`) sets the table size floor before warning.
- **Hard policy / throttling**: `NEWDB_WHERE_POLICY_MODE=off|warn|ratelimit|reject` (default `ratelimit`), with `NEWDB_WHERE_POLICY_MIN_ROWS` and `NEWDB_WHERE_RATE_LIMIT_QPS` controlling heavy-plan trigger and QPS limit. This is the runtime rollback switch for strict blocking.
- **“先建条件、再扫” (narrow first, then scan)**:
  1. Put the most **selective** conditions first in **AND** lists so the engine can use **equality** / index-friendly single-condition reduction where possible.
  2. For **stable sort keys**, use **PAGE** (page sidecar) so repeated `ORDER BY` does not resort the whole table every time.
  3. Avoid relying on **multi-column AND** where **no** term is index-friendly: that falls back to **full heap** scans. Add eq conditions on indexed attrs or restructure the query.
- **Sidecar invalidation**: `INSERT/DELETE/…` still drops **all** `*.eqidx.*` and **`*.idx.<order_key>.(asc|desc)`** (PAGE) for that table. **UPDATE/SETATTR** with `changed_attrs` only removes eq sidecars and PAGE sidecars for **those** attribute names, so unrelated order keys and attrs stay valid when possible.
- **Visibility checkpoint sidecar**: `*.vischk` caches visible slot lists bound to `data_sig + attr_sig + wal_lsn` (default enabled). Disable via `NEWDB_VISCHK=off` for emergency rollback.
- **Covering aggregate sidecar (incremental)**: `*.cov.<key_attr>.<include_attr>` accelerates common `COUNT(eq)` / `SUM|AVG(eq)` paths without full row decode.
- **EQ sidecar size control**: `NEWDB_EQ_BUCKETS=<N>` enables bucketed eq sidecar layout to reduce large high-cardinality parse overhead.
- **Lazy materialization diagnostics**: `NEWDB_LAZY_MATERIALIZE_WARN_ROWS` logs when an API forces full `materialize_all_rows` beyond threshold.
- **Single huge `.bin`**: for cold-start and maintenance, long-term **incremental checkpoint / visible-row snapshot in sidecar** is a large design. Short term, **partition or shard** by time or key range (multiple table files) to keep per-file `N` smaller and spread I/O.
- For large result sets, prefer **PAGE** and streaming-style consumption in the app rather than one giant in-memory `SELECT` result.

## Acceptance Checklist

- [ ] Fresh data dir shows defaults via `SHOW TUNING`:
  - `WALSYNC=normal normal_interval_ms=20`
  - `AUTOVACUUM ... ops_threshold=300`
- [ ] `scripts/bench/txn_write_bench.ps1` runs successfully for `full/normal/off`.
- [ ] `txn_wal_vacuum_test.mdb` runs without command errors.
- [ ] `demodb.walsync.conf` updates after `WALSYNC` changes.
- [ ] `demodb.vacuum.conf` updates after `AUTOVACUUM threshold` changes.
- [ ] `scripts/soak/test_loop.ps1 -RunHighPressure` completes and reports `hp_max_query_avg_ms` / `hp_normal_avg_txn_ms` / `hp_min_build_tps`.

