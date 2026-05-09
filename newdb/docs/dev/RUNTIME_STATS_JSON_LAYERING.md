# Runtime stats JSON layering

Today, C API runtime stats, JSONL snapshots, **`SHOW TUNING JSON`**, and **`SHOW STATUS JSON`** share one builder: [`format_runtime_stats_json`](../../cli/shell/c_api/runtime_stats_json_builder.h) ([`runtime_stats_json_builder.cc`](../../cli/shell/c_api/runtime_stats_json_builder.cc)), which includes [`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc).

## Named JSON fragments (contract vocabulary)

- **`newdb_engine_runtime_stats_json`**: the subset of `stats` keys produced exclusively by the engine writers [`runtime_stats_snapshot_json_write.cpp`](../../engine/src/json/runtime_stats_snapshot_json_write.cpp) (`append_runtime_stats_snapshot_json_members_*`). Suitable when a consumer needs **only** coordinator/WAL/POD counters and must not depend on the CLI bridge TU.
- **`newdb_cli_runtime_stats_json`**: the complementary fragment emitted inside [`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc) — `txn_isolation`, WHERE / heap-decode sandwich fields, and txn-facade toggles (`wal_adaptive_enabled`, `vacuum_running`, …) that require `ShellState` / CLI stack.
- **Combined** (`SHOW TUNING JSON`, `newdb_session_runtime_stats`, JSONL `stats` object): one merged object; see also `###` layer headings in [`RUNTIME_STATS_SCHEMA.md`](../../scripts/validate/RUNTIME_STATS_SCHEMA.md).

## Current layering

- **Types**: [`TxnRuntimeStats`](../../cli/modules/txn/coordinator/txn_runtime_stats.h) aliases the POD layout in [`txn_runtime_stats_snapshot.h`](../../engine/include/newdb/txn_runtime_stats_snapshot.h) (engine-visible; coordinator still fills the struct).
- **POD serialization (engine)**: [`append_runtime_stats_snapshot_json_members_before_where`](../../engine/include/newdb/runtime_stats_snapshot_json_write.h) / [`after_heap`](../../engine/include/newdb/runtime_stats_snapshot_json_write.h) in [`runtime_stats_snapshot_json_write.cpp`](../../engine/src/json/runtime_stats_snapshot_json_write.cpp) emit all JSON members that depend **only** on the snapshot POD (uses [`newdb::json_escape`](../../engine/include/newdb/json_escape.h) for embedded strings).
- **CLI extension (bridge)**: `_generated_runtime_json.inc` stitches isolation, WHERE counters, heap decode slots, and txn facade fields (`wal_adaptive_enabled`, `vacuum_running`, …). Edit this fragment when adding coordinator/shell-only keys; extend the engine writers when adding POD-only keys.

## Why two engine appenders (`before_where` / `after_heap`)

[`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc) must emit **TxnRuntimeStats POD fields**, then **WHERE counters + heap decode slots** from `ShellState`, then **remaining POD fields**, then **txn facade knobs**. That ordering is fixed by the runtime-stats schema contract; merging [`append_runtime_stats_snapshot_json_members_before_where`](../../engine/include/newdb/runtime_stats_snapshot_json_write.h) and [`after_heap`](../../engine/include/newdb/runtime_stats_snapshot_json_write.h) into one engine call would duplicate or relocate CLI-only sections unless the generator emits the full object in one TU.

## Follow-up

1. Optional **code generator** from [`txn_runtime_stats_snapshot.h`](../../engine/include/newdb/txn_runtime_stats_snapshot.h) for POD keys only, still emitting two fragments around the CLI sandwich.
2. If/when coordinator-only fields shrink further, revisit a single ordered `ostream` pipeline (engine + callbacks).

Treat runtime stats JSON as **CliEmbedTier** capability ([`C_API_CAPABILITY_TIERS.md`](C_API_CAPABILITY_TIERS.md)).
