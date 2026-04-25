# newdb Rust + Vue GUI

This folder now uses a Tauri (Rust) + Vue desktop architecture.

## Design

- Keep the original Qt GUI (`newdb_gui`) unchanged.
- Rust backend provides desktop commands (`src-tauri/src/lib.rs`).
- Vue frontend provides modern multi-panel UI (`src/App.vue`).
- Command execution delegates to existing `newdb_demo` so legacy command capability is preserved.
- Dedicated benchmark executable `newdb_perf` is used for million-scale performance test runs.

## Features migrated

- Table list panel and table switching.
- Data page query (page / page size / order / desc).
- Command console with immediate execution.
- MDB script tab (multi-line command execution).
- Transaction buttons: `BEGIN`, `COMMIT`, `ROLLBACK`.
- Write-path tuning commands: `WALSYNC`, `AUTOVACUUM`, `SHOW TUNING`.
- Tools menu quick actions: `SHOW TUNING`, `WALSYNC normal 20`, `AUTOVACUUM on 300/off`.
- Tools menu performance entry: `百万级性能压测(可执行)...` (invokes `newdb_perf`).
- Million-scale benchmark recommendation: run `100k` tier first, then extend to `500k/1000k` after confirming elapsed time.
- High-throughput ingestion commands:
  - `BULKINSERT(start_id,count[,dept])` (safe default batch mode)
  - `BULKINSERTFAST(start_id,count[,dept])` (faster when IDs are guaranteed fresh/non-duplicate)
- Logs panel.

## Run

```bash
cd newdb/rust_gui
npm install
npm run tauri:dev
```

## Build

```bash
cd newdb/rust_gui
pwsh ./scripts/sync_runtime_binaries.ps1 -BuildDir ../build_mingw
npm run tauri:build
```

## Backend command bridge

Rust backend calls `newdb_demo` with:

- `--exec-line <CMD>` for single command execution
- `--page <NO> <SIZE> --order <KEY> [--desc]` for data page fetch

Rust backend calls `newdb_perf` with:

- `--demo-exe <path>`
- `--data-dir <path>`
- `--sizes <csv>` / `--query-loops <n>` / `--txn-per-mode <n>` / `--build-chunk-size <n>`

Make sure `newdb_demo` is available under one of:

- `../build_shared/newdb_demo(.exe)`
- `../build/newdb_demo(.exe)`
- `../build_all/newdb_demo(.exe)`

And `newdb_perf` is available under one of:

- `../build_shared/newdb_perf(.exe)`
- `../build/newdb_perf(.exe)`
- `../build_all/newdb_perf(.exe)`

For bundle resources, sync runtime binaries into `src-tauri/bin` first:

```bash
pwsh ./scripts/sync_runtime_binaries.ps1 -BuildDir ../build_mingw
```

This syncs:

- `newdb_demo.exe`
- `newdb_perf.exe`
- `newdb_runtime_report.exe`
- `libnewdb.dll`
