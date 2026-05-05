# database

[中文](readme.md) | English

A hands-on database engineering repository. This file is a **shortened** view of [`newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md): **top-level layout** and **high-level data flow** only. Per-module tables, field glossaries, and CI observability details stay in the full doc.

## Repository layout

```
database/                    # repo root
├── waterfall/               # paged storage & shared foundations (linked by newdb_core)
├── newdb/                   # main tree: engine + CLI + tools + tests + Rust GUI + scripts + docs
│   ├── engine/              # storage engine (C++): heap, WAL, MVCC, C ABI, cache
│   ├── cli/                 # interactive command layer (C++): shell, dispatch, modules
│   ├── tools/               # perf, smoke, runtime_report
│   ├── tests/               # GoogleTest + gtest_capi bridge sources
│   ├── rust_gui/            # Tauri + Vue desktop GUI
│   ├── scripts/             # CI, benches, validation, soak
│   ├── docs/
│   ├── intro/               # LaTeX → PDF
│   └── CMakeLists.txt
├── gtest_capi/              # optional gtest C API sample/packaging
├── docs/                    # repo-level notes (complements newdb/docs)
├── rules/
├── Makefile
└── README.md / README.en.md
```

**Compile-time direction (macro)**: `waterfall` ← `newdb/engine` ← binaries/libs; `cli` uses public headers under `engine/include/newdb/*` only.

## Data flow: build & link

```mermaid
flowchart TB
  subgraph repo["repo root database"]
    WF[waterfall]
  end

  subgraph newdb["newdb/"]
    ENG[newdb_core engine]
    DEM[newdb_demo]
    DLL[newdb_shared libnewdb]
    TOOLS[newdb_perf / smoke / runtime_report]
    TESTS[newdb_tests]
    GCAPI[gtest_capi DLL]
  end

  subgraph gui["rust_gui"]
    TAURI[Tauri + Vue]
    BIN[src-tauri/bin synced artifacts]
  end

  WF --> ENG
  ENG --> DEM
  ENG --> DLL
  ENG --> TOOLS
  ENG --> TESTS
  ENG --> GCAPI
  DLL --> TAURI
  BIN --> TAURI
  DEM -.subprocess / optional.-> TAURI
```

## Data flow: interactive commands (GUI / demo / C API)

One logical command may use **in-process DLL** or **demo subprocess**; both are “command text → buffers / logs”.

```mermaid
sequenceDiagram
  participant U as User
  participant G as rust_gui
  participant D as newdb_demo.exe
  participant L as libnewdb.dll C API
  participant S as engine Session API
  participant H as HeapTable + Page IO
  participant W as WalManager

  U->>G: command / menu
  alt DLL path
    G->>L: session_execute(line, buf)
    L->>S: parse & dispatch
  else subprocess path
    G->>D: --exec-line / stdin pipe
    D->>S: CLI dispatch → engine
  end
  S->>H: read/write heap pages
  S->>W: WAL append, sync policy
  H-->>S: row sets / state
  W-->>S: LSN / recovery anchors
  S-->>L: result text / error codes
  L-->>G: buffered output
  D-->>G: stdout/stderr
  G-->>U: grids / log panes
```

## Data flow: persistence & recovery (disk)

```mermaid
flowchart LR
  subgraph workspace["workspace directory"]
    BIN["*.bin heap data"]
    ATTR["*.attr etc."]
    WAL["demodb.wal"]
    LSN["demodb.wal_lsn / walsync.conf"]
    STATS["*.tablestats optional"]
    EQ["*.eqbloom sidecars"]
  end

  CLI[CLI handlers txn/wal] --> WAL
  CLI --> BIN
  ENG[engine WalManager codec] --> WAL
  ENG --> BIN
  REC[recovery wal_manager_recover_support] --> WAL
  REC --> BIN
  WHERE[where executor + table_stats] -.optional.-> STATS
  SIDE[sidecar writers] --> EQ
```

**Read path (macro)**: open table → `HeapTable` + optional page_cache → MVCC visibility → WHERE/sidecars.

**Write path (macro)**: coordinator → `WalManager` → heap & sidecar updates.

## Quick links

- Source: `newdb/` · GUI: `newdb/rust_gui/`
- PDF intro: `newdb/intro/out/newdb-intro.pdf`
- Developer guide: `docs/dev-guide.md`
- Module boundaries: `newdb/docs/architecture/MODULE_BOUNDARIES.md`
- Build & test: `newdb/docs/dev/BUILD.md`
- Full data-flow doc: [`PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md)

## Repository

- GitHub: [skyline019/database](https://github.com/skyline019/database)
