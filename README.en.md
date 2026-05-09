# database

[中文](readme.md) | English

A hands-on database engineering repository. This file is a **shortened** view of [`newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md): **top-level layout** and **high-level data flow** only. Per-module tables, field glossaries, and CI observability details stay in the full doc. **CMake**: the CLI is split into **`newdb_shell_*` OBJECT** libraries folded into static **`newdb_capi_adapter`** and linked by default **`newdb_shared`**; **`newdb_demo`** / integration tests link **`newdb_shell`** for the REPL slice. See [`MODULE_BOUNDARIES.md`](newdb/docs/architecture/MODULE_BOUNDARIES.md) and [`BUILD.md`](newdb/docs/dev/BUILD.md) for the assembly diagram and **plugin / slim** modes.

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
    CAP[newdb_capi_adapter]
    SH[newdb_shell]
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
  ENG --> CAP
  CAP --> DLL
  ENG --> SH
  SH --> DEM
  ENG --> TOOLS
  SH --> TESTS
  ENG --> GCAPI
  DLL --> TAURI
  BIN --> TAURI
  DEM -.subprocess / optional.-> TAURI
```

(Default **full embed**: `DLL` = `newdb_core` + `newdb_capi_adapter`. **Plugin** mode uses a separate `newdb_cli_backend` and **`NEWDB_CLI_BACKEND_PATH`**; see **Track P** below and [`C_API_PLUGIN_BACKEND.md`](newdb/docs/dev/C_API_PLUGIN_BACKEND.md).)

### Three `libnewdb` link shapes (cheat sheet)

Same as [PROJECT_DATAFLOW_WHOLE.md](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md) **§3.1** in the full doc.

```mermaid
flowchart TB
  subgraph defEmbed [Default_full_embed]
    E0[newdb_core]
    C0[newdb_capi_adapter]
    Sh0[newdb_shared]
    E0 --> C0
    C0 --> Sh0
  end
  subgraph slimMode [NEWDB_SHARED_SLIM]
    E1[newdb_core]
    Slim[c_api_slim]
    Sh1[newdb_shared]
    E1 --> Slim
    Slim --> Sh1
  end
  subgraph plugMode [Plugin_backend]
    E2[newdb_core]
    Sh2[newdb_shared]
    BE[newdb_cli_backend]
    E2 --> Sh2
    Sh2 -->|runtime_dlopen| BE
  end
```

## Data flow: interactive commands (GUI / demo / C API)

One logical command may use **in-process DLL** (**full_embed** or **plugin**) or **demo subprocess**; both are “command text → buffers / logs”.

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
  alt in_process_full_embed
    G->>L: session_execute(line, buf)
    L->>S: static newdb_capi_adapter then Session
  else in_process_plugin
    G->>L: session_execute(line, buf)
    L->>S: after dlopen newdb_cli_backend same Session edge
  else demo_subprocess
    G->>D: --exec-line / stdin pipe
    D->>S: newdb_shell dispatch to engine
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

**Note (default)**: CLI write paths ship inside **`newdb_capi_adapter`** linked into the main DLL; under **plugin**, the same logic lives in **`newdb_cli_backend`**, while WAL/heap relationships to the engine stay the same.

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

## C API plugin layout (Track P)

With the **plugin** preset you typically ship **two** binaries: the main **`newdb` / `libnewdb` (`newdb_shared`)** linked to the engine core only, plus **`newdb_cli_backend`** loaded at runtime. Set **`NEWDB_CLI_BACKEND_PATH`** to the backend shared library (absolute path) before `newdb_session_create` (same idea as the **`plugin-shared`** preset in `CMakePresets.json`). Details: [`C_API_PLUGIN_BACKEND.md`](newdb/docs/dev/C_API_PLUGIN_BACKEND.md), [`BUILD.md`](newdb/docs/dev/BUILD.md), and [`plugin_backend_packaging.md`](newdb/scripts/ci/plugin_backend_packaging.md).

**Track Q (WHERE / query sink)**: still a separate milestone; see the **WHERE planner** fork in [`MODULE_BOUNDARIES.md`](newdb/docs/architecture/MODULE_BOUNDARIES.md).

## Quick links

- Source: `newdb/` · GUI: `newdb/rust_gui/`
- PDF intro: `newdb/intro/out/newdb-intro.pdf`
- Developer guide: `docs/dev-guide.md`
- Module boundaries: `newdb/docs/architecture/MODULE_BOUNDARIES.md`
- Build & test: `newdb/docs/dev/BUILD.md`
- Full data-flow doc: [`PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md)

## Repository

- GitHub: [skyline019/database](https://github.com/skyline019/database)
