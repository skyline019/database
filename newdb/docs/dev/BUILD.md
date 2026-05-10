# newdb 构建说明（CMake）

本文描述 `newdb/CMakeLists.txt` 常用配置、MSVC 静态 CRT（`/MT`）、MinGW 全量链接与测试命令。

## C API 产物矩阵（官方发行 / slim / full embed）

- **官方发行（推荐）：Plugin 成对交付**：`-DNEWDB_C_API_PLUGIN_BACKEND=ON`、`-DNEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`、`-DNEWDB_BUILD_SHARED=ON`（且 **非** slim）时，宿主 `newdb_shared` **仅**链 `newdb_core`，完整会话能力由同树构建的 **`newdb_cli_backend` 共享库**在运行时加载；集成方在 `newdb_session_create` 前设置 **`NEWDB_CLI_BACKEND_PATH`**（绝对路径）。详见 [C_API_PLUGIN_BACKEND.md](C_API_PLUGIN_BACKEND.md)、[plugin_backend_packaging.md](../../scripts/ci/plugin_backend_packaging.md)、[INSTALL_PLUGIN.md](../../scripts/ci/INSTALL_PLUGIN.md)。**打包目录**：`cmake --build <dir> --target shared_bundle` 在 `shared_bundle/<Config>/` 下复制宿主库、backend、`README_PLUGIN.txt`、以及 `INSTALL_PLUGIN.md`（Windows 另有 `set_newdb_plugin_env.cmd`）。发行安装可选用 `-DNEWDB_INSTALL_PLUGIN_RELEASE=ON` 将二者安装到 `lib/newdb/`（见 CMakeLists 选项）。
- **瘦构建（slim）**：`-DNEWDB_SHARED_SLIM=ON` 时，共享库仅链 `newdb_core` 与 `engine/src/api/c/c_api_slim.cpp`（桩会话 API；不链 `newdb_capi_adapter`）。输出名仍为 `newdb`（Windows）或与平台一致的 `libnewdb`。
- **Full embed（单库 / 便于本地一体调试）**：默认 `-DNEWDB_BUILD_SHARED=ON`、`-DNEWDB_SHARED_SLIM=OFF`、且未开 plugin 时，`newdb_shared` 静态链入 `newdb_capi_adapter`（dispatch / bridge / txn / WHERE / sidecar；不含交互 REPL 单独 TU）。完整 C API，**不宣称**与 CLI 代码静态解耦。参见 [MODULE_BOUNDARIES.md](../architecture/MODULE_BOUNDARIES.md) §Shared library modes。
- **选型**：对外交付 / GUI 同进程集成 → **plugin**；只要引擎校验 / ABI → slim；仅本地或特殊场景要单 DLL → full embed。
- **回归**：开启 `-DNEWDB_BUILD_TESTS=ON` 时会构建 `newdb_capi_slim_tests`（编译定义 `NEWDB_C_API_SLIM=1`），在进程内覆盖瘦 C API 的版本、`newdb_check_schema_file`、execute 错误文案与 runtime stats 桩 JSON。
- **CMake Presets**（仓库 [`CMakePresets.json`](CMakePresets.json)）：`plugin-shared` / **`plugin-shared-release`**（官方发行矩阵）、`slim-shared`、`full-shared`（单库 embed）。示例：`cmake --preset plugin-shared-release`，`cmake --build --preset plugin-shared-release-rel`，再 `cmake --build --preset plugin-shared-release-rel --target shared_bundle`。`testPresets` 供 CMake 3.25+ 的 `ctest --preset <name>` 使用。plugin 运行前设置 **`NEWDB_CLI_BACKEND_PATH`**。
- **Windows + gtest DLL**：`newdb_capi_slim_tests` 与 `newdb_tests` 一样在 `NEWDB_GTEST_BUILD_SHARED_DLL=ON` 时 POST_BUILD 复制运行时 DLL 到可执行文件目录，便于 `ctest -R CApiSlim` 无需手改 `PATH`。从 Visual Studio「测试资源管理器」或直接在 IDE 里运行 `newdb_capi_slim_tests.exe` 时，请确认 exe 输出目录下已有复制的 `gtest*.dll`（与 POST_BUILD 一致），或将该目录加入 PATH；命令行回归仍以 **`ctest`** 为准。
- **为何不在 CMake 里给 `gtest_discover_tests` 配统一 `ENVIRONMENT PATH=`**：discovered 测试条目与 `newdb_capi_slim_tests` 输出目录在多配置生成器 + GTest 发现模式下不一一对应；强行注入 `'PATH=' $<TARGET_FILE_DIR:...>` 仍可能在 IDE/CTest 混用场景遗漏。当前以 POST_BUILD 复制 DLL 与在上文 PATH 说明为准。
- **GUI / 校验脚本**：`newdb/scripts/validate` 为运行时契约与校验脚本的 **canonical**；`rust_gui/scripts/validate` 与 `rust_gui/src-tauri/resources/scripts/validate` 为打包镜像，CI 通过 `scripts/validate/check_rust_gui_validate_mirror.py` 对齐。[`rust_gui/src/commandPolicy.ts`](../../rust_gui/src/commandPolicy.ts) 仅在 GUI 源码树维护一份，无打包副本时不要求镜像门禁。

## CMake shell 积木目标（OBJECT）

CLI/shell 按编译单元拆成若干 **`newdb_shell_*` OBJECT 库**，在 **`newdb_capi_adapter`** 里用 `$<TARGET_OBJECTS:…>` 聚合成静态适配层，再由默认 **`newdb_shared`** 链入（plugin/slim 模式除外）。名称与依赖装配以 **[MODULE_BOUNDARIES.md](../architecture/MODULE_BOUNDARIES.md)** 的 **Release assembly gates** 与 Mermaid 图为准。

| CMake 目标 | 职责（一句） |
|------------|----------------|
| `newdb_shell_state` | Shell 状态、Facade、C API 桥与 runtime stats 等「脊骨」TU |
| `newdb_shell_bootstrap_capi` | 非 REPL 引导、mdb 脚本、diag、export 等 C API 路径 |
| `newdb_shell_bootstrap_repl` | 交互式 REPL（`demo_shell.cc`）；仅叠进 **`newdb_shell`**，不进 embed 适配层 |
| `newdb_shell_dispatch` | `process_command_line`、路由与各 handler / support / services |
| `newdb_shell_common` | logging、utils、table_view、import 等跨模块基础 |
| `newdb_shell_catalog` | schema catalog |
| `newdb_query` | **STATIC**：WHERE 解析、执行计划、统计（源在 `cli/modules/where`）；由 `newdb_capi_adapter` **PUBLIC** 链接 |
| `newdb_shell_txn` | 事务协调器与各子服务 |
| `newdb_shell_sidecar` | 索引 sidecar、可见性、存储健康等 |

**Preset**：`CMakePresets.json` 中的 `full-shared`（默认 embed）、`slim-shared`、`plugin-shared` 与上文 C API 矩阵一致；plugin 运行前需设置 **`NEWDB_CLI_BACKEND_PATH`**（见 [C_API_PLUGIN_BACKEND.md](C_API_PLUGIN_BACKEND.md)）。

## 通用

在仓库内 `newdb` 目录为工程根：

```bash
cmake -S . -B build
cmake --build build --config Release   # 多配置生成器（Visual Studio）必须带 --config
ctest --test-dir build -C Release --output-on-failure
```

单配置生成器（Ninja、`MinGW Makefiles`）使用 `-DCMAKE_BUILD_TYPE=Release`，`ctest` 一般不必 `-C`。

## MSVC / Visual Studio：全量编译与测试（日常推荐）

多配置生成器下常用 **`RelWithDebInfo`**（与本地调试、CI 常用配置一致）。在 **`newdb`** 目录执行：

```powershell
cmake -S . -B build
cmake --build build --config RelWithDebInfo --parallel
ctest --test-dir build -C RelWithDebInfo --output-on-failure --parallel 4
```

**PDB 争用**：`newdb_tests` 在 MSVC 下已加编译选项 **`/FS`**（多 TU 共享 PDB 时更安全）。若仍偶发 **`C1041`**（无法写入程序数据库），可将 CL 并行降为 1：

```powershell
cmake --build build --config RelWithDebInfo -- /p:CL_MPCount=1
```

**分阶段 / 提速回归**（改观察类、事务、WAL、WHERE 后可选；完整门禁仍以全量 `ctest` 为准）：

```powershell
ctest --test-dir build -C RelWithDebInfo --output-on-failure `
  -R "Wal|Txn|Where|QueryTableStats|IndexCatalog|WalSegmentScanner|TableStorageHealth|CiMicrobench|DispatchRouting"
```

**运行时统计 JSONL 契约**（与 [`scripts/validate/RUNTIME_STATS_SCHEMA.md`](../../scripts/validate/RUNTIME_STATS_SCHEMA.md) 对齐）：

```powershell
python scripts/validate/validate_runtime_stats.py path\to\runtime_snapshot.jsonl
# 可选分层（默认仍为完整 required_stats_keys）：--stats-keys-tier engine | cli_embed
```

路线图与阶段说明见 [`OPTIMIZATION_PLAN_2026.md`](../roadmap/OPTIMIZATION_PLAN_2026.md)；性能与 Bench 门见 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md)（含 **PR / Nightly / Release** 矩阵、**Baseline JSONL 归档契约（v2）**（`capture_baseline.py --emit-archive-contract` / `--write-archive-manifest`）与 **`--max-wal-recovery-last-elapsed-ms`** 建议起点）。

### Nightly 子集（`ctest -R StorageSoakLight`）

**`StorageSoakLight.*`**（[`tests/test_storage_soak.cpp`](../../tests/test_storage_soak.cpp)）偏运行时间与 I/O，可在夜间/soak 流水线用 **`ctest -C <Config> -R StorageSoakLight --output-on-failure`** 单独跑或并入更大矩阵；**默认全量 `ctest` 仍会运行它们**。

**Nightly 风格 verify（PowerShell）**（与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3 一致）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ci/verify_clean_reconfigure.ps1 `
  -BuildDir build_nightly -BenchGateStorage -BenchGateWalRecovery
```

**Soak / Nightly 报告落盘**：`scripts/soak/nightly_soak_runner.ps1` 默认将趋势 JSONL 写入 **`scripts/results/nightly_soak_trend.jsonl`** 与 **`scripts/results/test_loop_trend.jsonl`**（相对 `newdb/` 根）；与 perf summary 的联合校验见该脚本内逻辑。与 storage / WAL recovery 数值门、fixture 路径一致的矩阵见 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3–§4。

## GoogleTest 离线源码（可选）

**推荐（仓库内）**：将 googletest 源码放在与 `newdb/` 同级的 **`gtest_capi/third_party/googletest/`**（顶层含 `CMakeLists.txt`）。`newdb` 配置测试时会自动使用该目录，无需传变量。亦支持 **`gtest_capi/googletest/`** 或已有的 **`gtest_capi/_deps/googletest-src/`**。

**自研跨平台 C 封装（`gtest_capi`）**：在仓库根目录的 **`gtest_capi/`** 可独立 `cmake` 出共享库（Windows 为 `gtest_capi.dll` / MinGW 为 `libgtest_capi.dll`，Linux 为 `libgtest_capi.so`），供其它语言或进程经 **C ABI** 驱动 GoogleTest。头文件为 **`gtest_capi/include/gtest_capi.h`**；在 Windows 上仅链接该 DLL 时编译定义 **`GTEST_C_API_SHARED_USE`**。安装规则：`cmake --install`（可选关 `-DGTEST_CAPI_INSTALL=OFF`）。

否则可在**已成功下载过一次**的构建目录中复用源码树，例如：

```bash
cmake -S . -B build-mt -DNEWDB_MSVC_STATIC_RUNTIME=ON \
  -Dgoogletest_SOURCE_DIR=/path/to/existing/build/_deps/googletest-src
```

## MSVC：静态 C 运行库（`/MT`）

选项 **`NEWDB_MSVC_STATIC_RUNTIME`**（默认 `OFF`）：开启后对 MSVC 使用静态 CRT（`MultiThreaded` / `MultiThreadedDebug`），可执行文件通常不再依赖 `MSVCP140.dll`、`VCRUNTIME140.dll` 等。

与 `/MT` 同时开测试时，工程会将 **`gtest_force_shared_crt`** 设为 **`OFF`**，并在需要时 **`NEWDB_GTEST_BUILD_SHARED_DLL=OFF`**，避免与 GoogleTest 的 `/MD` 混链。FetchContent 拉取的 **`gtest` / `gtest_main` / `gmock` / `gmock_main`** 目标还会被显式设为相同的 **`MSVC_RUNTIME_LIBRARY`**，避免子工程未继承顶层 `CMAKE_MSVC_RUNTIME_LIBRARY` 时出现混链。

示例（Visual Studio 生成器，单独 build 目录便于与 `/MD` 默认树并存）：

```powershell
cmake -S "e:/db/DB/newdb" -B "e:/db/DB/newdb/build-msvc-mt" -DNEWDB_MSVC_STATIC_RUNTIME=ON -DNEWDB_BUILD_TESTS=ON
cmake --build "e:/db/DB/newdb/build-msvc-mt" --config Release
ctest --test-dir "e:/db/DB/newdb/build-msvc-mt" -C Release --output-on-failure
```

使用 **Ninja + MSVC** 时同样需要 `-DNEWDB_MSVC_STATIC_RUNTIME=ON`，且多配置下的 Release 构建示例：

```powershell
cmake -S "e:/db/DB/newdb" -B "e:/db/DB/newdb/build-mt-ninja" -G Ninja `
  -DCMAKE_BUILD_TYPE=Release -DNEWDB_MSVC_STATIC_RUNTIME=ON -DNEWDB_BUILD_TESTS=ON
cmake --build "e:/db/DB/newdb/build-mt-ninja"
ctest --test-dir "e:/db/DB/newdb/build-mt-ninja" --output-on-failure
```

## MinGW：可执行文件与主要 DLL 的静态运行时链接

在 **`MINGW`** 工具链下，CMake 会对 `newdb_demo`、`newdb_smoke`、`newdb_perf`、`newdb_concurrent_perf`、`newdb_runtime_report` 以及（若启用）`newdb_shared`、`newdb_tests`、`gtest_capi`、`newdb_gui` 统一添加：

- `-static-libgcc`
- `-static-libstdc++`
- `-Wl,-Bstatic,-l:libwinpthread.a,-Bdynamic`

以降低目标机上缺少 `libwinpthread-1.dll` 或与 `libstdc++` 版本不匹配的风险。若 **`NEWDB_GTEST_BUILD_SHARED_DLL=ON`**，上述链接选项也会应用到 **`gtest` / `gtest_main` 等共享库目标**，减少测试 DLL 单独依赖自带 MinGW 运行时的情况。

如需「尽量完全静态」的可执行文件（体积更大、可能与部分第三方 DLL 不兼容），可在配置阶段追加例如 `-DCMAKE_EXE_LINKER_FLAGS=-static`（按工具链与依赖自行验证）。

示例（MSYS2 MinGW64，请按本机路径调整编译器）：

```powershell
cmake -S "e:/db/DB/newdb" -B "e:/db/DB/newdb/build-mingw" -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Release `
  "-DCMAKE_C_COMPILER=E:/msys64/mingw64/bin/gcc.exe" `
  "-DCMAKE_CXX_COMPILER=E:/msys64/mingw64/bin/g++.exe"
cmake --build "e:/db/DB/newdb/build-mingw" --parallel 8
ctest --test-dir "e:/db/DB/newdb/build-mingw" --output-on-failure
```

## 命令路由与回归测试

- Phase-2 动词前缀快路径：`cli/shell/dispatch/router/dispatch_routing.{h,cc}` 中的 `shell_line_targets_phase2_only`。
- 表驱动前缀单测：`tests/test_dispatch_routing.cpp`（随 `newdb_tests` 构建）。
- CI Bench 门默认通过 `scripts/ci/ci_bench_gate.py` 额外跑 `CiMicrobench` 与 `DispatchRouting`（见 [PERF_AND_CI_BUDGETS.md](../ci/PERF_AND_CI_BUDGETS.md)）。
- **全量编译与可选 `ctest -R` 片段**：见上文「MSVC / Visual Studio：全量编译与测试」。

## 产品与 ABI（规划中）

- **默认 C ABI 动态库**：`newdb_shared` 链接 **`newdb_core` + `newdb_capi_adapter`**（CLI/dispatch 闭包，不含交互式 REPL TU）。`newdb_demo` 链 **`newdb_shell`**（`newdb_demo_lib` 为其 ALIAS，`newdb_shell` = adapter + [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc)）。
- **瘦 C ABI（可选）**：`-DNEWDB_SHARED_SLIM=ON` 时，`newdb_shared` **仅**链接 `newdb_core`，使用 [`c_api_slim.cpp`](../../engine/src/api/c/c_api_slim.cpp)；会话类 API（`execute`、完整 runtime stats、`where_plan_json` 等）为桩实现或受限行为，适合嵌入方只需版本/ABI/`newdb_check_schema_file` 等、且希望减小链接闭包；完整功能需默认 `OFF` 或另链 `newdb_capi_adapter`/`newdb_shell`/子进程 `newdb_demo`。

## 其他常用选项

| 选项 | 含义 |
|------|------|
| `NEWDB_BUILD_TESTS` | 是否构建 `newdb_tests` 并注册 CTest（默认 `ON`） |
| `NEWDB_BUILD_SHARED` | 是否构建 C ABI 共享库 `newdb`（`newdb_shared`；默认 `ON`） |
| `NEWDB_SHARED_SLIM` | 仅 `newdb_core` + 瘦 C API，不链接 `newdb_capi_adapter`（默认 `OFF`；见上文） |
| `NEWDB_BUILD_GUI` | 是否构建 Qt `newdb_gui`（默认 `OFF`） |
| `NEWDB_ENABLE_COVERAGE` | GCC/Clang 覆盖率（默认 `OFF`） |

### 可选：WAL 恢复 / sidecar 调试环境变量

回归 **`ctest -C …`** 时通常无需设置。开发与 soak 时可参考 [STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md)：`NEWDB_RECOVER_MIN_LSN`、`NEWDB_RECOVER_ENABLE_OFFSET_SEEK`、`NEWDB_RECOVER_USE_CHECKPOINT_LSN`；平等索引 catalog 行为见 [TXN_ISOLATION_AND_LOCKING.md](../txn/TXN_ISOLATION_AND_LOCKING.md) 的 **`NEWDB_INDEX_CATALOG_ENFORCE`**（**CI 可选**：在流水线中显式设为 **`1`** 以在 catalog 身份不一致时硬失败；默认构建与本地测试不必开启）。**GitHub Actions** 提供独立 job **`linux-index-catalog-enforce`**（见 [`newdb-ci.yml`](../../.github/workflows/newdb-ci.yml)），对约定 **`ctest -R`** 子集在 **`NEWDB_INDEX_CATALOG_ENFORCE=1`** 下回归。**Page cache**：惰性堆解码路径上可选 **`NEWDB_PAGE_CACHE_MAX_BYTES`**（正整数总字节上限，默认 `0` 关闭）。**查询成本**：在已有 `TableStats` 提示时 **`NEWDB_QUERY_COST_MODEL=1`** 用行数/NDV 估计为 AND 链选种子条件（与 **`NEWDB_QUERY_USE_TABLE_STATS=1`** 配合）。查询统计：在已开启 **`NEWDB_QUERY_USE_TABLE_STATS=1`** 时，可再加 **`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 将 NDV 统计写入数据文件旁 **`*.tablestats`**（见 `table_stats.h`）。**Vacuum**：`NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 与可选 **`NEWDB_VACUUM_HEALTH_SLOT_WEIGHT`** 见同一治理文档。**WHERE 策略**：可选 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**（正整数）在 ratelimit 模式下收紧 `where_policy_scan_cap_rows` 的有效上限，用于单次估算扫描行预算（默认关闭）。

模块目录约定见 [MODULE_BOUNDARIES.md](../architecture/MODULE_BOUNDARIES.md)。对标 LevelDB/InnoDB 与路线图见 [COMPARE_LEVELDB_INNODB_ROADMAP.md](../roadmap/COMPARE_LEVELDB_INNODB_ROADMAP.md)。
