# newdb 构建说明（CMake）

本文描述 `newdb/CMakeLists.txt` 常用配置、MSVC 静态 CRT（`/MT`）、MinGW 全量链接与测试命令。

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

若环境无法稳定访问 GitHub，可在**已成功下载过一次**的构建目录中复用源码树，例如：

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

- **瘦 C API 动态库**：未来可能提供仅依赖 `newdb_core` 的共享库变体；当前 `newdb_shared` 仍通过 `newdb_demo_lib` 编排交互式 demo/CLI 能力。

## 其他常用选项

| 选项 | 含义 |
|------|------|
| `NEWDB_BUILD_TESTS` | 是否构建 `newdb_tests` 并注册 CTest（默认 `ON`） |
| `NEWDB_BUILD_SHARED` | 是否构建 C ABI 共享库 `newdb`（`newdb_shared`；默认 `ON`） |
| `NEWDB_BUILD_GUI` | 是否构建 Qt `newdb_gui`（默认 `OFF`） |
| `NEWDB_ENABLE_COVERAGE` | GCC/Clang 覆盖率（默认 `OFF`） |

### 可选：WAL 恢复 / sidecar 调试环境变量

回归 **`ctest -C …`** 时通常无需设置。开发与 soak 时可参考 [STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md)：`NEWDB_RECOVER_MIN_LSN`、`NEWDB_RECOVER_ENABLE_OFFSET_SEEK`、`NEWDB_RECOVER_USE_CHECKPOINT_LSN`；平等索引 catalog 行为见 [TXN_ISOLATION_AND_LOCKING.md](../txn/TXN_ISOLATION_AND_LOCKING.md) 的 **`NEWDB_INDEX_CATALOG_ENFORCE`**（**CI 可选**：在流水线中显式设为 **`1`** 以在 catalog 身份不一致时硬失败；默认构建与本地测试不必开启）。**GitHub Actions** 提供独立 job **`linux-index-catalog-enforce`**（见 [`newdb-ci.yml`](../../.github/workflows/newdb-ci.yml)），对约定 **`ctest -R`** 子集在 **`NEWDB_INDEX_CATALOG_ENFORCE=1`** 下回归。**Page cache**：惰性堆解码路径上可选 **`NEWDB_PAGE_CACHE_MAX_BYTES`**（正整数总字节上限，默认 `0` 关闭）。**查询成本**：在已有 `TableStats` 提示时 **`NEWDB_QUERY_COST_MODEL=1`** 用行数/NDV 估计为 AND 链选种子条件（与 **`NEWDB_QUERY_USE_TABLE_STATS=1`** 配合）。查询统计：在已开启 **`NEWDB_QUERY_USE_TABLE_STATS=1`** 时，可再加 **`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 将 NDV 统计写入数据文件旁 **`*.tablestats`**（见 `table_stats.h`）。**Vacuum**：`NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 与可选 **`NEWDB_VACUUM_HEALTH_SLOT_WEIGHT`** 见同一治理文档。**WHERE 策略**：可选 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**（正整数）在 ratelimit 模式下收紧 `where_policy_scan_cap_rows` 的有效上限，用于单次估算扫描行预算（默认关闭）。

模块目录约定见 [MODULE_BOUNDARIES.md](../architecture/MODULE_BOUNDARIES.md)。对标 LevelDB/InnoDB 与路线图见 [COMPARE_LEVELDB_INNODB_ROADMAP.md](../roadmap/COMPARE_LEVELDB_INNODB_ROADMAP.md)。
