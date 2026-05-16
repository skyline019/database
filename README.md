# StructDB

分层运行时（Facade → Orchestrator → Planner → Scheduler → GraphExecutor）、混合存储（WAL / LSM 骨架 + 页式 buffer pool、Redo/Undo、checkpoint）与 **同进程嵌入式可持久会话客户端** 的 C++17 实验工程；底座为 `src/Base/waterfall`（`wf::*`）。

## 文档

| 资源 | 路径 |
|------|------|
| **LaTeX 源码级手册（与 newdb/intro 同构；WSL 仅用于 `latexmk` 生成 PDF，非引擎编译）** | [intro/README.md](intro/README.md) → `intro/out/structdb-intro.pdf` |
| **架构总览（数据流 Mermaid + 代码版图 + 源码摘录）** | [Docs/ARCHITECTURE.md](Docs/ARCHITECTURE.md) |
| 工程方针（含 **§4.0 文件系统保底**、事务链 §4.1–4.3） | [Docs/POLICY.md](Docs/POLICY.md) |
| 版本变更记录 | [Docs/CHANGELOG.md](Docs/CHANGELOG.md) |
| 文档索引（含 `phases/` 专文目录） | [Docs/README.md](Docs/README.md) |
| 六期事务链（读视图、GTest 切片） | [Docs/phases/TESTING_TXN_CHAIN.md](Docs/phases/TESTING_TXN_CHAIN.md) · [POLICY §4.1–§4.5](Docs/POLICY.md) |
| 七期 InnoDB 映射（redo/undo/MTR 类比、耐久 Level） | [Docs/phases/TXN_INNODB_MAP.md](Docs/phases/TXN_INNODB_MAP.md) |
| 八期 `undo.log` 回收（4C 子集、WAL 独立） | [Docs/phases/UNDO_LOG_4C.md](Docs/phases/UNDO_LOG_4C.md) |
| 九期 L0 compaction / I/O 骨架 | [Docs/COMPACTION.md](Docs/COMPACTION.md) · [src/engine/infra/include/structdb/infra/io_backend.hpp](src/engine/infra/include/structdb/infra/io_backend.hpp) |
| 十期 checkpoint **v2** 与 `undo.log` 前缀回收 | [Docs/phases/PHASE10.md](Docs/phases/PHASE10.md) · [Docs/phases/CHECKPOINT_UNDO_PREFIX.md](Docs/phases/CHECKPOINT_UNDO_PREFIX.md) |
| 十一期 L0 compaction **阈值自动调度**（`flush_memtable` 后） | [Docs/phases/PHASE11.md](Docs/phases/PHASE11.md) · [Docs/COMPACTION.md](Docs/COMPACTION.md) |
| **十二期** `MANIFEST` **L0/L1** 与可选 **L1 合并输出** | [Docs/phases/PHASE12.md](Docs/phases/PHASE12.md) · [Docs/COMPACTION.md](Docs/COMPACTION.md) |
| **十三期及后续** 路线（计划草案） | [Docs/phases/PHASE13_PLUS_PLAN.md](Docs/phases/PHASE13_PLUS_PLAN.md) |
| **二十四期** 部署边界 / WAL 重放 / 新人入口 | [Docs/phases/PHASE24.md](Docs/phases/PHASE24.md) · [Docs/phases/WAL_REPLAY.md](Docs/phases/WAL_REPLAY.md) · [Docs/ONBOARDING.md](Docs/ONBOARDING.md) |
| **二十九期** perf 门禁与运维可观测性索引 | [Docs/phases/PHASE29.md](Docs/phases/PHASE29.md) |
| **三十五期** L0 锁外合并、`EmbedClient::submit` 互斥、可选 `data_dir` 建议锁 | [Docs/phases/PHASE35.md](Docs/phases/PHASE35.md) |

- **持久化保底**：备份与恢复以 **`data_dir` + `session_dir`** 下文档化文件为准（**`POLICY` §4.0**）；默认引擎根为相对路径 **`_data/`**（`structdb_app` 同默认；可用 `--data-dir` 覆盖），默认 embed 会话为 **`_data/embed_session/`**。表数据/schema 为引擎 **`mdb$v2$*`** KV，无单独「每表一文件」第二权威。跨进程若多程序指向 **同一 `data_dir`**，须 **单一 WAL 打开者** 或启用 **建议文件锁**（`structdb_engine_open_ex` 与 `STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK`，见 [PHASE35.md](Docs/phases/PHASE35.md)）。
- **REPL / MDB**：面向 **同进程嵌入式** 会话；`session.txn` 提供未提交逻辑状态恢复（见 `CHANGELOG` 崩溃模型与 `fsync_each_session_txn_op`）。非分布式、非多主。**`BEGIN` 激活时不得切换 `TXNISOLATION`**（须先 `COMMIT`/`ROLLBACK`），见 `POLICY` §4.1。
- **MVCC**：`mdb$` 键为 **读序号裁剪** 的单版本可见性，非完整行多版本链。存储侧 **`ROLLBACK` 与 `undo_stack_`**：默认 **`mdb_chain_rollback_on_mdb_rollback=false`** 时 **`ROLLBACK` 不**链式弹 `undo_stack_`；**二十三 23C** 门闩为 **true** 且 **`mdb_persist_in_begin=true`** 时，MDB **`ROLLBACK`** 先将栈弹回 **`BEGIN` 水位**（受限模型，见 `POLICY` §4.3、[Docs/phases/PHASE23.md](Docs/phases/PHASE23.md)）。
- **WAL / 空间回收（四期 + 二十/二十一期）**：可选 **WAL 前缀裁剪**（`wal_try_trim_prefix_through_checkpoint` / `EngineConfigSnapshot::wal_auto_trim_prefix_after_flush`）仅在 **`flush_memtable` 已将前缀 WAL 对应数据刷入 SST** 后安全；**二十期**起 **多段 WAL**（`wal.segments` v2、`wal/archive/`）见 [Docs/phases/PHASE20.md](Docs/phases/PHASE20.md)；**二十一期**起可选 **`wal/archive/` 封存回收**（`EngineConfigSnapshot::wal_archive_gc_after_flush`，须同时开启前缀裁剪，默认 **false**，见 [Docs/phases/PHASE21.md](Docs/phases/PHASE21.md)）。**`undo.log`** 可选 **`undo_auto_truncate_after_flush`** 或显式 **`undo_try_truncate_when_stack_empty`**（八期子集，见 [Docs/phases/UNDO_LOG_4C.md](Docs/phases/UNDO_LOG_4C.md)）；**`undo.log` 物理分段轮转** 仍为十六期 16B 后续（与 WAL 多段不同）。详见 [Docs/POLICY.md](Docs/POLICY.md) §3.3–3.5 与 [Docs/CHANGELOG.md](Docs/CHANGELOG.md)。
- **Checkpoint 元数据（五期）**：引擎侧 **`checkpoint.a` / `checkpoint.b` + `checkpoint.active`**（CRC32C）与遗留 **`checkpoint` 文本双写**，崩溃恢复时仍 **以 WAL 为准**；会话 `session.ckpt` 可记录 **`checkpoint_seq`**（第三行）。详见 [Docs/POLICY.md](Docs/POLICY.md) §3.4。

## 快速构建（Windows / MSVC）

```powershell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release -j
ctest --test-dir build -C Release
```

可执行文件示例：`build/tests/Release/structdb_tests.exe`、`build/src/app/Release/structdb.exe`（具体子路径随生成器略有不同）。

### 常用 CMake 选项

- `STRUCTDB_STATIC_MSVC_RUNTIME`：默认 **ON**，与 `ThirdParty/gtest_capi` 静态 CRT 对齐。
- `STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK`：默认 **OFF**（使用 `ThirdParty/` 本地 fmt、spdlog、benchmark）；设为 **ON** 时通过 FetchContent 拉取依赖（需网络）。
- `STRUCTDB_BUILD_TESTS` / `STRUCTDB_BUILD_BENCHMARKS`：控制测试与微基准目标。
- `STRUCTDB_ENABLE_PERF_GATE`：默认 **OFF**；为 **ON** 时注册 **`ctest` `structdb_perf_gate`**（**Python 3.8+**、与 [`benchmarks/baselines/structdb_bench_baseline.json`](benchmarks/baselines/structdb_bench_baseline.json) 对比；**MSVC 多配置**下请用 **`ctest -C Release -L perf`**）。详见 [`Docs/POLICY.md`](Docs/POLICY.md) §6.2。
- `STRUCTDB_BUILD_CAPI_SHARED`：默认 **ON**（与 **`STRUCTDB_BUILD_TESTS`** 一起时 **`ctest`** 会跑 **`structdb_capi_shared_smoke`**）；设为 **OFF** 可跳过共享库与 smoke，仅保留静态 **`structdb_capi`**（见 [Docs/phases/PHASE27.md](Docs/phases/PHASE27.md)、[Docs/phases/PHASE28.md](Docs/phases/PHASE28.md)）。

详见 [Docs/POLICY.md](Docs/POLICY.md) 第 5 节。

## 仓库布局（节选）

| 目录 | 说明 |
|------|------|
| `src/Base/` | `wf_waterfall` 静态库与 `waterfall` 源码 |
| `src/engine/` | `infra`、`planner`、`scheduler`、`runtime`、`orchestrator`、`facade`、`storage` |
| `src/client/embed/` | 嵌入式客户端与对外薄头 |
| `src/app/` | 进程入口（默认 `--data-dir` 为 **`_data`**；`--session-dir` 缺省为 **`_data/embed_session`**） |
| `gui/rust_gui/` | Tauri + Vue GUI（可选；见 [Docs/phases/PHASE30.md](Docs/phases/PHASE30.md)） |
| `tests/` | GoogleTest + `gtest_capi` 驱动（落盘产物在 **CMake 构建目录** 下 `test_runs/<时间戳>/`，按进程隔离） |
| `benchmarks/` | Google Benchmark 基线 |
| `ThirdParty/` | 第三方源码与 `gtest_capi` |
| `src/c_api/` | C 绑定（`structdb_run_mdb_file` / `_ex`、会话 API；可选 `structdb_capi_shared`，见 [Docs/phases/PHASE27.md](Docs/phases/PHASE27.md)、[Docs/phases/PHASE28.md](Docs/phases/PHASE28.md)） |
| `cmake/StructDBThirdParty.cmake` | fmt / spdlog / benchmark 的 vendor 与 FetchContent 逻辑 |

## 许可与第三方

**StructDB 自有代码**（除 `ThirdParty/`、`build/` 等目录外由本仓库维护的源码与文档）以 [**MIT License**](LICENSE)（SPDX：`MIT`）授权。各 `ThirdParty/*` 组件遵循其各自许可证文件。
