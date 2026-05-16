# 三十二期（PHASE32）：大体量代码包络耦合子模块化

## 目标与非目标

**目标**：在 **[PHASE26.md](PHASE26.md)** 已建立的 **`MdbEnginePorts`** 与集中声明（[`mdb_runner_internal.hpp`](../../src/client/mdb/include/structdb/client/detail/mdb_runner_internal.hpp)）之上，将原 **`mdb_runner_ops.cpp`** 大文件按主题拆为 **`mdb_ops_*.cpp`** 多编译单元；并将 **`StorageEngine`** 的 SST/WAL 元数据辅助与 flush/compact/checkpoint 成员实现跨文件拆分（`storage_engine_detail.*` 与 compact/checkpoint 多 TU；**三十三期** 再细分为 `storage_engine_compaction_lsm.cpp` 与 `storage_engine_segments_worker_checkpoint.cpp`，见 **[PHASE33.md](PHASE33.md)**），缩短增量编译链。

**非目标**：磁盘格式、默认配置、事务链 / WAL / checkpoint 语义变更；新建独立 `structdb_client_mdb_core` 子 CMake target（仍使用 `structdb_client_mdb` 多源文件）。

**依赖纪律**（与 [`POLICY.md`](../POLICY.md) §2.2 一致）：`engine/storage` 不反向依赖 orchestrator；MDB 子模块经 **`MdbEnginePorts`** / Facade 窄 KV 面访问存储。

---

## 子条 32A–32E（MDB 源文件映射）

| 子条 | 文件 | 内容摘要 |
|------|------|----------|
| **32A** | `mdb_ops_string_wire.cpp` | 快照键、trim、字面量 / 日期时间、`hex`/`wire`、CSV/JSON 辅助、`serialize_table` / `deserialize_table`、`type_matches` / `type_mismatch_msg` |
| **32B** | `mdb_ops_logical_index.cpp` | `col_name_eq`、`is_string_type`、`schema_col_index`、字符串索引维护、`clone_table`、`mdb_append_iso_datetime` |
| **32C** | `mdb_ops_predicate.cpp` | `LIKE` 匹配、`op_is_like`、`cmp_datetime_parts`、标量/类型化比较、行过滤与收集 |
| **32D** | `mdb_ops_txn_log.cpp` | `session.txn` 路径、`parse_u64_dec_sv`、V2 追加/拆分/重放、会话恢复 |
| **32E** | `mdb_ops_persist_load.cpp`、`mdb_ops_pages_journal_import.cpp` | 表加载/持久化/存在性/RENAME/DROP 键收集（**三十四期** 由 `mdb_ops_storage_and_tools.cpp` 再切）；`PAGE`/`PAGE_JSON`、DEFATTR、`IMPORTDIR`、journal tail |

共享 **`mdb_ops_detail.hpp`**：`ascii_strncasecmp` 的 `inline` 实现（避免多 TU 重复静态块）。

---

## 存储轨道（已落地）

- **`storage_engine_detail.hpp` / `storage_engine_detail.cpp`**：`namespace structdb::storage::storage_engine_detail` 内的 SST 读写、段元数据、`manifest_sst_paths_lookup_order` 等（由原 `storage_engine.cpp` 匿名命名空间迁出）。
- **Compact / checkpoint 轨道（三十二期为单文件，三十三期再切）**：原 **`storage_engine_compact_checkpoint.cpp`** 承载 `flush_memtable`、各层 `compact_*`、`checkpoint`、compaction worker 等；**三十三期** 起拆为 **`storage_engine_compaction_lsm.cpp`** 与 **`storage_engine_segments_worker_checkpoint.cpp`**（见 **PHASE33**）。**`storage_engine.cpp`** 在 **三十三期** 起进一步收窄为 `COMMIT_SEQ` 薄核； ctor / `open` / 写路径 / 读路径见 **PHASE33** 所列 TU。上述文件共享 [`storage_engine.hpp`](../../src/engine/storage/include/structdb/storage/storage_engine.hpp)。

---

## 验收命令

```powershell
cmake --build <build_dir> --target structdb_tests
ctest --test-dir <build_dir> -R "structdb_tests" --output-on-failure
.\build\tests\Release\structdb_tests.exe --gtest_filter="Mdb.*"
```

（MDB 用例合并在 **`structdb_tests`** 中，无独立 **`mdb_tests`** CMake 目标时以上即可。）

触及存储写序或 compaction 的改动额外跑 **[PHASE31.md](PHASE31.md)** 推荐 **`--gtest_filter=*Phase31*`**（或该文 / `TESTING_TXN_CHAIN` §13 的显式前缀一行）。

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-14 | 初稿：32A–32E、存储拆分索引、验收 |
| 2026-05-14 | 文档对齐：目标/存储轨道/验收与 `CHANGELOG`·`PHASE26`·`README`·`POLICY`·`PHASE31`·`ONBOARDING`·`ARCHITECTURE` |
| 2026-05-14 | 「后续」：compact/checkpoint 单 TU 已由 **[PHASE33.md](PHASE33.md)** 再切；本页存储轨道表述同步 |
| 2026-05-14 | **32E** 文件名与 **[PHASE34.md](PHASE34.md)**：`mdb_ops_storage_and_tools` 再分为 `persist_load` / `pages_journal_import` |
